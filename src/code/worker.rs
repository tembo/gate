//! The worker side of `execute_mcp_code`: runs one script in QuickJS and reports how it went.
//!
//! The worker is gate itself, started with `--code-worker` and an empty environment. QuickJS
//! has no file, network or process APIs here, but the worker is a separate process, not an
//! OS sandbox.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    io::Write,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, anyhow, bail};
use rquickjs::{
    AsyncContext, AsyncRuntime, CatchResultExt, CaughtError, Ctx, Exception, Function, Promise,
    Value, context::EvalOptions, function::Async,
};
use tokio::{io::Stdin, sync::oneshot};

use super::{
    MAX_CALLS, MAX_CALLS_IN_FLIGHT, MAX_RESULT_BYTES, MEMORY_LIMIT_BYTES,
    ipc::{self, FromWorker, ToWorker},
    log_cost, timed_out,
};

const STACK_LIMIT_BYTES: usize = 256 * 1024;
/// The most unhandled rejections reported as warnings
const MAX_WARNINGS: usize = 10;

/// Sets up `console`, `mcp` and the hooks the worker uses, before the script runs
const PRELUDE: &str = r#"
(() => {
    const call = globalThis.__gateCall;
    const callTool = async (serverId, toolName, args = {}) => {
        if (typeof serverId !== "string" || typeof toolName !== "string") {
            throw new TypeError("a tool call needs a server ID and a tool name, as strings");
        }
        if (typeof args !== "object" || args === null || Array.isArray(args)) {
            throw new TypeError("a tool's arguments must be an object");
        }
        // `call` registers the call right away, even though its result comes later
        const outcome = JSON.parse(await call(serverId, toolName, JSON.stringify(args)));
        if ("Err" in outcome) throw new Error(outcome.Err);
        return outcome.Ok;
    };
    // `mcp.server.tool(args)`. `then` is never a tool, so awaiting these doesn't treat
    // them as promises; `mcp.call` reaches tools named `then` or `call`.
    const server = (serverId) => new Proxy({}, {
        get(_, toolName) {
            if (typeof toolName !== "string" || toolName === "then") return undefined;
            return (args) => callTool(serverId, toolName, args);
        },
    });
    globalThis.mcp = new Proxy(Object.freeze({ call: callTool }), {
        get(target, serverId) {
            if (serverId === "call") return target.call;
            if (typeof serverId !== "string" || serverId === "then") return undefined;
            return server(serverId);
        },
    });

    const log = globalThis.__gateLog;
    const format = (value) => {
        if (typeof value === "string") return value;
        try {
            const json = JSON.stringify(value);
            return json === undefined ? String(value) : json;
        } catch {
            return String(value);
        }
    };
    const write = (...values) => log(values.map(format).join(" "));
    globalThis.console = Object.freeze({ log: write, info: write, warn: write, error: write, debug: write });

    // Rejections that are still unhandled when the script ends become warnings. One that
    // gets a handler later is removed again.
    const unhandled = new Map();
    globalThis.__gateTrackRejection = (promise, reason, handled) => {
        if (handled) unhandled.delete(promise);
        else unhandled.set(promise, reason);
    };
    globalThis.__gateWarnings = (max) => {
        const reasons = Array.from(unhandled.values());
        const warnings = reasons.slice(0, max).map((reason) =>
            `unhandled rejection: ${reason instanceof Error ? reason.message : format(reason)}`);
        if (reasons.length > max) warnings.push(`and ${reasons.length - max} more unhandled rejections`);
        return warnings;
    };
    globalThis.__gateSerialize = (value) => {
        const json = JSON.stringify(value === undefined ? null : value);
        if (json === undefined) throw new TypeError("the result isn't JSON-serializable");
        return json;
    };
})();
"#;

pub fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // rquickjs's async types aren't `Send`, so everything runs on this thread
    tokio::task::LocalSet::new().block_on(&runtime, run())
}

async fn run() -> anyhow::Result<()> {
    let mut stdin = tokio::io::stdin();
    let start: Option<ToWorker> = ipc::read(&mut stdin)
        .await
        .map_err(|error| anyhow!(error))?;
    let Some(ToWorker::Start { code, timeout_ms }) = start else {
        bail!("expected a start message");
    };
    let calls = SharedCalls::default();
    let reader = tokio::task::spawn_local(read_results(stdin, calls.clone()));
    let message = match execute(&code, Duration::from_millis(timeout_ms), calls).await {
        Ok((result, warnings)) => FromWorker::Complete { result, warnings },
        Err(error) => FromWorker::Failed { error },
    };
    reader.abort();
    send(&message).context("can't send the result")
}

type CallOutcome = Result<serde_json::Value, String>;

#[derive(Default)]
struct Calls {
    next_id: u64,
    made: usize,
    /// Calls waiting for their results. This is also how many are in flight: a call keeps
    /// its slot until its result arrives, so finishing, failing or the gateway disconnecting
    /// all free it.
    pending: HashMap<u64, oneshot::Sender<CallOutcome>>,
    /// Set once the gateway stops sending results
    disconnected: bool,
}

type SharedCalls = Rc<RefCell<Calls>>;

/// Settles calls as their results arrive
async fn read_results(mut stdin: Stdin, calls: SharedCalls) {
    loop {
        match ipc::read::<ToWorker>(&mut stdin).await {
            Ok(Some(ToWorker::CallResult { id, outcome })) => {
                if let Some(pending) = calls.borrow_mut().pending.remove(&id) {
                    let _ = pending.send(outcome);
                }
            }
            Ok(Some(ToWorker::Start { .. })) => {}
            // The gateway is gone, or sent something unreadable. Dropping the pending calls'
            // senders fails them.
            Ok(None) | Err(_) => {
                let mut calls = calls.borrow_mut();
                calls.disconnected = true;
                calls.pending.clear();
                return;
            }
        }
    }
}

/// Defines `__gateCall`, which `mcp` uses to call tools through the gateway
fn install_call(ctx: &Ctx<'_>, calls: SharedCalls) -> rquickjs::Result<()> {
    let call = Function::new(
        ctx.clone(),
        Async(
            move |server_id: String, tool_name: String, arguments: String| {
                // Everything but waiting happens now, as the script makes the call, so a call it
                // never awaits is still counted and sent
                let registered = register(&calls, server_id, tool_name, &arguments);
                async move {
                    let outcome = match registered {
                        Ok(result) => result.await.unwrap_or_else(|_| {
                            Err("the gateway disconnected before the call's result arrived".into())
                        }),
                        Err(error) => Err(error),
                    };
                    // The JavaScript side reads `{ "Ok": result }` or `{ "Err": message }`
                    serde_json::to_string(&outcome).unwrap_or_else(|error| {
                        format!(r#"{{"Err":"can't read the result: {error}"}}"#)
                    })
                }
            },
        ),
    )?;
    ctx.globals().set("__gateCall", call)
}

fn register(
    calls: &SharedCalls,
    server_id: String,
    tool_name: String,
    arguments: &str,
) -> Result<oneshot::Receiver<CallOutcome>, String> {
    let mut calls = calls.borrow_mut();
    // The gateway enforces these limits too. Checking here means a script can't flood it
    // with calls it would refuse anyway.
    if calls.disconnected {
        return Err("the gateway disconnected".into());
    }
    if calls.made >= MAX_CALLS {
        return Err(format!("the script made more than {MAX_CALLS} tool calls"));
    }
    if calls.pending.len() >= MAX_CALLS_IN_FLIGHT {
        return Err(format!(
            "the script has {MAX_CALLS_IN_FLIGHT} tool calls in flight already; await some before making more"
        ));
    }
    let arguments = serde_json::from_str(arguments)
        .map_err(|error| format!("can't read the tool's arguments: {error}"))?;
    let id = calls.next_id;
    let call = FromWorker::Call {
        id,
        server_id,
        tool_name,
        arguments,
    };
    send(&call).map_err(|error| format!("can't send the tool call: {error}"))?;
    calls.next_id += 1;
    calls.made += 1;
    let (result, receiver) = oneshot::channel();
    calls.pending.insert(id, result);
    Ok(receiver)
}

/// Writes a message to the parent. Blocking is fine: nothing else runs on this thread.
fn send(message: &FromWorker) -> anyhow::Result<()> {
    let bytes = ipc::encode(message).map_err(|error| anyhow!(error))?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.flush()?;
    Ok(())
}

async fn execute(
    code: &str,
    timeout: Duration,
    calls: SharedCalls,
) -> Result<(serde_json::Value, Vec<String>), String> {
    let deadline = Instant::now() + timeout;
    let source = super::typescript::compile(code)?;
    if Instant::now() >= deadline {
        return Err(timed_out(timeout));
    }
    let failed = |error: rquickjs::Error| format!("can't start the JavaScript engine: {error}");
    let runtime = AsyncRuntime::new().map_err(failed)?;
    runtime.set_memory_limit(MEMORY_LIMIT_BYTES).await;
    runtime.set_max_stack_size(STACK_LIMIT_BYTES).await;
    // Stops CPU-bound code. The parent has its own deadline for scripts that just wait.
    runtime
        .set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)))
        .await;
    runtime
        .set_host_promise_rejection_tracker(Some(Box::new(|ctx, promise, reason, handled| {
            if let Ok(track) = ctx.globals().get::<_, Function>("__gateTrackRejection") {
                let _ = track.call::<_, ()>((promise, reason, handled));
            }
        })))
        .await;
    let context = AsyncContext::full(&runtime).await.map_err(failed)?;

    let mut timers = None;
    let outcome = context
        .async_with(async |ctx| {
            let describe = |error: CaughtError<'_>| describe(error, deadline, timeout, &source);
            timers = Some(super::web::install(&ctx).catch(&ctx).map_err(describe)?);
            install_log(&ctx).catch(&ctx).map_err(describe)?;
            install_call(&ctx, calls).catch(&ctx).map_err(describe)?;
            ctx.eval::<(), _>(PRELUDE).catch(&ctx).map_err(describe)?;

            let mut options = EvalOptions::default();
            options.filename = Some(super::typescript::GENERATED_FILENAME.into());
            let promise: Promise = ctx
                .eval_with_options(source.code.as_bytes(), options)
                .catch(&ctx)
                .map_err(describe)?;
            let value: Value = promise.into_future().await.catch(&ctx).map_err(describe)?;
            let globals = ctx.globals();
            let serialize: Function = globals
                .get("__gateSerialize")
                .catch(&ctx)
                .map_err(describe)?;
            let json: String = serialize.call((value,)).catch(&ctx).map_err(describe)?;
            if json.len() > MAX_RESULT_BYTES {
                return Err(format!(
                    "the result is {} bytes, over the {MAX_RESULT_BYTES}-byte limit",
                    json.len()
                ));
            }
            let warnings: Function = globals
                .get("__gateWarnings")
                .catch(&ctx)
                .map_err(describe)?;
            let warnings: Vec<String> = warnings
                .call((MAX_WARNINGS,))
                .catch(&ctx)
                .map_err(describe)?;
            Ok((json, warnings))
        })
        .await;
    if let Some(timers) = timers {
        timers.cancel_all();
    }
    let (json, warnings) = outcome?;
    let result = serde_json::from_str(&json).map_err(|error| format!("invalid result: {error}"))?;
    Ok((result, warnings))
}

/// Defines `__gateLog`, which sends each line to the parent as it's logged
fn install_log(ctx: &Ctx<'_>) -> rquickjs::Result<()> {
    let logged = Rc::new(Cell::new(0));
    let log = Function::new(ctx.clone(), move |ctx: Ctx<'_>, text: String| {
        logged.set(logged.get() + log_cost(&text));
        if logged.get() > MAX_RESULT_BYTES {
            return Err(Exception::throw_message(
                &ctx,
                &format!("the script logged more than the {MAX_RESULT_BYTES}-byte limit"),
            ));
        }
        send(&FromWorker::Log(text))
            .map_err(|error| Exception::throw_message(&ctx, &error.to_string()))
    })?;
    ctx.globals().set("__gateLog", log)
}

fn describe(
    error: CaughtError<'_>,
    deadline: Instant,
    timeout: Duration,
    source: &super::typescript::CompiledScript,
) -> String {
    // The interrupt handler stops the script with an uncatchable error
    if Instant::now() >= deadline {
        return timed_out(timeout);
    }
    match error {
        CaughtError::Error(rquickjs::Error::Allocation) => out_of_memory(),
        CaughtError::Exception(exception)
            if exception.message().as_deref() == Some("out of memory") =>
        {
            out_of_memory()
        }
        // QuickJS sometimes reports a failed allocation as a thrown `null`, but scripts can
        // throw `null` too, so this is only a guess
        CaughtError::Value(value) if value.is_null() => format!(
            "the script threw `null`, possibly because it ran out of memory (the limit is {} MiB)",
            MEMORY_LIMIT_BYTES / 1024 / 1024
        ),
        CaughtError::Exception(exception) => {
            let mut error = String::from("Error:");
            if let Some(message) = exception.message() {
                error.push(' ');
                error.push_str(&message);
            }
            if let Some(stack) = exception.stack() {
                error.push('\n');
                error.push_str(&source.map_stack(&stack));
            }
            error
        }
        error => error.to_string(),
    }
}

fn out_of_memory() -> String {
    format!(
        "the script ran out of memory (the limit is {} MiB)",
        MEMORY_LIMIT_BYTES / 1024 / 1024
    )
}
