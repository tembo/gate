//! The worker side of `execute_mcp_code`: runs one script in QuickJS and reports how it went.
//!
//! The worker is gate itself, started with `--code-worker` and an empty environment. QuickJS
//! has no file, network or process APIs here, but the worker is a separate process, not an
//! OS sandbox.

use std::{
    cell::Cell,
    io::Write,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, anyhow, bail};
use rquickjs::{
    AsyncContext, AsyncRuntime, CatchResultExt, CaughtError, Ctx, Exception, Function, Promise,
    Value,
};

use super::{
    MAX_OUTPUT_BYTES, MEMORY_LIMIT_BYTES,
    ipc::{self, FromWorker, ToWorker},
    log_cost, timed_out,
};

const STACK_LIMIT_BYTES: usize = 256 * 1024;
/// The most unhandled rejections reported as warnings
const MAX_WARNINGS: usize = 10;

/// Sets up `console` and the hooks the worker uses, before the script runs
const PRELUDE: &str = r#"
(() => {
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
    let start = ipc::read(&mut stdin)
        .await
        .map_err(|error| anyhow!(error))?;
    let Some(ToWorker::Start { code, timeout_ms }) = start else {
        bail!("expected a start message");
    };
    let message = match execute(&code, Duration::from_millis(timeout_ms)).await {
        Ok((result, warnings)) => FromWorker::Complete { result, warnings },
        Err(error) => FromWorker::Failed { error },
    };
    send(&message).context("can't send the result")
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
) -> Result<(serde_json::Value, Vec<String>), String> {
    let failed = |error: rquickjs::Error| format!("can't start the JavaScript engine: {error}");
    let runtime = AsyncRuntime::new().map_err(failed)?;
    runtime.set_memory_limit(MEMORY_LIMIT_BYTES).await;
    runtime.set_max_stack_size(STACK_LIMIT_BYTES).await;
    // Stops CPU-bound code. The parent has its own deadline for scripts that just wait.
    let deadline = Instant::now() + timeout;
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

    let outcome = context
        .async_with(async |ctx| {
            let describe = |error: CaughtError<'_>| describe(error, deadline, timeout);
            install_log(&ctx).catch(&ctx).map_err(describe)?;
            ctx.eval::<(), _>(PRELUDE).catch(&ctx).map_err(describe)?;

            let source = format!("(async () => {{\n{code}\n}})()");
            let promise: Promise = ctx.eval(source).catch(&ctx).map_err(describe)?;
            let value: Value = promise.into_future().await.catch(&ctx).map_err(describe)?;
            let globals = ctx.globals();
            let serialize: Function = globals
                .get("__gateSerialize")
                .catch(&ctx)
                .map_err(describe)?;
            let json: String = serialize.call((value,)).catch(&ctx).map_err(describe)?;
            if json.len() > MAX_OUTPUT_BYTES {
                return Err(format!(
                    "the result is {} bytes, over the {MAX_OUTPUT_BYTES}-byte limit",
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
    let (json, warnings) = outcome?;
    let result = serde_json::from_str(&json).map_err(|error| format!("invalid result: {error}"))?;
    Ok((result, warnings))
}

/// Defines `__gateLog`, which sends each line to the parent as it's logged
fn install_log(ctx: &Ctx<'_>) -> rquickjs::Result<()> {
    let logged = Rc::new(Cell::new(0));
    let log = Function::new(ctx.clone(), move |ctx: Ctx<'_>, text: String| {
        logged.set(logged.get() + log_cost(&text));
        if logged.get() > MAX_OUTPUT_BYTES {
            return Err(Exception::throw_message(
                &ctx,
                &format!("the script logged more than the {MAX_OUTPUT_BYTES}-byte limit"),
            ));
        }
        send(&FromWorker::Log(text))
            .map_err(|error| Exception::throw_message(&ctx, &error.to_string()))
    })?;
    ctx.globals().set("__gateLog", log)
}

fn describe(error: CaughtError<'_>, deadline: Instant, timeout: Duration) -> String {
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
        error => error.to_string(),
    }
}

fn out_of_memory() -> String {
    format!(
        "the script ran out of memory (the limit is {} MiB)",
        MEMORY_LIMIT_BYTES / 1024 / 1024
    )
}
