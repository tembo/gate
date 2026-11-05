//! `execute_mcp_code`: runs a JavaScript or TypeScript snippet in a fresh worker process and returns its
//! result, so an agent can combine several steps without every intermediate result passing
//! through its context.

mod ipc;
mod typescript;
mod web;
pub mod worker;

use std::{collections::BTreeSet, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use rmcp::model::CallToolResult;
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, ChildStdout, Command},
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use self::ipc::{FromWorker, MAX_MESSAGE_BYTES, ToWorker};
use crate::registry::Registry;

/// Starts gate as a code worker instead of a gateway
pub const WORKER_FLAG: &str = "--code-worker";
/// The most script source a script can have
pub const MAX_SOURCE_BYTES: usize = 256 * 1024;
/// The most a script's output can take once serialized: its result, logs and warnings
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
/// Output kept for warnings, so the one about calls still running when the script ended
/// always fits. The result and logs get the rest.
const WARNINGS_RESERVE_BYTES: usize = 512;
/// The most a script's result and logs can take once serialized
pub const MAX_RESULT_BYTES: usize = MAX_OUTPUT_BYTES - WARNINGS_RESERVE_BYTES;
/// How much of a server or tool name goes in an error that must stay short
const MAX_NAME_IN_ERROR_BYTES: usize = 256;
/// The most of a failed script's error message that's kept, leaving room for its logs
const MAX_ERROR_BYTES: usize = 16 * 1024;
/// How much memory a script's JavaScript values can take
pub const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
/// How long past the script's own deadline the parent waits for the worker. The worker's
/// error is clearer, so it gets the first chance to report a timeout.
const WATCHDOG_GRACE: Duration = Duration::from_secs(1);
/// Messages queued between the worker's pipes and the execution
const QUEUED_MESSAGES: usize = 16;
/// The most tool calls a script can make, and have in flight at once, like the original
/// gateway. Calls past either limit fail rather than wait.
pub const MAX_CALLS: usize = 100;
pub const MAX_CALLS_IN_FLIGHT: usize = 8;
/// How long calls still running when a script ends get to tell their servers they're
/// cancelled, before they're dropped
const CANCEL_GRACE: Duration = Duration::from_secs(2);

/// Makes the command that starts a worker. Tests swap in fake workers.
pub type WorkerCommand = Arc<dyn Fn() -> Command + Send + Sync>;

#[derive(Clone)]
pub struct CodeRunner {
    worker: WorkerCommand,
    timeout: Duration,
    registry: Arc<Registry>,
}

#[derive(Serialize)]
struct Output {
    result: Value,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    logs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

/// What a log entry costs against the output limit: its JSON string and the comma after
/// it. Charging that rather than just its text means even empty entries count.
pub fn log_cost(line: &str) -> usize {
    serde_json::to_string(line).map_or(line.len(), |json| json.len()) + 1
}

pub fn timed_out(timeout: Duration) -> String {
    format!(
        "the script timed out after {}s; tool calls it already made may have taken effect",
        timeout.as_secs()
    )
}

impl CodeRunner {
    /// Runs scripts in workers started from this executable, calling tools through
    /// `registry`
    pub fn new(executable: PathBuf, timeout: Duration, registry: Arc<Registry>) -> Self {
        let worker = Arc::new(move || {
            let mut command = Command::new(&executable);
            command.arg(WORKER_FLAG);
            command
        });
        Self::with_worker(worker, timeout, registry)
    }

    fn with_worker(worker: WorkerCommand, timeout: Duration, registry: Arc<Registry>) -> Self {
        Self {
            worker,
            timeout,
            registry,
        }
    }

    /// Runs a script, returning its output as JSON
    pub async fn run(&self, code: String, ct: &CancellationToken) -> Result<String, String> {
        let ceiling = Arc::new(self.registry.allowed(None, ct).await);
        if ct.is_cancelled() {
            return Err("the script was cancelled".into());
        }
        // The execution runs in its own task, so it still cleans up if this future is
        // dropped: dropping it cancels the execution
        let execution_ct = ct.child_token();
        let _cancel_on_drop = execution_ct.clone().drop_guard();
        let execution = execute(
            self.worker.clone(),
            self.registry.clone(),
            code,
            self.timeout,
            execution_ct,
            ceiling,
        );
        tokio::spawn(execution)
            .await
            .unwrap_or_else(|error| Err(format!("the script's execution failed: {error}")))
    }
}

async fn execute(
    worker: WorkerCommand,
    registry: Arc<Registry>,
    code: String,
    timeout: Duration,
    ct: CancellationToken,
    ceiling: Arc<BTreeSet<String>>,
) -> Result<String, String> {
    let mut command = worker();
    command
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let spawned = {
        let _spawning = crate::spawn::lock();
        command.spawn()
    };
    let mut child = spawned.map_err(|error| format!("can't start the code worker: {error}"))?;
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");

    // Each pipe has its own task, so a worker that stops reading or writing can't hold up
    // the deadline or cancellation
    let (to_worker, outgoing) = mpsc::channel(QUEUED_MESSAGES);
    let writer = tokio::spawn(write_messages(stdin, outgoing));
    let (incoming_sender, mut incoming) = mpsc::channel(QUEUED_MESSAGES);
    let reader = tokio::spawn(read_messages(stdout, incoming_sender));

    let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    let start = ipc::encode(&ToWorker::<Value>::Start { code, timeout_ms })?;
    deliver(&to_worker, start)?;

    let deadline = sleep(timeout + WATCHDOG_GRACE);
    tokio::pin!(deadline);
    let mut logs = Vec::new();
    let mut logged = 0;
    // The script's tool calls, each ending with the message that answers it
    let mut calls = JoinSet::new();
    let mut calls_made = 0;
    let outcome = loop {
        tokio::select! {
            () = &mut deadline => break Err(timed_out(timeout)),
            () = ct.cancelled() => {
                break Err(
                    "the script was cancelled; tool calls it already made may have taken effect"
                        .to_owned(),
                );
            }
            message = incoming.recv() => match message {
                Some(Ok(FromWorker::Log(line))) => {
                    logged += log_cost(&line);
                    if logged > MAX_RESULT_BYTES {
                        break Err(format!("the script logged more than the {MAX_RESULT_BYTES}-byte limit"));
                    }
                    logs.push(line);
                }
                Some(Ok(FromWorker::Call { id, server_id, tool_name, arguments })) => {
                    let refused = if calls_made >= MAX_CALLS {
                        Some(format!("the script made more than {MAX_CALLS} tool calls"))
                    } else if calls.len() >= MAX_CALLS_IN_FLIGHT {
                        Some(format!("the script has more than {MAX_CALLS_IN_FLIGHT} tool calls in flight"))
                    } else {
                        None
                    };
                    if let Some(error) = refused {
                        if let Err(error) = deliver(&to_worker, reply(id, &server_id, &tool_name, Err(error))) {
                            break Err(error);
                        }
                        continue;
                    }
                    calls_made += 1;
                    let call = run_call(registry.clone(), id, server_id, tool_name, arguments, ct.clone(), ceiling.clone());
                    calls.spawn(call);
                }
                Some(Ok(FromWorker::Complete { result, mut warnings })) => {
                    let still_running = calls.len();
                    if still_running > 0 {
                        // First, so it's the last warning left out if the output's too large
                        warnings.insert(0, cancelled_calls(still_running));
                    }
                    break finish(Output { result, logs, warnings }, still_running);
                }
                Some(Ok(FromWorker::Failed { error })) => break Err(failure(&error, &logs)),
                Some(Err(error)) => break Err(format!("bad message from the code worker: {error}")),
                None => {
                    let status = match exit_status(&mut child).await {
                        Some(status) => format!(" ({status})"),
                        None => String::new(),
                    };
                    break Err(format!("the code worker exited without a result{status}"));
                }
            },
            Some(answered) = calls.join_next(), if !calls.is_empty() => match answered {
                Ok(reply) => {
                    if let Err(error) = deliver(&to_worker, reply) {
                        break Err(error);
                    }
                }
                Err(error) => break Err(format!("a tool call failed: {error}")),
            },
        }
    };

    // Whatever happened: stop taking calls, ask the ones still running to cancel, and stop
    // the worker
    ct.cancel();
    drop(to_worker);
    let _ = child.start_kill();
    writer.abort();
    reader.abort();
    // Gives running calls a moment to tell their servers they're cancelled, rather than
    // dropping them mid-request. Nothing is ever retried.
    let drain = async { while calls.join_next().await.is_some() {} };
    let _ = tokio::time::timeout(CANCEL_GRACE, drain).await;
    calls.abort_all();
    let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    outcome
}

/// Queues a message for the worker without waiting. The queue can't fill up: a call keeps
/// its slot in the worker until the worker has read its result, so no more than
/// `MAX_CALLS_IN_FLIGHT` results are ever running or queued at once.
fn deliver(to_worker: &mpsc::Sender<Vec<u8>>, message: Vec<u8>) -> Result<(), String> {
    to_worker.try_send(message).map_err(|error| match error {
        TrySendError::Full(_) => "the code worker isn't reading what the gateway sends".into(),
        TrySendError::Closed(_) => "the code worker stopped reading what the gateway sends".into(),
    })
}

/// Makes one of the script's tool calls, with the execution's cancellation token
async fn run_call(
    registry: Arc<Registry>,
    id: u64,
    server_id: String,
    tool_name: String,
    arguments: Value,
    ct: CancellationToken,
    ceiling: Arc<BTreeSet<String>>,
) -> Vec<u8> {
    if !ceiling.contains(&server_id) {
        return reply(
            id,
            &server_id,
            &tool_name,
            Err(format!("unknown server `{server_id}`")),
        );
    }
    let outcome = match arguments {
        Value::Object(arguments) => registry.call(&server_id, &tool_name, arguments, &ct).await,
        _ => Err("a tool's arguments must be an object".into()),
    };
    reply(id, &server_id, &tool_name, outcome)
}

/// Encodes a call's result for the worker
fn reply(
    id: u64,
    server_id: &str,
    tool_name: &str,
    outcome: Result<CallToolResult, String>,
) -> Vec<u8> {
    let message = ToWorker::CallResult {
        id,
        outcome: outcome.as_ref().map_err(Clone::clone),
    };
    ipc::encode(&message).unwrap_or_else(|_| {
        // The tool ran, so the script mustn't take this for a call that never happened. The
        // names are cut short so this always fits in a message.
        let tool_name = truncate(tool_name, MAX_NAME_IN_ERROR_BYTES);
        let server_id = truncate(server_id, MAX_NAME_IN_ERROR_BYTES);
        let error = match outcome {
            Ok(_) => format!(
                "`{tool_name}` on `{server_id}` ran, but its result exceeds the {MAX_MESSAGE_BYTES}-byte limit, so it couldn't be delivered"
            ),
            Err(_) => format!("calling `{tool_name}` on `{server_id}` failed, with an error too large to deliver"),
        };
        let message = ToWorker::<Value>::CallResult { id, outcome: Err(error) };
        ipc::encode(&message).expect("a short error fits in a message")
    })
}

fn cancelled_calls(count: usize) -> String {
    if count == 1 {
        "1 tool call was still running when the script ended; cancellation was requested, but it may already have taken effect".into()
    } else {
        format!(
            "{count} tool calls were still running when the script ended; cancellation was requested, but they may already have taken effect"
        )
    }
}

/// Waits briefly for a worker that closed its output to exit, for its status
async fn exit_status(child: &mut tokio::process::Child) -> Option<std::process::ExitStatus> {
    timeout(Duration::from_secs(1), child.wait())
        .await
        .ok()?
        .ok()
}

/// Serializes a finished script's output. Its result and logs get `MAX_RESULT_BYTES`, and its
/// warnings what's left: first the one about calls still running, if any, which always fits,
/// then others while they fit, with a note about any left out if that fits too.
fn finish(mut output: Output, still_running: usize) -> Result<String, String> {
    let encode = |output: &Output| {
        serde_json::to_string(output).map_err(|error| format!("can't encode the output: {error}"))
    };
    let warnings = std::mem::take(&mut output.warnings);
    let without_warnings = encode(&output)?;
    if without_warnings.len() > MAX_RESULT_BYTES {
        let mut error = format!(
            "the script's result and logs take {} bytes, over the {MAX_RESULT_BYTES}-byte limit",
            without_warnings.len()
        );
        if still_running > 0 {
            error.push_str(&format!("; {}", cancelled_calls(still_running)));
        }
        return Err(error);
    }
    for kept in (1..=warnings.len()).rev() {
        output.warnings = warnings[..kept].to_vec();
        let left_out = warnings.len() - kept;
        if left_out > 0 {
            output.warnings.push(left_out_note(left_out, " more"));
            let json = encode(&output)?;
            if json.len() <= MAX_OUTPUT_BYTES {
                return Ok(json);
            }
            output.warnings.pop();
        }
        let json = encode(&output)?;
        if json.len() <= MAX_OUTPUT_BYTES {
            return Ok(json);
        }
    }
    // Only reached with no cancelled calls, so no warning that has to be kept
    if !warnings.is_empty() {
        output.warnings = vec![left_out_note(warnings.len(), "")];
        let json = encode(&output)?;
        if json.len() <= MAX_OUTPUT_BYTES {
            return Ok(json);
        }
    }
    Ok(without_warnings)
}

fn left_out_note(count: usize, more: &str) -> String {
    if count == 1 {
        format!("1{more} warning was left out to fit the output limit")
    } else {
        format!("{count}{more} warnings were left out to fit the output limit")
    }
}

/// A failed script's error, followed by the end of what it logged, since that often
/// explains the failure. Both are cut short as needed to fit the output limit.
fn failure(error: &str, logs: &[String]) -> String {
    let mut text = truncate(error, MAX_ERROR_BYTES);
    if logs.is_empty() {
        return text;
    }
    text.push_str("\n\nlogs:");
    // Keeps the last lines that fit, leaving room to say how many were left out
    let mut room = MAX_OUTPUT_BYTES.saturating_sub(text.len() + 64);
    let kept = logs
        .iter()
        .rev()
        .take_while(|line| match room.checked_sub(line.len() + 1) {
            Some(left) => {
                room = left;
                true
            }
            None => false,
        })
        .count();
    let left_out = logs.len() - kept;
    if left_out > 0 {
        text.push_str(&format!("\n… {left_out} earlier lines left out"));
    }
    for line in &logs[left_out..] {
        text.push('\n');
        text.push_str(line);
    }
    text
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let end = text.floor_char_boundary(max);
    format!(
        "{}… ({} more bytes left out)",
        &text[..end],
        text.len() - end
    )
}

/// Writes encoded messages to the worker
async fn write_messages(mut stdin: ChildStdin, mut messages: mpsc::Receiver<Vec<u8>>) {
    while let Some(bytes) = messages.recv().await {
        if stdin.write_all(&bytes).await.is_err() || stdin.flush().await.is_err() {
            return;
        }
    }
}

async fn read_messages(
    mut stdout: ChildStdout,
    messages: mpsc::Sender<Result<FromWorker, String>>,
) {
    loop {
        let message = match ipc::read(&mut stdout).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => return,
            Err(error) => Err(error),
        };
        let failed = message.is_err();
        if messages.send(message).await.is_err() || failed {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde_json::json;

    use super::*;

    /// A runner whose workers run this shell script, with an empty environment
    fn fake_worker(script: String, timeout: Duration) -> CodeRunner {
        // Resolve through the test runner's PATH before the worker's environment
        // is cleared. NixOS does not provide /bin/sleep (or a usable default PATH).
        let sleep = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|directory| directory.join("sleep"))
            .find(|path| path.is_file())
            .expect("sleep must be available on the test runner's PATH");
        let worker = Arc::new(move || {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", &script, "gate-test"]).arg(&sleep);
            command
        });
        let config = crate::config::Config {
            gateway: Default::default(),
            access_policies: Default::default(),
            credential_providers: Default::default(),
            servers: Default::default(),
        };
        let registry = Registry::start(
            Arc::new(config),
            Duration::from_secs(1),
            [Duration::ZERO; 2],
        );
        CodeRunner::with_worker(worker, timeout, Arc::new(registry))
    }

    async fn run(runner: &CodeRunner, code: &str) -> Result<String, String> {
        runner.run(code.into(), &CancellationToken::new()).await
    }

    #[test]
    fn charges_log_entries_their_serialized_size() {
        assert_eq!(log_cost(""), 3);
        assert_eq!(log_cost("hi"), 5);
        // Escaping counts too
        assert_eq!(log_cost("\"\u{0}"), 2 + 2 + 6 + 1);
    }

    #[test]
    fn bounds_failures() {
        let error = "e".repeat(100_000);
        let logs: Vec<String> = (0..10_000).map(|i| format!("line {i}")).collect();
        let text = failure(&error, &logs);
        assert!(text.len() <= MAX_OUTPUT_BYTES, "{}", text.len());
        assert!(text.contains("more bytes left out"));
        assert!(text.contains("earlier lines left out"));
        assert!(text.ends_with("line 9999"));

        // Short ones are left alone
        assert_eq!(
            failure("boom", &["a".into(), "b".into()]),
            "boom\n\nlogs:\na\nb"
        );
        assert_eq!(failure("boom", &[]), "boom");
    }

    /// An output whose result and logs take exactly `bytes` once serialized
    fn output_of(bytes: usize, warnings: Vec<String>) -> Output {
        // `{"result":""}` is 13 bytes
        Output {
            result: Value::String("r".repeat(bytes - 13)),
            logs: vec![],
            warnings,
        }
    }

    #[test]
    fn keeps_the_cancelled_calls_warning_at_the_limit() {
        let warnings = vec![cancelled_calls(1), "w".repeat(1000), "w".repeat(1000)];
        let json = finish(output_of(MAX_RESULT_BYTES, warnings), 1).unwrap();
        assert!(json.len() <= MAX_OUTPUT_BYTES, "{}", json.len());
        let json: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            json["result"].as_str().unwrap().len(),
            MAX_RESULT_BYTES - 13
        );
        assert_eq!(
            json["warnings"],
            json!([cancelled_calls(1), left_out_note(2, " more")])
        );

        // Without a cancelled call, the other warnings can all be left out
        let json = finish(output_of(MAX_RESULT_BYTES, vec!["w".repeat(1000)]), 0).unwrap();
        let json: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            json["warnings"],
            json!(["1 warning was left out to fit the output limit"])
        );
        // And warnings that fit are all kept
        let json = finish(output_of(100, vec!["a".into(), "b".into()]), 0).unwrap();
        let json: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["warnings"], json!(["a", "b"]));
    }

    #[test]
    fn results_over_their_budget_are_errors() {
        let error =
            finish(output_of(MAX_RESULT_BYTES + 1, vec![cancelled_calls(1)]), 1).unwrap_err();
        assert!(error.contains("over the 65024-byte limit"), "{error}");
        assert!(error.contains("cancellation was requested"), "{error}");
    }

    #[tokio::test]
    async fn oversized_results_with_long_names_are_still_delivered_as_errors() {
        let result = CallToolResult::success(vec![rmcp::model::ContentBlock::text(
            "x".repeat(MAX_MESSAGE_BYTES),
        )]);
        let tool_name = "t".repeat(MAX_MESSAGE_BYTES);
        let bytes = reply(1, "s", &tool_name, Ok(result));
        assert!(bytes.len() < 2048, "{}", bytes.len());
        let message: Option<ToWorker> = ipc::read(&mut &bytes[..]).await.unwrap();
        let Some(ToWorker::CallResult {
            id: 1,
            outcome: Err(error),
        }) = message
        else {
            panic!("{message:?}");
        };
        assert!(error.contains("ran, but its result exceeds"), "{error}");
    }

    #[tokio::test]
    async fn reports_bad_messages() {
        let cases = [
            (r"printf '\377\377\377\377'", "over the 4194304-byte limit"),
            (
                r"printf '\000\000\000\005{'",
                "ended in the middle of a message",
            ),
            (r"printf '\000\000\000\003{x}'", "malformed message"),
        ];
        for (script, expected) in cases {
            let runner = fake_worker(script.into(), Duration::from_secs(5));
            let error = run(&runner, "return 1").await.unwrap_err();
            assert!(
                error.starts_with("bad message from the code worker"),
                "{error}"
            );
            assert!(error.contains(expected), "{script}: {error}");
        }
    }

    #[tokio::test]
    async fn reports_workers_that_exit() {
        let runner = fake_worker("exit 3".into(), Duration::from_secs(5));
        let error = run(&runner, "return 1").await.unwrap_err();
        assert!(
            error.starts_with("the code worker exited without a result"),
            "{error}"
        );
        assert!(error.contains('3'), "{error}");
    }

    #[tokio::test]
    async fn keeps_its_deadline_when_the_worker_stops_reading() {
        // The script is far bigger than a pipe's buffer, so writing it blocks forever
        let runner = fake_worker(r#"exec "$1" 30"#.into(), Duration::from_secs(1));
        let started = Instant::now();
        let error = run(&runner, &"x".repeat(1024 * 1024)).await.unwrap_err();
        assert_eq!(error, timed_out(Duration::from_secs(1)));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    /// A worker that writes its PID to a file, then waits
    fn waiting_worker() -> (CodeRunner, tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("pid");
        let script = format!(r#"echo $$ > '{}'; exec "$1" 30"#, pid_file.display());
        (fake_worker(script, Duration::from_secs(30)), dir, pid_file)
    }

    async fn wait_for_pid(pid_file: &std::path::Path) -> String {
        loop {
            if let Ok(pid) = std::fs::read_to_string(pid_file)
                && !pid.trim().is_empty()
            {
                return pid.trim().to_owned();
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits for the process to be gone, reaped as well as killed
    async fn wait_until_gone(pid: &str) {
        let gone = async {
            loop {
                let status = std::process::Command::new("kill")
                    .args(["-0", pid])
                    .stderr(Stdio::null())
                    .status()
                    .unwrap();
                if !status.success() {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        };
        timeout(Duration::from_secs(5), gone)
            .await
            .expect("the worker should be stopped");
    }

    #[tokio::test]
    async fn cancelling_stops_the_worker() {
        let (runner, _dir, pid_file) = waiting_worker();
        let ct = CancellationToken::new();
        let run = runner.run("return 1".into(), &ct);
        let cancel = async {
            let pid = wait_for_pid(&pid_file).await;
            ct.cancel();
            pid
        };
        let (error, pid) = tokio::join!(run, cancel);
        assert!(error.unwrap_err().starts_with("the script was cancelled"));
        wait_until_gone(&pid).await;
    }

    #[tokio::test]
    async fn dropping_a_run_stops_the_worker() {
        let (runner, _dir, pid_file) = waiting_worker();
        let ct = CancellationToken::new();
        let run = runner.run("return 1".into(), &ct);
        tokio::select! {
            _ = run => panic!("the worker never finishes"),
            pid = wait_for_pid(&pid_file) => {
                // Leaving the `select!` drops the run
                wait_until_gone(&pid).await;
            }
        }
    }
}
