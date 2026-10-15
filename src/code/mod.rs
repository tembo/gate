//! `execute_mcp_code`: runs a JavaScript snippet in a fresh worker process and returns its
//! result, so an agent can combine several steps without every intermediate result passing
//! through its context.

mod ipc;
pub mod worker;

use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, ChildStdout, Command},
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use self::ipc::{FromWorker, ToWorker};

/// Starts gate as a code worker instead of a gateway
pub const WORKER_FLAG: &str = "--code-worker";
/// The most JavaScript source a script can have
pub const MAX_SOURCE_BYTES: usize = 256 * 1024;
/// The most a script's output can take once serialized: its result, logs and warnings
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
/// The most of a failed script's error message that's kept, leaving room for its logs
const MAX_ERROR_BYTES: usize = 16 * 1024;
/// How much memory a script's JavaScript values can take
pub const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
/// How long past the script's own deadline the parent waits for the worker. The worker's
/// error is clearer, so it gets the first chance to report a timeout.
const WATCHDOG_GRACE: Duration = Duration::from_secs(1);
/// Messages queued between the worker's pipes and the execution
const QUEUED_MESSAGES: usize = 16;

/// Makes the command that starts a worker. Tests swap in fake workers.
pub type WorkerCommand = Arc<dyn Fn() -> Command + Send + Sync>;

#[derive(Clone)]
pub struct CodeRunner {
    worker: WorkerCommand,
    timeout: Duration,
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
    /// Runs scripts in workers started from this executable
    pub fn new(executable: PathBuf, timeout: Duration) -> Self {
        let worker = Arc::new(move || {
            let mut command = Command::new(&executable);
            command.arg(WORKER_FLAG);
            command
        });
        Self::with_worker(worker, timeout)
    }

    fn with_worker(worker: WorkerCommand, timeout: Duration) -> Self {
        Self { worker, timeout }
    }

    /// Runs a script, returning its output as JSON
    pub async fn run(&self, code: String, ct: &CancellationToken) -> Result<String, String> {
        // The execution runs in its own task, so it still cleans up if this future is
        // dropped: dropping it cancels the execution
        let execution_ct = ct.child_token();
        let _cancel_on_drop = execution_ct.clone().drop_guard();
        let execution = execute(self.worker.clone(), code, self.timeout, execution_ct);
        tokio::spawn(execution)
            .await
            .unwrap_or_else(|error| Err(format!("the script's execution failed: {error}")))
    }
}

async fn execute(
    worker: WorkerCommand,
    code: String,
    timeout: Duration,
    ct: CancellationToken,
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
    let start = ipc::encode(&ToWorker::Start { code, timeout_ms })?;
    to_worker
        .try_send(start)
        .expect("nothing else has been queued");

    let deadline = sleep(timeout + WATCHDOG_GRACE);
    tokio::pin!(deadline);
    let mut logs = Vec::new();
    let mut logged = 0;
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
                    if logged > MAX_OUTPUT_BYTES {
                        break Err(format!("the script logged more than the {MAX_OUTPUT_BYTES}-byte limit"));
                    }
                    logs.push(line);
                }
                Some(Ok(FromWorker::Complete { result, warnings })) => {
                    break finish(Output { result, logs, warnings });
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
            }
        }
    };

    // Stops the worker whatever happened, and reaps it
    drop(to_worker);
    let _ = child.start_kill();
    writer.abort();
    reader.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    outcome
}

/// Waits briefly for a worker that closed its output to exit, for its status
async fn exit_status(child: &mut tokio::process::Child) -> Option<std::process::ExitStatus> {
    timeout(Duration::from_secs(1), child.wait())
        .await
        .ok()?
        .ok()
}

fn finish(output: Output) -> Result<String, String> {
    let json = serde_json::to_string(&output)
        .map_err(|error| format!("can't encode the output: {error}"))?;
    if json.len() > MAX_OUTPUT_BYTES {
        return Err(format!(
            "the script's output is {} bytes, over the {MAX_OUTPUT_BYTES}-byte limit",
            json.len()
        ));
    }
    Ok(json)
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

    use super::*;

    /// A runner whose workers run this shell script, with an empty environment
    fn fake_worker(script: String, timeout: Duration) -> CodeRunner {
        let worker = Arc::new(move || {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", &script]);
            command
        });
        CodeRunner::with_worker(worker, timeout)
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
        let runner = fake_worker("exec /bin/sleep 30".into(), Duration::from_secs(1));
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
        let script = format!("echo $$ > '{}'; exec /bin/sleep 30", pid_file.display());
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
