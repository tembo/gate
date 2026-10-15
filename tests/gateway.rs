//! Runs the `gate` binary against real downstream servers:
//! 1. another `gate` over stdio
//! 2. in-process server over Streamable HTTP

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::{
    RoleClient, ServerHandler, ServiceExt,
    model::{CallToolRequest, CallToolRequestParams, CallToolResult, ClientRequest, ServerResult},
    service::{PeerRequestOptions, RequestHandle, RunningService},
    tool, tool_handler, tool_router,
    transport::{
        TokioChildProcess,
        streamable_http_server::{StreamableHttpService, session::local::LocalSessionManager},
    },
};
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tempfile::TempDir;

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{ChildStderr, Command},
    sync::{mpsc, watch},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

const GATE: &str = env!("CARGO_BIN_EXE_gate");

struct Gateway {
    client: RunningService<RoleClient, ()>,
    _dir: TempDir,
}

impl Gateway {
    /// Starts `gate` with these servers. `${EMPTY_CONFIG}` points at a config with no servers,
    /// for running `gate` itself as a downstream server
    async fn start(servers: Value, env: &[(&str, &str)]) -> Self {
        Self::spawn(servers, env, &[], Stdio::inherit()).await.0
    }

    /// Like `start`, with extra arguments and control over the gateway's stderr
    async fn spawn(
        servers: Value,
        env: &[(&str, &str)],
        args: &[&str],
        stderr: Stdio,
    ) -> (Self, Option<ChildStderr>) {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("mcp.json");
        let empty_config = dir.path().join("empty.json");
        std::fs::write(&config, json!({ "mcpServers": servers }).to_string()).unwrap();
        std::fs::write(&empty_config, r#"{ "mcpServers": {} }"#).unwrap();

        let mut command = Command::new(GATE);
        command.arg("--config").arg(&config).args(args);
        // Retries quickly, so failing servers reach `failed` without slowing tests down
        if !args.contains(&"--retry-delays") {
            command.args(["--retry-delays", "10,10"]);
        }
        command
            .env("EMPTY_CONFIG", &empty_config)
            .env("GATE", GATE)
            .envs(env.iter().copied());
        let (transport, stderr) = TokioChildProcess::builder(command)
            .stderr(stderr)
            .spawn()
            .unwrap();
        let client = ().serve(transport).await.unwrap();
        (Self { client, _dir: dir }, stderr)
    }

    /// Starts calling a gateway tool without waiting for the result, so it can be cancelled
    async fn start_call(&self, tool: &str, arguments: Value) -> RequestHandle<RoleClient> {
        let params = CallToolRequestParams::new(tool.to_owned())
            .with_arguments(arguments.as_object().unwrap().clone());
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(params));
        self.client
            .send_cancellable_request(request, PeerRequestOptions::no_options())
            .await
            .unwrap()
    }

    async fn call(&self, tool: &str, arguments: Value) -> (Value, bool) {
        let params = CallToolRequestParams::new(tool.to_owned())
            .with_arguments(arguments.as_object().unwrap().clone());
        output(self.client.call_tool(params).await.unwrap())
    }

    async fn details(&self, tools: Value) -> Vec<Value> {
        let (output, is_error) = self
            .call("get_mcp_tool_details", json!({ "tools": tools }))
            .await;
        assert!(!is_error, "{output}");
        output["results"].as_array().unwrap().clone()
    }

    async fn servers(&self) -> Value {
        self.call("list_mcp_servers", json!({})).await.0["servers"].clone()
    }

    /// Polls `list_mcp_servers` until the server has this status, returning its entry
    async fn wait_for_status(&self, server_id: &str, status: &str) -> Value {
        let poll = async {
            loop {
                let servers = self.servers().await;
                let server = servers
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|server| server["serverId"] == server_id)
                    .unwrap()
                    .clone();
                if server["status"] == status {
                    return server;
                }
                sleep(Duration::from_millis(20)).await;
            }
        };
        timeout(Duration::from_secs(10), poll)
            .await
            .unwrap_or_else(|_| panic!("`{server_id}` should become {status}"))
    }
}

/// Turns a gateway tool's result into its JSON output, and whether it was an error
fn output(result: CallToolResult) -> (Value, bool) {
    let text = result.content[0].as_text().unwrap().text.clone();
    let output = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (output, result.is_error == Some(true))
}

/// Waits for a call started with `Gateway::start_call`
async fn finish_call(call: RequestHandle<RoleClient>) -> (Value, bool) {
    match call.await_response().await.unwrap() {
        ServerResult::CallToolResult(result) => output(result),
        response => panic!("unexpected response: {response:?}"),
    }
}

/// A server that runs a shell script. The script sees the gateway's environment, including
/// `$GATE` and `$EMPTY_CONFIG`.
fn script(script: &str) -> Value {
    json!({ "command": "sh", "args": ["-c", script] })
}

/// Waits for the server to write a PID to `pid_file` other than `old`
async fn wait_for_new_pid(pid_file: &std::path::Path, old: &str) -> String {
    let poll = async {
        loop {
            match std::fs::read_to_string(pid_file) {
                Ok(pid) if pid != old => return pid,
                _ => sleep(Duration::from_millis(20)).await,
            }
        }
    };
    timeout(Duration::from_secs(10), poll)
        .await
        .expect("the server should restart")
}

fn kill(pid: &str) {
    let status = std::process::Command::new("kill")
        .args(["-9", pid.trim()])
        .status()
        .unwrap();
    assert!(status.success());
}

fn nested_gate() -> Value {
    json!({ "command": GATE, "args": ["--config", "${EMPTY_CONFIG}"], "description": "Nested" })
}

#[tokio::test]
async fn discovers_stdio_tools() {
    let gateway = Gateway::start(json!({ "inner": nested_gate() }), &[]).await;

    let results = gateway
        .details(json!([
            { "serverId": "inner", "toolName": "list_mcp_servers" },
            { "serverId": "inner", "toolName": "nope" },
            { "serverId": "missing", "toolName": "list_mcp_servers" },
        ]))
        .await;
    assert_eq!(results[0]["serverId"], "inner");
    assert_eq!(results[0]["tool"]["name"], "list_mcp_servers");
    assert!(results[0]["tool"]["inputSchema"].is_object());
    assert_eq!(results[1]["error"], "server `inner` has no tool `nope`");
    assert_eq!(results[2]["error"], "unknown server `missing`");

    assert_eq!(
        gateway.servers().await,
        json!([{
            "serverId": "inner",
            "transport": "stdio",
            "description": "Nested",
            "status": "ready",
            "toolCount": 3,
        }])
    );
}

#[tokio::test]
async fn reports_failed_servers() {
    let gateway = Gateway::start(
        json!({
            "crashes": { "command": "sh", "args": ["-c", "echo boom >&2; exit 1"] },
            "missing": { "command": "/nonexistent/server" },
            "inner": nested_gate(),
        }),
        &[],
    )
    .await;
    gateway.wait_for_status("crashes", "failed").await;
    gateway.wait_for_status("missing", "failed").await;

    let results = gateway
        .details(json!([
            { "serverId": "crashes", "toolName": "x" },
            { "serverId": "missing", "toolName": "x" },
            { "serverId": "inner", "toolName": "call_mcp_tool" },
        ]))
        .await;
    let crashed = results[0]["error"].as_str().unwrap();
    assert!(crashed.contains("server `crashes` failed"), "{crashed}");
    assert!(
        crashed.contains("boom"),
        "stderr should explain the failure: {crashed}"
    );
    let missing = results[1]["error"].as_str().unwrap();
    assert!(
        missing.contains("failed to start `/nonexistent/server`"),
        "{missing}"
    );
    // One server failing doesn't affect the others
    assert_eq!(results[2]["tool"]["name"], "call_mcp_tool");

    let servers = gateway.servers().await;
    let statuses: Vec<_> = servers
        .as_array()
        .unwrap()
        .iter()
        .map(|server| (server["serverId"].clone(), server["status"].clone()))
        .collect();
    assert_eq!(
        statuses,
        [
            (json!("crashes"), json!("failed")),
            (json!("inner"), json!("ready")),
            (json!("missing"), json!("failed")),
        ]
    );
}

#[tokio::test]
async fn limits_batch_size() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let item = json!({ "serverId": "s", "toolName": "t" });

    let (_, is_error) = gateway
        .call("get_mcp_tool_details", json!({ "tools": [] }))
        .await;
    assert!(is_error);
    let (_, is_error) = gateway
        .call("get_mcp_tool_details", json!({ "tools": vec![item; 21] }))
        .await;
    assert!(is_error);
}

/// What happened to `Echo`'s `sleep` tool. Every event is kept, so tests can count them.
#[derive(Debug, PartialEq)]
enum Event {
    Started,
    Cancelled,
}

#[derive(Clone)]
struct Echo {
    events: mpsc::UnboundedSender<Event>,
}

#[tool_router]
impl Echo {
    #[tool(description = "Echoes")]
    async fn echo(&self) -> String {
        "echo".into()
    }

    #[tool(description = "Sleeps until cancelled")]
    async fn sleep(&self, ct: CancellationToken) -> String {
        let _ = self.events.send(Event::Started);
        tokio::select! {
            () = sleep(Duration::from_secs(30)) => "slept".into(),
            () = ct.cancelled() => {
                let _ = self.events.send(Event::Cancelled);
                "cancelled".into()
            }
        }
    }
}

#[tool_handler]
impl ServerHandler for Echo {}

struct EchoServer {
    url: String,
    events: mpsc::UnboundedReceiver<Event>,
    /// Requests are held until this is `true`, to keep the gateway connecting
    open: watch::Sender<bool>,
}

impl EchoServer {
    async fn next_event(&mut self) -> Event {
        timeout(Duration::from_secs(5), self.events.recv())
            .await
            .expect("`sleep` should report an event")
            .unwrap()
    }

    /// Collects events until there's been none for a second
    async fn remaining_events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = timeout(Duration::from_secs(1), self.events.recv()).await {
            events.push(event);
        }
        events
    }
}

/// Serves `Echo` over Streamable HTTP, only to requests with `Authorization: Bearer test-token`
async fn serve_echo() -> EchoServer {
    async fn require_token(request: Request, next: Next) -> Response {
        let authorized = request
            .headers()
            .get("authorization")
            .is_some_and(|value| value == "Bearer test-token");
        if authorized {
            next.run(request).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        }
    }

    async fn hold(
        State(mut open): State<watch::Receiver<bool>>,
        request: Request,
        next: Next,
    ) -> Response {
        let _ = open.wait_for(|open| *open).await;
        next.run(request).await
    }

    let (sender, events) = mpsc::unbounded_channel();
    let (open, open_receiver) = watch::channel(true);
    let service: StreamableHttpService<Echo, LocalSessionManager> = StreamableHttpService::new(
        move || {
            Ok(Echo {
                events: sender.clone(),
            })
        },
        Default::default(),
        Default::default(),
    );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(require_token))
        .layer(middleware::from_fn_with_state(open_receiver, hold));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    EchoServer { url, events, open }
}

fn echo(url: &str) -> Value {
    json!({ "url": url, "headers": { "Authorization": "Bearer ${TOKEN}" } })
}

const TOKEN: (&str, &str) = ("TOKEN", "test-token");

#[tokio::test]
async fn discovers_http_tools_with_headers() {
    let url = serve_echo().await.url;
    let gateway = Gateway::start(
        json!({
            "echo": { "url": url, "headers": { "Authorization": "Bearer ${TOKEN}" } },
            "denied": { "url": url },
        }),
        &[("TOKEN", "test-token")],
    )
    .await;
    gateway.wait_for_status("denied", "failed").await;

    let results = gateway
        .details(json!([
            { "serverId": "echo", "toolName": "echo" },
            { "serverId": "denied", "toolName": "echo" },
        ]))
        .await;
    assert_eq!(results[0]["tool"]["name"], "echo");
    let denied = results[1]["error"].as_str().unwrap();
    assert!(denied.contains("server `denied` failed"), "{denied}");

    let servers = gateway.servers().await;
    assert_eq!(servers[1]["serverId"], "echo");
    assert_eq!(servers[1]["transport"], "http");
    assert_eq!(servers[1]["toolCount"], 2);
}

#[tokio::test]
async fn connection_errors_hide_url_secrets() {
    // Nothing listens on port 1, so connecting fails with a transport error
    let gateway = Gateway::start(
        json!({
            "query": { "url": "http://127.0.0.1:1/mcp?key=${TOKEN}" },
            // The HTTP client strips userinfo from URLs in its errors
            "userinfo": { "url": "http://user:${TOKEN}@127.0.0.1:1/mcp?key=${TOKEN}" },
        }),
        &[("TOKEN", "hunter2")],
    )
    .await;
    gateway.wait_for_status("query", "failed").await;
    gateway.wait_for_status("userinfo", "failed").await;

    let results = gateway
        .details(json!([
            { "serverId": "query", "toolName": "x" },
            { "serverId": "userinfo", "toolName": "x" },
        ]))
        .await;
    for result in results {
        let error = result["error"].as_str().unwrap();
        assert!(error.contains("http://127.0.0.1:1"), "{error}");
        assert!(!error.contains("hunter2"), "{error}");
    }
    let servers = gateway.servers().await.to_string();
    assert!(!servers.contains("hunter2"), "{servers}");
}

#[tokio::test]
async fn bounds_stderr_without_newlines() {
    let gateway = Gateway::start(
        json!({ "noisy": {
            "command": "sh",
            "args": ["-c", "head -c 100000 /dev/zero | tr '\\0' x >&2; exit 1"],
        } }),
        &[],
    )
    .await;
    gateway.wait_for_status("noisy", "failed").await;

    let results = gateway
        .details(json!([{ "serverId": "noisy", "toolName": "x" }]))
        .await;
    let error = results[0]["error"].as_str().unwrap();
    assert!(error.ends_with("xxxx"), "{error}");
    // Only the end of stderr is kept, plus the error itself
    assert!(error.len() < 4096 + 200, "{} bytes", error.len());
}

#[tokio::test]
async fn forwards_stderr_before_newlines() {
    // Writes 10000 bytes with no newline, then stays up without answering
    let (_gateway, stderr) = Gateway::spawn(
        json!({ "noisy": {
            "command": "sh",
            "args": ["-c", "head -c 10000 /dev/zero | tr '\\0' x >&2; sleep 10"],
        } }),
        &[],
        &[],
        Stdio::piped(),
    )
    .await;

    let mut lines = BufReader::new(stderr.unwrap()).lines();
    for _ in 0..2 {
        let line = timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("stderr should be forwarded in pieces, before a newline or EOF")
            .unwrap()
            .unwrap();
        assert_eq!(line, format!("[mcp:noisy] {}", "x".repeat(4096)));
    }
}

#[tokio::test]
async fn forwards_calls() {
    let url = serve_echo().await.url;
    let gateway = Gateway::start(
        json!({
            "inner": nested_gate(),
            "echo": { "url": url, "headers": { "Authorization": "Bearer ${TOKEN}" } },
        }),
        &[("TOKEN", "test-token")],
    )
    .await;

    // Over HTTP, leaving out `arguments`
    let output = gateway
        .call(
            "call_mcp_tool",
            json!({ "serverId": "echo", "toolName": "echo" }),
        )
        .await;
    assert_eq!(output, (json!("echo"), false));

    // Over stdio, with arguments
    let (output, is_error) = gateway
        .call(
            "call_mcp_tool",
            json!({
                "serverId": "inner",
                "toolName": "get_mcp_tool_details",
                "arguments": { "tools": [{ "serverId": "s", "toolName": "t" }] },
            }),
        )
        .await;
    assert!(!is_error, "{output}");
    assert_eq!(output["results"][0]["error"], "unknown server `s`");

    // The server's own errors are passed through
    let output = gateway
        .call(
            "call_mcp_tool",
            json!({
                "serverId": "inner",
                "toolName": "get_mcp_tool_details",
                "arguments": { "tools": [] },
            }),
        )
        .await;
    assert_eq!(
        output,
        (json!("`tools` must have between 1 and 20 items"), true)
    );
}

#[tokio::test]
async fn reports_call_errors() {
    let gateway = Gateway::start(
        json!({
            "inner": nested_gate(),
            "crashes": { "command": "sh", "args": ["-c", "exit 1"] },
        }),
        &[],
    )
    .await;
    gateway.wait_for_status("crashes", "failed").await;
    let call = |server_id: &str, tool_name: &str| {
        let arguments = json!({ "serverId": server_id, "toolName": tool_name });
        async { gateway.call("call_mcp_tool", arguments).await }
    };

    let (output, is_error) = call("missing", "t").await;
    assert!(is_error);
    assert_eq!(output, "unknown server `missing`");

    let (output, is_error) = call("crashes", "t").await;
    assert!(is_error);
    let output = output.as_str().unwrap();
    assert!(output.starts_with("server `crashes` failed"), "{output}");

    // Tools aren't checked against the catalog, so the server reports unknown ones
    let (output, is_error) = call("inner", "nope").await;
    assert!(is_error);
    let output = output.as_str().unwrap();
    assert!(
        output.starts_with("calling `nope` on `inner` failed"),
        "{output}"
    );
}

#[tokio::test]
async fn times_out_calls() {
    let mut server = serve_echo().await;
    let (gateway, _) = Gateway::spawn(
        json!({ "echo": echo(&server.url) }),
        &[TOKEN],
        &["--call-timeout", "1"],
        Stdio::inherit(),
    )
    .await;

    let output = gateway
        .call(
            "call_mcp_tool",
            json!({ "serverId": "echo", "toolName": "sleep" }),
        )
        .await;
    assert_eq!(
        output,
        (
            json!(
                "calling `sleep` on `echo` timed out after 1s; the downstream outcome is unknown"
            ),
            true
        )
    );
    // rmcp tells the server to stop
    assert_eq!(server.next_event().await, Event::Started);
    assert_eq!(server.next_event().await, Event::Cancelled);
}

#[tokio::test]
async fn forwards_cancellation() {
    let mut server = serve_echo().await;
    let gateway = Gateway::start(json!({ "echo": echo(&server.url) }), &[TOKEN]).await;

    let call = gateway
        .start_call(
            "call_mcp_tool",
            json!({ "serverId": "echo", "toolName": "sleep" }),
        )
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    call.cancel(None).await.unwrap();
    assert_eq!(server.next_event().await, Event::Cancelled);
}

#[tokio::test]
async fn cancelled_calls_are_not_dispatched_after_connecting() {
    let mut server = serve_echo().await;
    // Keeps the gateway connecting until the call is cancelled
    server.open.send_replace(false);
    let gateway = Gateway::start(json!({ "echo": echo(&server.url) }), &[TOKEN]).await;

    let call = gateway
        .start_call(
            "call_mcp_tool",
            json!({ "serverId": "echo", "toolName": "sleep" }),
        )
        .await;
    // Gives the gateway time to start waiting for the connection
    sleep(Duration::from_millis(300)).await;
    call.cancel(None).await.unwrap();
    server.open.send_replace(true);

    // Waits for the connection, which other requests can still use
    let results = gateway
        .details(json!([{ "serverId": "echo", "toolName": "sleep" }]))
        .await;
    assert_eq!(results[0]["tool"]["name"], "sleep");
    assert_eq!(
        server.remaining_events().await,
        [],
        "the cancelled call shouldn't be dispatched"
    );
}

#[tokio::test]
async fn retries_until_connected() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("failed-once");
    let gateway = Gateway::start(
        json!({ "flaky": script(r#"
            if [ -e "$MARKER" ]; then exec "$GATE" --config "$EMPTY_CONFIG"; fi
            touch "$MARKER"; exit 1
        "#) }),
        &[("MARKER", marker.to_str().unwrap())],
    )
    .await;

    let server = gateway.wait_for_status("flaky", "ready").await;
    assert_eq!(server["toolCount"], 3);
}

#[tokio::test]
async fn waits_for_the_current_attempt_but_not_retries() {
    let dir = tempfile::tempdir().unwrap();
    let release = dir.path().join("release");
    // Fails every attempt, but holds the first until the test releases it
    let (gateway, _) = Gateway::spawn(
        json!({ "held": script(r#"while [ ! -e "$RELEASE" ]; do sleep 0.02; done; exit 1"#) }),
        &[("RELEASE", release.to_str().unwrap())],
        &["--retry-delays", "10000,10000"],
        Stdio::inherit(),
    )
    .await;

    let waiting = gateway
        .start_call(
            "get_mcp_tool_details",
            json!({ "tools": [{ "serverId": "held", "toolName": "x" }] }),
        )
        .await;
    // A round trip after sending the request gets it to the gateway first. That doesn't prove
    // it has started waiting, but if it hadn't, it'd get "still connecting" and fail below,
    // rather than pass by accident.
    gateway.servers().await;
    std::fs::write(&release, "").unwrap();

    let (output, _) = finish_call(waiting).await;
    let error = output["results"][0]["error"].as_str().unwrap();
    assert!(error.starts_with("server `held` failed: "), "{error}");
    assert!(error.ends_with("; retrying in 10s"), "{error}");

    // The server is waiting to retry, so new requests don't wait
    let results = gateway
        .details(json!([{ "serverId": "held", "toolName": "x" }]))
        .await;
    assert_eq!(results[0]["error"], "server `held` is still connecting");
}

#[tokio::test]
async fn gives_up_after_three_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = dir.path().join("attempts");
    let gateway = Gateway::start(
        json!({ "broken": script(r#"echo attempt >> "$ATTEMPTS"; exit 1"#) }),
        &[("ATTEMPTS", attempts.to_str().unwrap())],
    )
    .await;

    gateway.wait_for_status("broken", "failed").await;
    // Nothing tries again after that
    sleep(Duration::from_millis(200)).await;
    let attempts = std::fs::read_to_string(&attempts).unwrap();
    assert_eq!(attempts.lines().count(), 3);

    let results = gateway
        .details(json!([{ "serverId": "broken", "toolName": "x" }]))
        .await;
    let error = results[0]["error"].as_str().unwrap();
    assert!(error.starts_with("server `broken` failed: "), "{error}");
    assert!(!error.contains("retrying"), "{error}");
}

/// Writes the shell's PID, then becomes `gate`, with `$CONFIG` or no servers. `exec` keeps the
/// PID, so killing it stops `gate` itself rather than leaving it running.
const RECORDS_PID: &str =
    r#"echo $$ > "$PID_FILE"; exec "$GATE" --config "${CONFIG:-$EMPTY_CONFIG}""#;

#[tokio::test]
async fn reconnects_when_a_connection_closes() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let gateway = Gateway::start(
        json!({ "inner": script(RECORDS_PID) }),
        &[("PID_FILE", pid_file.to_str().unwrap())],
    )
    .await;
    gateway.wait_for_status("inner", "ready").await;

    let first = std::fs::read_to_string(&pid_file).unwrap();
    kill(&first);
    wait_for_new_pid(&pid_file, &first).await;

    gateway.wait_for_status("inner", "ready").await;
    let results = gateway
        .details(json!([{ "serverId": "inner", "toolName": "list_mcp_servers" }]))
        .await;
    assert_eq!(results[0]["tool"]["name"], "list_mcp_servers");
}

#[tokio::test]
async fn reconnecting_does_not_replay_calls() {
    let mut server = serve_echo().await;
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let inner_config = dir.path().join("inner.json");
    let inner_servers = json!({ "mcpServers": { "echo": echo(&server.url) } });
    std::fs::write(&inner_config, inner_servers.to_string()).unwrap();
    // The test calls `Echo` through two gateways, and kills the inner one mid-call
    let gateway = Gateway::start(
        json!({ "inner": script(RECORDS_PID) }),
        &[
            TOKEN,
            ("PID_FILE", pid_file.to_str().unwrap()),
            ("CONFIG", inner_config.to_str().unwrap()),
        ],
    )
    .await;
    gateway.wait_for_status("inner", "ready").await;
    let through_inner = |tool_name: &str| {
        json!({
            "serverId": "inner",
            "toolName": "call_mcp_tool",
            "arguments": { "serverId": "echo", "toolName": tool_name },
        })
    };

    let call = gateway
        .start_call("call_mcp_tool", through_inner("sleep"))
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    let first = std::fs::read_to_string(&pid_file).unwrap();
    kill(&first);

    let (output, is_error) = finish_call(call).await;
    assert!(is_error, "{output}");
    let error = output.as_str().unwrap();
    assert!(
        error.starts_with("calling `call_mcp_tool` on `inner` failed"),
        "{error}"
    );

    wait_for_new_pid(&pid_file, &first).await;
    gateway.wait_for_status("inner", "ready").await;
    let output = gateway.call("call_mcp_tool", through_inner("echo")).await;
    assert_eq!(output, (json!("echo"), false));
    let events = server.remaining_events().await;
    assert!(!events.contains(&Event::Started), "{events:?}");
}

/// Counts attempts in `$ATTEMPTS`, leaving the attempt's number in `$n`
const COUNTS_ATTEMPTS: &str =
    r#"n=$(($(cat "$ATTEMPTS" 2>/dev/null || echo 0) + 1)); echo $n > "$ATTEMPTS""#;

fn attempts(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap().trim().to_owned()
}

#[tokio::test]
async fn retries_failed_servers_on_request() {
    let dir = tempfile::tempdir().unwrap();
    let attempts_file = dir.path().join("attempts");
    // Fails the three background attempts, then works
    let gateway = Gateway::start(
        json!({ "late": script(&format!(r#"
            {COUNTS_ATTEMPTS}
            if [ $n -ge 4 ]; then exec "$GATE" --config "$EMPTY_CONFIG"; fi
            exit 1
        "#)) }),
        &[("ATTEMPTS", attempts_file.to_str().unwrap())],
    )
    .await;
    gateway.wait_for_status("late", "failed").await;

    let results = gateway
        .details(json!([{ "serverId": "late", "toolName": "list_mcp_servers" }]))
        .await;
    assert_eq!(results[0]["tool"]["name"], "list_mcp_servers");
    assert_eq!(
        gateway.wait_for_status("late", "ready").await["toolCount"],
        3
    );
    assert_eq!(attempts(&attempts_file), "4");
}

#[tokio::test]
async fn concurrent_requests_share_a_retry() {
    let dir = tempfile::tempdir().unwrap();
    let attempts_file = dir.path().join("attempts");
    let release = dir.path().join("release");
    // Always fails, but holds attempts after the background ones until the test releases them
    let gateway = Gateway::start(
        json!({ "broken": script(&format!(r#"
            {COUNTS_ATTEMPTS}
            if [ $n -ge 4 ]; then
                while [ ! -e "$RELEASE" ]; do sleep 0.02; done
                echo "attempt $n failed" >&2
            fi
            exit 1
        "#)) }),
        &[
            ("ATTEMPTS", attempts_file.to_str().unwrap()),
            ("RELEASE", release.to_str().unwrap()),
        ],
    )
    .await;
    gateway.wait_for_status("broken", "failed").await;

    let request = json!({ "tools": [{ "serverId": "broken", "toolName": "x" }] });
    let first = gateway
        .start_call("get_mcp_tool_details", request.clone())
        .await;
    let second = gateway.start_call("get_mcp_tool_details", request).await;
    // Gets both requests to the gateway before the attempt fails. If either arrived later,
    // it'd start its own attempt, and the count below would be off.
    gateway.servers().await;
    std::fs::write(&release, "").unwrap();

    let (first, _) = finish_call(first).await;
    let (second, _) = finish_call(second).await;
    let error = first["results"][0]["error"].as_str().unwrap();
    assert!(error.starts_with("server `broken` failed: "), "{error}");
    assert!(error.contains("attempt 4 failed"), "{error}");
    assert_eq!(first, second);
    assert_eq!(attempts(&attempts_file), "4");
    gateway.wait_for_status("broken", "failed").await;
}

#[tokio::test]
async fn limits_background_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let release = dir.path().join("release");
    let held = script(r#"while [ ! -e "$RELEASE" ]; do sleep 0.02; done; exit 1"#);
    // The four held servers come first in config order, so they take every permit
    let gateway = Gateway::start(
        json!({
            "a1": held, "a2": held, "a3": held, "a4": held,
            "z": nested_gate(),
        }),
        &[("RELEASE", release.to_str().unwrap())],
    )
    .await;

    // Queued for a permit, so requests fail fast
    let results = gateway
        .details(json!([{ "serverId": "z", "toolName": "list_mcp_servers" }]))
        .await;
    assert_eq!(results[0]["error"], "server `z` is still connecting");

    std::fs::write(&release, "").unwrap();
    gateway.wait_for_status("z", "ready").await;
}

#[tokio::test]
async fn batches_retry_each_server_once() {
    let dir = tempfile::tempdir().unwrap();
    let attempts_file = dir.path().join("attempts");
    let gateway = Gateway::start(
        json!({ "broken": script(&format!("{COUNTS_ATTEMPTS}; exit 1")) }),
        &[("ATTEMPTS", attempts_file.to_str().unwrap())],
    )
    .await;
    gateway.wait_for_status("broken", "failed").await;

    let results = gateway
        .details(json!([
            { "serverId": "broken", "toolName": "a" },
            { "serverId": "broken", "toolName": "b" },
            { "serverId": "broken", "toolName": "c" },
        ]))
        .await;
    assert_eq!(attempts(&attempts_file), "4");
    assert_eq!(results[0]["error"], results[1]["error"]);
    assert_eq!(results[1]["error"], results[2]["error"]);
}

#[tokio::test]
async fn batches_retry_servers_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let release = dir.path().join("release");
    // Each server holds its retry until the test releases them
    let server = |name: &str| {
        let attempts = dir.path().join(name);
        script(&format!(
            r#"ATTEMPTS="{}"; {COUNTS_ATTEMPTS}
            if [ $n -ge 4 ]; then while [ ! -e "$RELEASE" ]; do sleep 0.02; done; fi
            exit 1"#,
            attempts.display()
        ))
    };
    let gateway = Gateway::start(
        json!({ "first": server("first"), "second": server("second") }),
        &[("RELEASE", release.to_str().unwrap())],
    )
    .await;
    gateway.wait_for_status("first", "failed").await;
    gateway.wait_for_status("second", "failed").await;

    let call = gateway
        .start_call(
            "get_mcp_tool_details",
            json!({ "tools": [
                { "serverId": "first", "toolName": "x" },
                { "serverId": "second", "toolName": "x" },
            ] }),
        )
        .await;
    // Both retries start before either finishes
    let both_started = async {
        while ["first", "second"].map(|name| attempts(&dir.path().join(name))) != ["4", "4"] {
            sleep(Duration::from_millis(20)).await;
        }
    };
    timeout(Duration::from_secs(10), both_started)
        .await
        .expect("both servers should retry at once");
    std::fs::write(&release, "").unwrap();

    let (output, _) = finish_call(call).await;
    for result in output["results"].as_array().unwrap() {
        let error = result["error"].as_str().unwrap();
        assert!(error.contains("failed"), "{error}");
    }
}
