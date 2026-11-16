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
    handler::server::wrapper::Parameters,
    model::{CallToolRequest, CallToolRequestParams, CallToolResult, ClientRequest, ServerResult},
    service::{PeerRequestOptions, RequestHandle, RunningService},
    tool, tool_handler, tool_router,
    transport::{
        TokioChildProcess,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use serde_json::{Value, json};
use std::{process::Stdio, sync::Arc, time::Duration};
use tempfile::TempDir;

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{ChildStderr, Command},
    sync::{Notify, mpsc, watch},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

const GATE: &str = env!("CARGO_BIN_EXE_tembo-gate");

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
        Self::spawn_config(json!({ "mcpServers": servers }), env, args, stderr).await
    }

    async fn spawn_config(
        configuration: Value,
        env: &[(&str, &str)],
        args: &[&str],
        stderr: Stdio,
    ) -> (Self, Option<ChildStderr>) {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("mcp.json");
        let empty_config = dir.path().join("empty.json");
        std::fs::write(&config, configuration.to_string()).unwrap();
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
            "toolCount": 5,
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
    /// `held` calls wait until this is `true`
    release: watch::Receiver<bool>,
    /// Notified when `sleep` or `held` starts
    started: Arc<Notify>,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Held {
    n: u64,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Nap {
    ms: u64,
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
        self.started.notify_one();
        tokio::select! {
            () = sleep(Duration::from_secs(30)) => "slept".into(),
            () = ct.cancelled() => {
                let _ = self.events.send(Event::Cancelled);
                "cancelled".into()
            }
        }
    }

    #[tool(description = "Waits until the test releases it, then returns `n`")]
    async fn held(
        &self,
        Parameters(Held { n }): Parameters<Held>,
        ct: CancellationToken,
    ) -> String {
        let _ = self.events.send(Event::Started);
        self.started.notify_one();
        let mut release = self.release.clone();
        tokio::select! {
            _ = release.wait_for(|released| *released) => n.to_string(),
            () = ct.cancelled() => {
                let _ = self.events.send(Event::Cancelled);
                "cancelled".into()
            }
        }
    }

    #[tool(description = "Sleeps for `ms` milliseconds")]
    async fn nap(&self, Parameters(Nap { ms }): Parameters<Nap>) -> String {
        sleep(Duration::from_millis(ms)).await;
        "napped".into()
    }

    #[tool(description = "Waits until `sleep` or `held` has started")]
    async fn until_started(&self) -> String {
        self.started.notified().await;
        "started".into()
    }

    #[tool(description = "Returns its arguments as JSON")]
    async fn reflect(
        &self,
        Parameters(arguments): Parameters<serde_json::Map<String, Value>>,
    ) -> String {
        serde_json::to_string(&arguments).unwrap()
    }

    #[tool(description = "Returns 5 MiB of text")]
    async fn big(&self) -> String {
        "x".repeat(5 * 1024 * 1024)
    }

    #[tool(name = "then", description = "Named like a promise's method")]
    async fn then_tool(&self) -> String {
        "then".into()
    }
}

#[tool_handler]
impl ServerHandler for Echo {}

struct EchoServer {
    url: String,
    events: mpsc::UnboundedReceiver<Event>,
    /// Requests are held until this is `true`, to keep the gateway connecting
    open: watch::Sender<bool>,
    /// Releases `held` calls
    release: watch::Sender<bool>,
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

/// How `Echo` answers requests over HTTP
#[derive(Clone, Copy, Debug)]
enum Responses {
    /// Opens an SSE stream for each response, rmcp's default
    Sse,
    /// Answers with plain JSON, without a session
    Json,
}

/// Serves `Echo` over Streamable HTTP, only to requests with `Authorization: Bearer test-token`
async fn serve_echo() -> EchoServer {
    serve_echo_with(Responses::Sse).await
}

async fn serve_echo_with(responses: Responses) -> EchoServer {
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
    let (release, release_receiver) = watch::channel(false);
    let started = Arc::new(Notify::new());
    let service: StreamableHttpService<Echo, LocalSessionManager> = StreamableHttpService::new(
        move || {
            Ok(Echo {
                events: sender.clone(),
                release: release_receiver.clone(),
                started: started.clone(),
            })
        },
        Default::default(),
        match responses {
            Responses::Sse => StreamableHttpServerConfig::default(),
            Responses::Json => StreamableHttpServerConfig::default()
                .with_legacy_session_mode(false)
                .with_json_response(true),
        },
    );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(require_token))
        .layer(middleware::from_fn_with_state(open_receiver, hold));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    EchoServer {
        url,
        events,
        open,
        release,
    }
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
    assert_eq!(servers[1]["toolCount"], 8);
}

/// A forward proxy, as tailscaled's userspace HTTP proxy is. Records each request's URL.
async fn serve_forward_proxy() -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
    let forwarded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let router = axum::Router::new().fallback({
        let forwarded = forwarded.clone();
        move |request: Request| async move {
            let url = request.uri().to_string();
            forwarded.lock().unwrap().push(url.clone());
            let (parts, body) = request.into_parts();
            let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            let upstream = client
                .request(parts.method, &url)
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .unwrap();
            let mut response = Response::builder().status(upstream.status());
            for (name, value) in upstream.headers() {
                response = response.header(name, value);
            }
            // Streams, so SSE responses pass through as they arrive
            response
                .body(axum::body::Body::from_stream(upstream.bytes_stream()))
                .unwrap()
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    (url, forwarded)
}

#[tokio::test]
async fn http_servers_can_go_through_a_proxy() {
    let proxied = serve_echo().await.url;
    let direct = serve_echo().await.url;
    let (proxy, forwarded) = serve_forward_proxy().await;
    let mut through_proxy = echo(&proxied);
    through_proxy["proxy"] = json!(proxy);
    let gateway = Gateway::start(
        json!({ "proxied": through_proxy, "direct": echo(&direct) }),
        &[TOKEN],
    )
    .await;
    gateway.wait_for_status("proxied", "ready").await;
    gateway.wait_for_status("direct", "ready").await;

    let (output, is_error) = gateway
        .call(
            "call_mcp_tool",
            json!({ "serverId": "proxied", "toolName": "echo", "arguments": {} }),
        )
        .await;
    assert!(!is_error, "{output}");
    // Every request to the proxied server, and only those, went through the proxy
    let forwarded = forwarded.lock().unwrap().clone();
    assert!(!forwarded.is_empty());
    assert!(forwarded.iter().all(|url| *url == proxied), "{forwarded:?}");
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
    assert_eq!(server["toolCount"], 5);
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
        5
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

impl Gateway {
    async fn search(&self, searches: Value) -> Vec<Value> {
        let (output, is_error) = self
            .call("search_mcp_tools", json!({ "searches": searches }))
            .await;
        assert!(!is_error, "{output}");
        output["results"].as_array().unwrap().clone()
    }
}

/// `server/tool` for each of a search result's matches
fn found(result: &Value) -> Vec<String> {
    result["matches"]
        .as_array()
        .unwrap_or_else(|| panic!("{result}"))
        .iter()
        .map(|found| {
            format!(
                "{}/{}",
                found["serverId"].as_str().unwrap(),
                found["toolName"].as_str().unwrap()
            )
        })
        .collect()
}

#[tokio::test]
async fn searches_tools() {
    let server = serve_echo().await;
    let gateway = Gateway::start(
        json!({ "echo": echo(&server.url), "inner": nested_gate() }),
        &[TOKEN],
    )
    .await;
    gateway.wait_for_status("echo", "ready").await;
    gateway.wait_for_status("inner", "ready").await;

    let results = gateway
        .search(json!([
            { "query": "echoes" },
            { "query": "list servers", "serverIds": ["inner"], "maxResults": 1 },
            { "query": "list", "serverIds": ["nope"] },
            {},
        ]))
        .await;
    assert_eq!(found(&results[0]), ["echo/echo"]);
    assert_eq!(results[0]["matches"][0]["description"], "Echoes");
    assert_eq!(found(&results[1]), ["inner/list_mcp_servers"]);
    assert_eq!(results[2], json!({ "error": "unknown server `nope`" }));
    let all = found(&results[3]);
    assert_eq!(all.len(), 13);
    // In catalog order, so `echo`'s eight tools come first
    assert!(
        all[..8].iter().all(|tool| tool.starts_with("echo/")),
        "{all:?}"
    );
    assert_eq!(results[3]["excludedServers"], json!([]));
}

#[tokio::test]
async fn searching_never_connects() {
    let dir = tempfile::tempdir().unwrap();
    let attempts_file = dir.path().join("attempts");
    let release = dir.path().join("release");
    let gateway = Gateway::start(
        json!({
            "broken": script(&format!("{COUNTS_ATTEMPTS}; exit 1")),
            "held": script(r#"while [ ! -e "$RELEASE" ]; do sleep 0.02; done; exit 1"#),
        }),
        &[
            ("ATTEMPTS", attempts_file.to_str().unwrap()),
            ("RELEASE", release.to_str().unwrap()),
        ],
    )
    .await;
    gateway.wait_for_status("broken", "failed").await;

    let results = gateway.search(json!([{ "query": "anything" }])).await;
    let excluded = results[0]["excludedServers"].as_array().unwrap();
    assert_eq!(excluded[0]["serverId"], "broken");
    assert_eq!(excluded[0]["status"], "failed");
    assert!(excluded[0]["error"].is_string());
    assert_eq!(
        excluded[1],
        json!({ "serverId": "held", "status": "connecting" })
    );

    // Unlike details and calls, searching doesn't retry failed servers
    sleep(Duration::from_millis(200)).await;
    assert_eq!(attempts(&attempts_file), "3");
    std::fs::write(&release, "").unwrap();
}

#[tokio::test]
async fn search_follows_reconnects() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let gateway = Gateway::start(
        json!({ "inner": script(RECORDS_PID) }),
        &[("PID_FILE", pid_file.to_str().unwrap())],
    )
    .await;
    gateway.wait_for_status("inner", "ready").await;
    let query = json!([{ "query": "list_mcp_servers", "maxResults": 1 }]);
    assert_eq!(
        found(&gateway.search(query.clone()).await[0]),
        ["inner/list_mcp_servers"]
    );

    let first = std::fs::read_to_string(&pid_file).unwrap();
    kill(&first);
    // While reconnecting, the server is excluded rather than searched
    let excluded = async {
        loop {
            let results = gateway.search(query.clone()).await;
            if results[0]["excludedServers"].as_array().unwrap().len() == 1 {
                return results[0].clone();
            }
            sleep(Duration::from_millis(20)).await;
        }
    };
    let result = timeout(Duration::from_secs(5), excluded).await.unwrap();
    assert_eq!(result["matches"], json!([]));
    assert_eq!(result["excludedServers"][0]["status"], "connecting");

    wait_for_new_pid(&pid_file, &first).await;
    gateway.wait_for_status("inner", "ready").await;
    assert_eq!(
        found(&gateway.search(query).await[0]),
        ["inner/list_mcp_servers"]
    );
}

#[tokio::test]
async fn limits_searches() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let invalid = [
        json!([]),
        json!(vec![json!({}); 21]),
        json!([{ "maxResults": 0 }]),
        json!([{ "maxResults": 51 }]),
    ];
    for searches in invalid {
        let (output, is_error) = gateway
            .call("search_mcp_tools", json!({ "searches": searches }))
            .await;
        assert!(is_error, "{searches}: {output}");
    }
}

impl Gateway {
    /// Runs a script with `execute_mcp_code`, returning its output and whether it was an error
    async fn run_code(&self, code: &str) -> (Value, bool) {
        self.call("execute_mcp_code", json!({ "code": code })).await
    }

    /// Runs a script that should fail, returning its error
    async fn code_error(&self, code: &str) -> String {
        let (output, is_error) = self.run_code(code).await;
        assert!(is_error, "{code}: {output}");
        output.as_str().unwrap().to_owned()
    }
}

#[tokio::test]
async fn runs_code() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let (output, is_error) = gateway
        .run_code(
            r#"
            console.log("hi", { a: 1 }, [2]);
            const sum = await Promise.resolve(1 + 1);
            return { sum, text: "café 🦀" };
            "#,
        )
        .await;
    assert!(!is_error, "{output}");
    assert_eq!(
        output,
        json!({ "result": { "sum": 2, "text": "café 🦀" }, "logs": ["hi {\"a\":1} [2]"] })
    );

    assert_eq!(
        gateway.run_code("let x = 1;").await,
        (json!({ "result": null }), false)
    );
    // Each script runs in a fresh worker
    gateway.run_code("globalThis.leftover = 1; return 1;").await;
    let (output, _) = gateway.run_code("return typeof leftover;").await;
    assert_eq!(output["result"], "undefined");
}

#[tokio::test]
async fn warns_about_unhandled_rejections() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let (output, is_error) = gateway
        .run_code(r#"Promise.reject(new Error("lost")); return 1;"#)
        .await;
    assert!(!is_error, "{output}");
    assert_eq!(
        output,
        json!({ "result": 1, "warnings": ["unhandled rejection: lost"] })
    );

    // A rejection that gets a handler later isn't reported
    let (output, _) = gateway
        .run_code(
            r#"
            const p = Promise.reject(new Error("late"));
            await null;
            p.catch(() => {});
            return 1;
            "#,
        )
        .await;
    assert_eq!(output, json!({ "result": 1 }));
}

#[tokio::test]
async fn maps_runtime_errors_to_the_original_script() {
    let gateway = Gateway::start(json!({}), &[]).await;
    for (code, locations) in [
        (
            "const x = 1;\n\nthrow new Error('boom');",
            vec!["script.ts:3:11"],
        ),
        (
            "interface X {\n value: string;\n}\nconst value: X = {value:'x'};\nthrow new Error('typed');",
            vec!["script.ts:5:11"],
        ),
        (
            "function fail(): never {\n throw new Error('nested');\n}\nawait Promise.resolve();\nfail();",
            vec!["at fail (script.ts:2:12)", "script.ts:5:1"],
        ),
        (
            "const x: string = '😀'; throw new Error('unicode');",
            vec!["script.ts:1:35"],
        ),
        (
            "enum Mode { A, B }\nclass X { constructor(public x: number) {} }\nthrow new Error('enum');",
            vec!["script.ts:3:11"],
        ),
        (
            "await new Promise((resolve, reject) => {\nsetTimeout(() => reject(new Error('timer')), 1);\n});",
            vec!["script.ts:2:29"],
        ),
        (
            "interface X { value: string }\nreturn { toJSON() { throw new Error('serialization'); } };",
            vec!["script.ts:2:31"],
        ),
    ] {
        let error = gateway.code_error(code).await;
        for location in locations {
            assert!(error.contains(location), "{code}: {error}");
        }
    }
}

#[tokio::test]
async fn keeps_runtime_error_messages_and_other_sources_intact() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let error = gateway
        .code_error("throw new Error('literal location\\n    at fake (gate-script.js:2:11)');")
        .await;
    assert!(
        error.starts_with("Error: literal location\n    at fake (gate-script.js:2:11)\n"),
        "{error}"
    );
    assert!(error.contains("at <anonymous> (script.ts:1:"), "{error}");

    let error = gateway
        .code_error("eval(\"throw new Error('eval')\");")
        .await;
    assert!(error.contains("at <eval> (<input>:1:11)"), "{error}");
    assert!(error.contains("at <anonymous> (script.ts:1:1)"), "{error}");
}

#[tokio::test]
async fn preserves_exception_names_and_hides_the_wrapper_frame() {
    let gateway = Gateway::start(json!({}), &[]).await;
    for (code, prefix) in [
        (
            "throw new TypeError('wrong type');",
            "TypeError: wrong type",
        ),
        ("missingVariable;", "ReferenceError:"),
        ("throw new RangeError('too far');", "RangeError: too far"),
        (
            "class MyError extends Error { name = 'MyError'; } throw new MyError('custom');",
            "MyError: custom",
        ),
        (
            "const e = new Error('fallback'); e.name = 42; throw e;",
            "Error: fallback",
        ),
        (
            "const e = new Error('getter'); Object.defineProperty(e, 'name', { get() { throw 'bad getter'; } }); throw e;",
            "Error: getter",
        ),
    ] {
        let error = gateway.code_error(code).await;
        assert!(error.starts_with(prefix), "{code}: {error}");
        assert!(error.contains("script.ts:1:"), "{error}");
        assert!(!error.contains("at <eval> (gate-script.js:"), "{error}");
    }
}

#[tokio::test]
async fn describes_non_error_throws_without_engine_internals() {
    let gateway = Gateway::start(json!({}), &[]).await;
    for (code, expected) in [
        ("throw 'x';", "Thrown: \"x\""),
        ("throw 42;", "Thrown: 42"),
        ("throw undefined;", "Thrown: undefined"),
        ("throw 123n;", "Thrown: 123n"),
        ("throw Symbol('reason');", "Thrown: Symbol(reason)"),
        (
            "throw { reason: 'bad', retry: false };",
            "Thrown: {\"reason\": \"bad\", \"retry\": false}",
        ),
        ("throw [1, , 'x'];", "Thrown: [1, [Empty], \"x\"]"),
        (
            "const a = { reason: 'cycle' }; a.self = a; throw a;",
            "Thrown: {\"reason\": \"cycle\", \"self\": [Circular]}",
        ),
        (
            "throw { get secret() { throw 'getter ran'; }, toJSON() { throw 'toJSON ran'; } };",
            "Thrown: {\"secret\": [Getter], \"toJSON\": [Function]}",
        ),
        (
            "const p = Proxy.revocable({}, {}); p.revoke(); throw p.proxy;",
            "Thrown: [Uninspectable object]",
        ),
        (
            "JSON.stringify = () => 'changed'; Object.keys = () => []; throw { reason: 'original' };",
            "Thrown: {\"reason\": \"original\"}",
        ),
    ] {
        let error = gateway.code_error(code).await;
        assert_eq!(error, expected, "{code}");
    }
    let error = gateway.code_error("throw null;").await;
    assert!(error.starts_with("Thrown: null"), "{error}");
    let error = gateway.code_error("throw 'x'.repeat(100000);").await;
    assert!(
        error.len() < 1024 && error.contains('…'),
        "{} bytes",
        error.len()
    );
    let error = gateway
        .code_error("throw Array(100).fill('x'.repeat(256));")
        .await;
    assert!(
        error.len() < 4096 && error.ends_with("… [truncated]"),
        "{error}"
    );
}

#[tokio::test]
async fn reports_script_errors() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let cases = [
        (r#"console.log("before"); throw new Error("boom");"#, "boom"),
        ("return (", "SyntaxError"),
        ("const a = {}; a.self = a; return a;", "circular"),
        ("return 1n;", "BigInt"),
        ("return () => 1;", "isn't JSON-serializable"),
        ("const f = () => f(); return f();", "stack"),
        (r#"return "x".repeat(70000);"#, "over the 65024-byte limit"),
        (
            r#"for (;;) console.log("x".repeat(1000));"#,
            "logged more than",
        ),
        (r#"await import("node:fs");"#, "node:fs"),
    ];
    for (code, expected) in cases {
        let error = gateway.code_error(code).await;
        assert!(error.contains(expected), "{code}: {error}");
    }
    // Empty log entries count against the limit too
    let error = gateway
        .code_error(r#"for (let i = 0; i < 100000; i++) console.log("");"#)
        .await;
    assert!(error.contains("logged more than"), "{error}");
    // Failures stay within the output limit
    for code in [
        r#"throw new Error("x".repeat(100000));"#,
        r#"for (let i = 0; i < 600; i++) console.log("y".repeat(100)); throw new Error("x".repeat(100000));"#,
    ] {
        let error = gateway.code_error(code).await;
        assert!(error.len() <= 64 * 1024, "{code}: {} bytes", error.len());
        assert!(error.contains("left out"), "{code}");
    }
    // What a failing script logged comes with its error
    let error = gateway
        .code_error(r#"console.log("before"); throw new Error("boom");"#)
        .await;
    assert!(error.ends_with("logs:\nbefore"), "{error}");

    let error = gateway.code_error("  ").await;
    assert_eq!(error, "`code` must not be empty");
    let error = gateway.code_error(&"x".repeat(300 * 1024)).await;
    assert!(error.contains("over the 262144-byte limit"), "{error}");
}

#[tokio::test]
async fn limits_scripts() {
    let (gateway, _) =
        Gateway::spawn(json!({}), &[], &["--code-timeout", "1"], Stdio::inherit()).await;
    let timed_out =
        "the script timed out after 1s; tool calls it already made may have taken effect";
    for code in ["while (true) {}", "await new Promise(() => {});"] {
        let started = std::time::Instant::now();
        assert_eq!(gateway.code_error(code).await, timed_out);
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{code}: {:?}",
            started.elapsed()
        );
    }

    let error = gateway
        .code_error("const a = []; while (true) a.push(new Array(100000).fill(1));")
        .await;
    assert!(error.contains("ran out of memory"), "{error}");
    // And later scripts still run
    assert_eq!(
        gateway.run_code("return 42;").await,
        (json!({ "result": 42 }), false)
    );
}

#[tokio::test]
async fn scripts_have_no_host_access() {
    let gateway = Gateway::start(json!({}), &[]).await;
    let (output, _) = gateway
        .run_code(
            "return [typeof process, typeof require, typeof fetch, typeof Bun, typeof Deno, typeof std, typeof os];",
        )
        .await;
    assert_eq!(output["result"], json!(vec!["undefined"; 7]));
}

async fn code_gateway(server: &EchoServer, args: &[&str]) -> Gateway {
    let servers = json!({ "echo": echo(&server.url), "inner": nested_gate() });
    let (gateway, _) = Gateway::spawn(servers, &[TOKEN], args, Stdio::inherit()).await;
    gateway.wait_for_status("echo", "ready").await;
    gateway.wait_for_status("inner", "ready").await;
    gateway
}

#[tokio::test]
async fn scripts_call_tools() {
    let server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;
    let (output, is_error) = gateway
        .run_code(
            r#"
            const inner = await mcp.inner.list_mcp_servers();
            const reflected = await mcp.call("echo", "reflect", { nested: { items: [1, true, null, "café 🦀"] } });
            const failed = await mcp.inner.get_mcp_tool_details({ tools: [] });
            let unknown;
            try { await mcp.nope.tool(); } catch (error) { unknown = error.message; }
            const then = await mcp.call("echo", "then");
            return {
                inner: JSON.parse(inner.content[0].text),
                reflected: JSON.parse(reflected.content[0].text),
                failed: [failed.isError, failed.content[0].text],
                unknown,
                then: then.content[0].text,
                proxy: [typeof mcp.then, typeof mcp.echo.then, typeof mcp[Symbol.iterator], typeof mcp.echo[Symbol.toPrimitive]],
            };
            "#,
        )
        .await;
    assert!(!is_error, "{output}");
    assert_eq!(
        output["result"],
        json!({
            "inner": { "servers": [] },
            "reflected": { "nested": { "items": [1, true, null, "café 🦀"] } },
            // A downstream error result resolves, rather than throwing
            "failed": [true, "`tools` must have between 1 and 20 items"],
            "unknown": "unknown server `nope`",
            "then": "then",
            "proxy": ["undefined", "undefined", "undefined", "undefined"],
        })
    );
}

#[tokio::test]
async fn script_calls_run_in_parallel() {
    let mut server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;
    let script = gateway
        .start_call(
            "execute_mcp_code",
            json!({ "code": r#"
                const results = await Promise.all([1, 2, 3].map((n) => mcp.echo.held({ n })));
                return results.map((result) => result.content[0].text);
            "# }),
        )
        .await;
    // All three are running before any is released
    for _ in 0..3 {
        assert_eq!(server.next_event().await, Event::Started);
    }
    server.release.send_replace(true);

    let (output, is_error) = finish_call(script).await;
    assert!(!is_error, "{output}");
    assert_eq!(output, json!({ "result": ["1", "2", "3"] }));
    assert_eq!(server.remaining_events().await, []);
}

#[tokio::test]
async fn limits_script_calls() {
    let server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;
    let (output, _) = gateway
        .run_code(
            r#"
            let tooLarge;
            try { await mcp.echo.big(); } catch (error) { tooLarge = error.message; }
            // Rejected before it's made, so it doesn't count toward the 100
            let notObject;
            try { await mcp.inner.list_mcp_servers([1]); } catch (error) { notObject = error.message; }
            // All nine start before any can finish, so the ninth is refused, and isn't counted
            const nine = await Promise.allSettled(Array.from({ length: 9 }, () => mcp.inner.list_mcp_servers()));
            const ninth = nine[8].reason.message;
            for (let made = 9; made < 100; made++) await mcp.inner.list_mcp_servers();
            let hundredFirst;
            try { await mcp.inner.list_mcp_servers(); } catch (error) { hundredFirst = error.message; }
            return { fulfilled: nine.filter((r) => r.status === "fulfilled").length, ninth, hundredFirst, notObject, tooLarge };
            "#,
        )
        .await;
    let result = &output["result"];
    assert_eq!(result["fulfilled"], 8, "{output}");
    assert_eq!(
        result["ninth"],
        "the script has 8 tool calls in flight already; await some before making more"
    );
    assert_eq!(
        result["hundredFirst"],
        "the script made more than 100 tool calls"
    );
    assert_eq!(result["notObject"], "a tool's arguments must be an object");
    let too_large = result["tooLarge"].as_str().unwrap_or_default();
    assert!(
        too_large.contains("ran, but its result exceeds the 4194304-byte limit"),
        "{output}"
    );
}

#[tokio::test]
async fn timing_out_cancels_script_calls() {
    let mut server = serve_echo().await;
    let gateway = code_gateway(&server, &["--code-timeout", "1"]).await;
    let error = gateway.code_error("await mcp.echo.sleep();").await;
    assert!(
        error.starts_with("the script timed out after 1s"),
        "{error}"
    );
    assert_eq!(server.next_event().await, Event::Started);
    assert_eq!(server.next_event().await, Event::Cancelled);
}

#[tokio::test]
async fn cancelling_a_script_cancels_its_calls() {
    let mut server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;
    let script = gateway
        .start_call(
            "execute_mcp_code",
            json!({ "code": "await mcp.echo.sleep();" }),
        )
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    script.cancel(None).await.unwrap();
    assert_eq!(server.next_event().await, Event::Cancelled);
}

const CANCELLED_CALL: &str = "1 tool call was still running when the script ended; cancellation was requested, but it may already have taken effect";

#[tokio::test]
async fn cancels_calls_scripts_do_not_await() {
    let mut server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;

    // Returning right away: the call may not even have reached the server
    let output = gateway.run_code("mcp.echo.sleep(); return 1;").await;
    assert_eq!(
        output,
        (json!({ "result": 1, "warnings": [CANCELLED_CALL] }), false)
    );
    let events = server.remaining_events().await;
    assert!(
        events.is_empty() || events == [Event::Started, Event::Cancelled],
        "{events:?}"
    );

    // Returning once the server has started it
    let output = gateway
        .run_code("mcp.echo.sleep(); await mcp.echo.until_started(); return 1;")
        .await;
    assert_eq!(
        output,
        (json!({ "result": 1, "warnings": [CANCELLED_CALL] }), false)
    );
    assert_eq!(server.next_event().await, Event::Started);
    assert_eq!(server.next_event().await, Event::Cancelled);
}

#[tokio::test]
async fn results_leave_room_for_the_cancelled_calls_warning() {
    let server = serve_echo().await;
    let gateway = code_gateway(&server, &[]).await;
    // `{"result":""}` is 13 bytes, and results and logs get 65024
    let (output, is_error) = gateway
        .run_code(r#"mcp.echo.sleep(); return "r".repeat(65011);"#)
        .await;
    assert!(!is_error, "{output}");
    assert_eq!(output["result"].as_str().unwrap().len(), 65011);
    assert_eq!(output["warnings"], json!([CANCELLED_CALL]));

    let error = gateway
        .code_error(r#"mcp.echo.sleep(); return "r".repeat(65012);"#)
        .await;
    assert!(error.contains("over the 65024-byte limit"), "{error}");
}

#[tokio::test]
async fn cancelled_script_calls_are_not_dispatched_after_connecting() {
    let mut server = serve_echo().await;
    // Keeps the gateway connecting to `echo` until the script has timed out
    server.open.send_replace(false);
    let (gateway, _) = Gateway::spawn(
        json!({ "echo": echo(&server.url) }),
        &[TOKEN],
        &["--code-timeout", "1"],
        Stdio::inherit(),
    )
    .await;
    let error = gateway.code_error("await mcp.echo.sleep();").await;
    assert!(error.starts_with("the script timed out"), "{error}");

    server.open.send_replace(true);
    gateway.wait_for_status("echo", "ready").await;
    assert_eq!(
        server.remaining_events().await,
        [],
        "the call shouldn't be dispatched"
    );
}

/// Writes a message to a worker, framed the way gate does
async fn write_message(stdin: &mut tokio::process::ChildStdin, message: Value) {
    use tokio::io::AsyncWriteExt;
    let json = message.to_string();
    stdin
        .write_all(&(json.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stdin.write_all(json.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
}

async fn read_message(stdout: &mut tokio::process::ChildStdout) -> Value {
    use tokio::io::AsyncReadExt;
    let mut length = [0; 4];
    stdout.read_exact(&mut length).await.unwrap();
    let mut json = vec![0; u32::from_be_bytes(length) as usize];
    stdout.read_exact(&mut json).await.unwrap();
    serde_json::from_slice(&json).unwrap()
}

#[tokio::test]
async fn workers_fail_pending_calls_when_the_gateway_disconnects() {
    // Plays the gateway's side, then goes away mid-call
    let mut worker = Command::new(GATE)
        .arg("--code-worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = worker.stdin.take().unwrap();
    let mut stdout = worker.stdout.take().unwrap();
    let code = r#"try { await mcp.call("s", "t"); return "resolved"; } catch (error) { return error.message; }"#;
    write_message(
        &mut stdin,
        json!({ "Start": { "code": code, "timeout_ms": 5000 } }),
    )
    .await;

    let call = read_message(&mut stdout).await;
    assert_eq!(call["Call"]["server_id"], "s");
    drop(stdin);
    let complete = timeout(Duration::from_secs(5), read_message(&mut stdout))
        .await
        .unwrap();
    assert_eq!(
        complete["Complete"]["result"],
        "the gateway disconnected before the call's result arrived"
    );
}

/// Makes 64 calls to `held` at once, checking they all reach the server before any returns
async fn makes_concurrent_http_calls(responses: Responses) {
    let mut server = serve_echo_with(responses).await;
    let gateway = Gateway::start(json!({ "echo": echo(&server.url) }), &[TOKEN]).await;
    gateway.wait_for_status("echo", "ready").await;

    let mut calls = Vec::new();
    for n in 0..64 {
        let arguments = json!({ "serverId": "echo", "toolName": "held", "arguments": { "n": n } });
        calls.push(gateway.start_call("call_mcp_tool", arguments).await);
    }
    for started in 0..64 {
        let event = timeout(Duration::from_secs(5), server.events.recv()).await;
        let event =
            event.unwrap_or_else(|_| panic!("{responses:?}: only {started} of 64 calls started"));
        assert_eq!(event, Some(Event::Started));
    }
    server.release.send_replace(true);
    for (n, call) in calls.into_iter().enumerate() {
        assert_eq!(finish_call(call).await, (json!(n), false));
    }
}

#[tokio::test]
async fn makes_concurrent_calls_to_sse_servers() {
    makes_concurrent_http_calls(Responses::Sse).await;
}

#[tokio::test]
async fn makes_concurrent_calls_to_json_servers() {
    makes_concurrent_http_calls(Responses::Json).await;
}

#[tokio::test]
async fn controls_downstream_environments() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        (json!({}), json!({}), "parent|parent||hook"),
        (json!({"inheritEnv": false}), json!({}), "|||"),
        (json!({"inheritEnv": []}), json!({}), "|||"),
        (
            json!({"inheritEnv": ["GATE_TEST_KEEP"]}),
            json!({}),
            "parent|||",
        ),
        (
            json!({"inheritEnv": false}),
            json!({"inheritEnv": true}),
            "parent|parent||hook",
        ),
        (
            json!({"inheritEnv": true}),
            json!({"inheritEnv": false}),
            "|||",
        ),
        (
            json!({"inheritEnv": true}),
            json!({"inheritEnv": ["GATE_TEST_KEEP"]}),
            "parent|||",
        ),
        (
            json!({"hookEnv": ["GATE_TEST_HOOK"]}),
            json!({}),
            "parent|parent||",
        ),
        (
            json!({"hookEnv": ["GATE_TEST_HOOK"], "inheritEnv": ["GATE_TEST_KEEP", "GATE_TEST_HOOK"]}),
            json!({}),
            "parent|||",
        ),
        (
            json!({"inheritEnv": false}),
            json!({"env": {"GATE_TEST_KEEP": "explicit", "GATE_TEST_EXTRA": "extra"}}),
            "explicit||extra|",
        ),
        (
            json!({}),
            json!({"env": {"GATE_TEST_KEEP": "explicit"}}),
            "explicit|parent||hook",
        ),
    ];
    for (i, (mut config, mut server, expected)) in cases.into_iter().enumerate() {
        let output = dir.path().join(format!("env-{i}"));
        server["command"] = json!("/bin/sh");
        server["args"] = json!([
            "-c",
            r#"printf '%s|%s|%s|%s' "$GATE_TEST_KEEP" "$GATE_TEST_DROP" "$GATE_TEST_EXTRA" "$GATE_TEST_HOOK" > "$1"; exit 1"#,
            "environment-test",
            output,
        ]);
        config["mcpServers"] = json!({"probe": server});
        let (gateway, _) = Gateway::spawn_config(
            config,
            &[
                ("GATE_TEST_KEEP", "parent"),
                ("GATE_TEST_DROP", "parent"),
                ("GATE_TEST_EXTRA", ""),
                ("GATE_TEST_HOOK", "hook"),
            ],
            &[],
            Stdio::null(),
        )
        .await;
        gateway.wait_for_status("probe", "failed").await;
        assert_eq!(
            std::fs::read_to_string(output).unwrap(),
            expected,
            "case {i}"
        );
    }
}

#[tokio::test]
async fn hook_values_are_not_inherited_under_other_names() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("env");
    let config = json!({
        "hookEnv": ["GATE_TEST_HOOK", "GATE_TEST_EMPTY_HOOK"],
        // A variable a hook only references isn't a secret, so its value can still be
        // inherited under another name
        "accessPolicies": {"p": {"http": {"url": "${GATE_TEST_POLICY_URL}"}}},
        "mcpServers": {"probe": {
            "command": "/bin/sh",
            "args": [
                "-c",
                r#"printf '%s|%s|%s|%s|%s' "$GATE_TEST_KEEP" "$GATE_TEST_ALIAS" "$GATE_TEST_EXPLICIT" "${GATE_TEST_EMPTY+set}" "$GATE_TEST_URL" > "$1"; exit 1"#,
                "environment-test",
                output,
            ],
            "env": {"GATE_TEST_EXPLICIT": "hook-secret"},
        }},
    });
    let (gateway, _) = Gateway::spawn_config(
        config,
        &[
            ("GATE_TEST_KEEP", "parent"),
            ("GATE_TEST_HOOK", "hook-secret"),
            ("GATE_TEST_ALIAS", "hook-secret"),
            ("GATE_TEST_EMPTY_HOOK", ""),
            ("GATE_TEST_EMPTY", ""),
            ("GATE_TEST_POLICY_URL", "http://127.0.0.1:1/policy"),
            ("GATE_TEST_URL", "http://127.0.0.1:1/policy"),
        ],
        &[],
        Stdio::null(),
    )
    .await;
    gateway.wait_for_status("probe", "failed").await;
    // The alias is dropped, a value set explicitly is kept, and empty hook values match nothing
    assert_eq!(
        std::fs::read_to_string(output).unwrap(),
        "parent||hook-secret|set|http://127.0.0.1:1/policy"
    );
}

struct PolicyServer {
    url: String,
    answer: Arc<std::sync::Mutex<(StatusCode, String)>>,
    requests: mpsc::UnboundedReceiver<Value>,
}

impl PolicyServer {
    fn allow(&self, servers: &[&str]) {
        *self.answer.lock().unwrap() = (StatusCode::OK, json!({"allowed": servers}).to_string());
    }
}

type PolicyState = (
    Arc<std::sync::Mutex<(StatusCode, String)>>,
    mpsc::UnboundedSender<Value>,
);

async fn serve_policy() -> PolicyServer {
    let answer = Arc::new(std::sync::Mutex::new((
        StatusCode::OK,
        json!({"allowed": []}).to_string(),
    )));
    let (sent, requests) = mpsc::unbounded_channel();
    let state = (answer.clone(), sent);
    let router = axum::Router::new()
        .route(
            "/access",
            axum::routing::post(
                |State((answer, sent)): State<PolicyState>,
                 headers: axum::http::HeaderMap,
                 axum::Json(body): axum::Json<Value>| async move {
                    assert_eq!(headers.get("authorization").unwrap(), "Bearer policy-token");
                    sent.send(body).unwrap();
                    let (status, body) = answer.lock().unwrap().clone();
                    (status, body)
                },
            ),
        )
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/access", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    PolicyServer {
        url,
        answer,
        requests,
    }
}

fn protected(mut server: Value) -> Value {
    server["access"] = json!({"policy": "platform", "params": {"connection": "c_123"}});
    server
}

async fn policy_gateway(policy: &PolicyServer, servers: Value) -> Gateway {
    Gateway::spawn_config(json!({
        "accessPolicies": {"platform": {"http": {"url": policy.url, "headers": {"Authorization": "Bearer ${POLICY_TOKEN}"}}}},
        "mcpServers": servers,
    }), &[("POLICY_TOKEN", "policy-token"), TOKEN], &[], Stdio::inherit()).await.0
}

#[tokio::test]
async fn access_changes_apply_to_discovery_details_and_calls() {
    let mut policy = serve_policy().await;
    let echo_server = serve_echo().await;
    let gateway = policy_gateway(
        &policy,
        json!({"private": protected(echo(&echo_server.url)), "public": nested_gate()}),
    )
    .await;
    gateway.wait_for_status("public", "ready").await;
    assert_eq!(gateway.servers().await.as_array().unwrap().len(), 1);
    let results = gateway
        .search(json!([{}, {"serverIds": ["private"]}, {"serverIds": ["missing"]}]))
        .await;
    assert!(
        found(&results[0])
            .iter()
            .all(|name| name.starts_with("public/"))
    );
    assert_eq!(results[1]["error"], "unknown server `private`");
    assert_eq!(results[2]["error"], "unknown server `missing`");
    assert_eq!(
        gateway
            .details(json!([{"serverId":"private", "toolName":"echo"}]))
            .await[0]["error"],
        "unknown server `private`"
    );
    assert_eq!(
        gateway
            .call(
                "call_mcp_tool",
                json!({"serverId":"private", "toolName":"echo"})
            )
            .await,
        (json!("unknown server `private`"), true)
    );

    policy.allow(&["private", "not-configured"]);
    gateway.wait_for_status("private", "ready").await;
    while policy.requests.try_recv().is_ok() {}
    let searches = gateway
        .search(json!([{"serverIds":["private"]}, {"serverIds":["private"]}]))
        .await;
    assert_eq!(searches.len(), 2);
    let request = policy.requests.try_recv().unwrap();
    assert_eq!(
        request,
        json!({"version":1,"kind":"access","servers":[{"server":"private","params":{"connection":"c_123"}}]})
    );
    assert!(
        policy.requests.try_recv().is_err(),
        "one policy request for the batch"
    );
    assert_eq!(
        gateway
            .details(json!([{"serverId":"private", "toolName":"echo"}]))
            .await[0]["tool"]["name"],
        "echo"
    );
    assert_eq!(
        gateway
            .call(
                "call_mcp_tool",
                json!({"serverId":"private", "toolName":"echo"})
            )
            .await,
        (json!("echo"), false)
    );

    for (status, body) in [
        (StatusCode::OK, "not JSON".to_string()),
        (StatusCode::OK, json!({"allowed": "private"}).to_string()),
        (
            StatusCode::FORBIDDEN,
            json!({"allowed":["private"]}).to_string(),
        ),
        (StatusCode::OK, "x".repeat(65537)),
    ] {
        *policy.answer.lock().unwrap() = (status, body);
        assert_eq!(gateway.servers().await.as_array().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn scripts_lose_access_and_cannot_gain_servers() {
    let policy = serve_policy().await;
    policy.allow(&["private"]);
    let mut server = serve_echo().await;
    let gateway = policy_gateway(
        &policy,
        json!({
            "private": protected(echo(&server.url)),
            "new": protected(echo(&server.url)),
        }),
    )
    .await;
    gateway.wait_for_status("private", "ready").await;
    let call = gateway
        .start_call(
            "execute_mcp_code",
            json!({"code": r#"
        await mcp.private.held({n:1});
        const errors = [];
        for (const id of ["private", "new"]) {
            try { await mcp.call(id, "echo"); errors.push("unexpected success"); }
            catch (error) { errors.push(error.message); }
        }
        return errors;
    "#}),
        )
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    policy.allow(&["new"]);
    server.release.send_replace(true);
    let (output, error) = finish_call(call).await;
    assert!(!error, "{output}");
    assert_eq!(
        output["result"],
        json!(["unknown server `private`", "unknown server `new`"])
    );
}

#[tokio::test]
async fn checks_access_again_after_waiting_for_a_connection() {
    let mut policy = serve_policy().await;
    policy.allow(&["private"]);
    let mut server = serve_echo().await;
    server.open.send_replace(false);
    let gateway = policy_gateway(&policy, json!({"private": protected(echo(&server.url))})).await;
    let call = gateway
        .start_call(
            "call_mcp_tool",
            json!({"serverId":"private", "toolName":"sleep"}),
        )
        .await;
    timeout(Duration::from_secs(5), policy.requests.recv())
        .await
        .unwrap()
        .unwrap();
    // Let the first answer reach the gateway while the connection remains held.
    sleep(Duration::from_millis(100)).await;
    policy.allow(&[]);
    server.open.send_replace(true);
    assert_eq!(
        finish_call(call).await,
        (json!("unknown server `private`"), true)
    );
    assert_eq!(server.remaining_events().await, []);
}

#[tokio::test]
async fn command_policies_fail_closed_and_hide_their_environment() {
    let dir = tempfile::tempdir().unwrap();
    let captured = dir.path().join("inherited");
    let response = dir.path().join("response");
    let request = dir.path().join("request");
    std::fs::write(&response, json!({"allowed":["private"]}).to_string()).unwrap();
    let gateway = Gateway::spawn_config(json!({
        "accessPolicies": {"platform": {
            "command":"/bin/sh", "args":["-c", "cat > \"$REQUEST\"; test \"$HOOK_SECRET\" = dummy || exit 1; cat \"$RESPONSE\""],
            "env": {"HOOK_SECRET":"${POLICY_SECRET}", "REQUEST":request, "RESPONSE":response},
        }},
        "mcpServers": {"private":protected({
            let mut server = script(r#"printf '%s' "$POLICY_SECRET" > "$CAPTURED"; exec "$GATE" --config "$EMPTY_CONFIG""#);
            server["env"] = json!({"CAPTURED": captured});
            server
        })},
    }), &[("POLICY_SECRET","dummy")], &[], Stdio::inherit()).await.0;
    gateway.wait_for_status("private", "ready").await;
    assert_eq!(std::fs::read_to_string(captured).unwrap(), "");
    let sent: Value = serde_json::from_str(&std::fs::read_to_string(request).unwrap()).unwrap();
    assert_eq!(sent["kind"], "access");
    std::fs::write(response, "secret-invalid-answer").unwrap();
    assert_eq!(gateway.servers().await, json!([]));
}

fn credential_lease(
    id: &str,
    values: Value,
    refresh_ms: Option<i64>,
    expires_ms: Option<i64>,
) -> Value {
    let mut lease = json!({"outcome":"lease","credentialId":id,"values":values});
    if let Some(ms) = refresh_ms {
        lease["refreshAfter"] = json!(chrono::Utc::now() + chrono::Duration::milliseconds(ms));
    }
    if let Some(ms) = expires_ms {
        lease["expiresAt"] = json!(chrono::Utc::now() + chrono::Duration::milliseconds(ms));
    }
    lease
}
impl PolicyServer {
    fn credentials(&self, answer: Value) {
        *self.answer.lock().unwrap() = (StatusCode::OK, answer.to_string());
    }
    async fn next_request(&mut self) -> Value {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
fn leased(mut server: Value) -> Value {
    server["credentials"] = json!({"provider":"p","params":{"connection":"c_123"}});
    server
}
async fn credential_gateway(
    provider: &PolicyServer,
    servers: Value,
    failure: &str,
    drain: u64,
) -> Gateway {
    Gateway::spawn_config(json!({
        "credentialProviders":{"p":{"http":{"url":provider.url,"headers":{"Authorization":"Bearer policy-token"}},"refreshFailure":failure,"drainSeconds":drain}},
        "mcpServers":servers
    }), &[TOKEN], &[], Stdio::inherit()).await.0
}
fn tool_call(tool: &str) -> Value {
    json!({"serverId":"s","toolName":tool,"arguments":{}})
}

#[tokio::test]
async fn credentials_refresh_same_values_without_interrupting_calls() {
    let mut provider = serve_policy().await;
    provider.credentials(credential_lease(
        "one",
        json!({"token":"test-token"}),
        Some(500),
        Some(900),
    ));
    let mut server = serve_echo().await;
    let gateway=credential_gateway(&provider,json!({"s":leased(json!({"url":server.url,"headers":{"Authorization":"Bearer ${credential:token}"}}))}),"keep",1).await;
    gateway.wait_for_status("s", "ready").await;
    assert_eq!(
        provider.next_request().await,
        json!({"version":1,"kind":"credentials","server":"s","params":{"connection":"c_123"}})
    );
    let running = gateway
        .start_call(
            "call_mcp_tool",
            json!({"serverId":"s","toolName":"held","arguments":{"n":7}}),
        )
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    provider.credentials(credential_lease(
        "one",
        json!({"token":"test-token"}),
        None,
        None,
    ));
    provider.next_request().await;
    sleep(Duration::from_millis(600)).await; // beyond the original hard expiry
    server.release.send_replace(true);
    let (result, error) = finish_call(running).await;
    assert!(!error, "{result}");
    assert!(server.events.try_recv().is_err());
    assert_eq!(gateway.servers().await[0]["status"], "ready");
}

#[tokio::test]
async fn transient_refresh_keeps_until_expiry_but_revoke_stops_immediately() {
    for failure in ["keep", "revoke"] {
        let mut provider = serve_policy().await;
        provider.credentials(credential_lease("one", json!({}), Some(400), Some(1300)));
        let gateway =
            credential_gateway(&provider, json!({"s":leased(nested_gate())}), failure, 0).await;
        gateway.wait_for_status("s", "ready").await;
        provider.next_request().await;
        provider.credentials(json!({"outcome":"transient"}));
        provider.next_request().await;
        sleep(Duration::from_millis(100)).await;
        if failure == "keep" {
            assert_eq!(gateway.servers().await[0]["status"], "ready");
            let (_, error) = gateway
                .call("call_mcp_tool", tool_call("list_mcp_servers"))
                .await;
            assert!(!error);
        }
        gateway.wait_for_status("s", "failed").await;
        let (_, error) = gateway
            .call("call_mcp_tool", tool_call("list_mcp_servers"))
            .await;
        assert!(error);
    }
}

#[tokio::test]
async fn rotation_drains_calls_waits_and_never_replays() {
    for drain in [0, 2] {
        let mut provider = serve_policy().await;
        provider.credentials(credential_lease(
            "one",
            json!({"token":"test-token","generation":"first"}),
            Some(400),
            None,
        ));
        let mut server = serve_echo().await;
        let gateway=credential_gateway(&provider,json!({"s":leased(json!({"url":server.url,"headers":{"Authorization":"Bearer ${credential:token}"}}))}),"keep",drain).await;
        gateway.wait_for_status("s", "ready").await;
        provider.next_request().await;
        let running = gateway
            .start_call(
                "call_mcp_tool",
                json!({"serverId":"s","toolName":"held","arguments":{"n":9}}),
            )
            .await;
        assert_eq!(server.next_event().await, Event::Started);
        provider.credentials(credential_lease(
            "two",
            json!({"token":"test-token","generation":"second"}),
            None,
            None,
        ));
        provider.next_request().await;
        if drain > 0 {
            gateway.wait_for_status("s", "connecting").await;
            let waiting = gateway.start_call("call_mcp_tool", tool_call("echo")).await;
            server.release.send_replace(true);
            let (result, error) = finish_call(running).await;
            assert!(!error, "{result}");
            let (result, error) = finish_call(waiting).await;
            assert!(!error, "{result}");
        } else {
            let (result, error) = finish_call(running).await;
            assert!(
                error && result.to_string().contains("downstream outcome is unknown"),
                "{result}"
            );
            assert_eq!(server.next_event().await, Event::Cancelled);
        }
        gateway.wait_for_status("s", "ready").await;
        assert!(server.events.try_recv().is_err(), "calls must not replay");
    }
}

#[tokio::test]
async fn credential_provider_http_statuses_take_precedence_over_body() {
    for code in [401, 403, 404, 410, 408, 429, 500, 200] {
        let mut provider = serve_policy().await;
        *provider.answer.lock().unwrap() = (
            StatusCode::from_u16(code).unwrap(),
            if code == 200 {
                "malformed".into()
            } else {
                credential_lease("one", json!({}), None, None).to_string()
            },
        );
        let gateway =
            credential_gateway(&provider, json!({"s":leased(nested_gate())}), "keep", 0).await;
        let server = gateway.wait_for_status("s", "failed").await;
        let denied = matches!(code, 401 | 403 | 404 | 410);
        assert_eq!(
            server["error"].as_str().unwrap().contains("denied"),
            denied,
            "{server}"
        );
        let mut requests = 0;
        while provider.requests.try_recv().is_ok() {
            requests += 1;
        }
        assert_eq!(requests, if denied { 1 } else { 3 });
    }
}

#[tokio::test]
async fn command_credentials_merge_rotate_and_redact_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let answer = dir.path().join("answer");
    let observed = dir.path().join("observed");
    let request = dir.path().join("request");
    std::fs::write(
        &answer,
        credential_lease(
            "one",
            json!({"SECRET":"first-secret-value","OLD":"old-value"}),
            Some(500),
            None,
        )
        .to_string(),
    )
    .unwrap();
    let configuration = json!({
        "hookEnv":["HOOK_SECRET"],
        "credentialProviders":{"p":{"command":"/bin/sh","args":["-c","cat > \"$REQUEST\"; cat \"$ANSWER\""],"env":{"REQUEST":request,"ANSWER":answer},"drainSeconds":0}},
        "mcpServers":{"s":{"command":"/bin/sh","args":["-c","printf '%s|%s|%s\\n' \"$SECRET\" \"${OLD-unset}\" \"${NEW-unset}\" >> \"$OBSERVED\"; printf 'first-' >&2; sleep 0.02; printf 'secret-value\\n' >&2; exec \"$GATE\" --config \"$EMPTY_CONFIG\""],"env":{"OBSERVED":observed},"credentials":{"provider":"p","mergeEnv":true}}}
    });
    let (gateway, stderr) = Gateway::spawn_config(
        configuration,
        &[("HOOK_SECRET", "only-hooks")],
        &[],
        Stdio::piped(),
    )
    .await;
    let mut lines = BufReader::new(stderr.unwrap()).lines();
    gateway.wait_for_status("s", "ready").await;
    let first = timeout(Duration::from_secs(3), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        first.contains("<redacted>") && !first.contains("first-secret-value"),
        "{first}"
    );
    std::fs::write(
        &answer,
        credential_lease(
            "two",
            json!({"SECRET":"second-secret-value","NEW":"new-value"}),
            None,
            None,
        )
        .to_string(),
    )
    .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let text = std::fs::read_to_string(&observed).unwrap();
            if text.lines().count() >= 2 {
                assert_eq!(
                    text,
                    "first-secret-value|old-value|unset\nsecond-secret-value|unset|new-value\n"
                );
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    gateway.wait_for_status("s", "ready").await;
    let body: Value = serde_json::from_str(&std::fs::read_to_string(request).unwrap()).unwrap();
    assert_eq!(body["kind"], "credentials");
}

/// Minimal stateless MCP server for testing HTTP status and JSON-RPC attribution independently.
async fn auth_server(
    method: &'static str,
    variant: &'static str,
) -> (String, mpsc::UnboundedReceiver<(String, String)>) {
    let (sent, requests) = mpsc::unbounded_channel();
    let router=axum::Router::new().route("/mcp",axum::routing::post(move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
        let sent=sent.clone();
        async move {
            let token=headers.get("authorization").and_then(|h|h.to_str().ok()).unwrap_or("").to_owned();
            let name=body["method"].as_str().unwrap_or("");
            let _=sent.send((name.to_owned(),token.clone()));
            if body.get("id").is_none() { return StatusCode::ACCEPTED.into_response(); }
            let id=body["id"].clone();
            if name==method && token=="Bearer bad-secret-token" {
                let error=json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"authentication failed"}});
                return match variant {
                    "plain" => (StatusCode::UNAUTHORIZED,"no").into_response(),
                    "challenge" => (StatusCode::UNAUTHORIZED,[("www-authenticate","Bearer")],"no").into_response(),
                    "rpc" => (StatusCode::UNAUTHORIZED,axum::Json(error)).into_response(),
                    "data" => { let mut error=error; error["error"]["data"]=json!({"status":401}); axum::Json(error).into_response() }
                    "structured" => axum::Json(json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"structuredContent":{"status":401},"content":[{"type":"text","text":"rejected"}]}})).into_response(),
                    "matcher" => axum::Json(json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":"vendor authentication failure"}]}})).into_response(),
                    _ => unreachable!(),
                };
            }
            let result=match name {
                "initialize" => json!({"protocolVersion":body["params"]["protocolVersion"],"capabilities":{"tools":{}},"serverInfo":{"name":"auth-test","version":"1"}}),
                "tools/list" => json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
                "tools/call" => json!({"content":[{"type":"text","text":"ok"}]}),
                _ => return axum::Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"unknown method"}})).into_response(),
            };
            axum::Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response()
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, requests)
}

#[tokio::test]
async fn auth_failures_during_setup_and_calls_are_attributed_without_replay() {
    for method in ["initialize", "tools/list", "tools/call"] {
        for variant in ["plain", "challenge", "rpc", "data", "structured", "matcher"] {
            if method != "tools/call" && matches!(variant, "structured" | "matcher") {
                continue;
            }
            let (url, mut downstream) = auth_server(method, variant).await;
            let mut provider = serve_policy().await;
            let mut initial =
                credential_lease("one", json!({"token":"bad-secret-token"}), None, None);
            initial["authFailures"] =
                json!([{"isError":true,"text":"^vendor authentication failure$"}]);
            provider.credentials(initial);
            let gateway=credential_gateway(&provider,json!({"s":leased(json!({"url":url,"headers":{"Authorization":"Bearer ${credential:token}"}}))}),"keep",0).await;
            provider.next_request().await;
            if method == "tools/call" {
                gateway.wait_for_status("s", "ready").await;
                provider.credentials(credential_lease(
                    "two",
                    json!({"token":"good-secret-token"}),
                    None,
                    None,
                ));
                let (result, error) = gateway.call("call_mcp_tool", tool_call("echo")).await;
                assert!(
                    !result.to_string().contains("interrupted"),
                    "{variant}: {result}"
                );
                // The failing call is surfaced, never retried with the new credential.
                assert!(
                    error
                        || result.to_string().contains("rejected")
                        || result.to_string().contains("vendor authentication failure"),
                    "{method} {variant}: {result}"
                );
                let refresh = provider.next_request().await;
                assert_eq!(refresh["rejectedCredentialId"], "one", "{method} {variant}");
                gateway.wait_for_status("s", "ready").await;
            } else {
                let state = gateway.wait_for_status("s", "failed").await;
                assert!(
                    state["error"]
                        .as_str()
                        .unwrap()
                        .contains("no other credential"),
                    "{method} {variant}: {state}"
                );
                assert_eq!(
                    provider.next_request().await["rejectedCredentialId"],
                    "one",
                    "{method} {variant}"
                );
                while provider.requests.try_recv().is_ok() {}
                provider.credentials(credential_lease(
                    "two",
                    json!({"token":"good-secret-token"}),
                    None,
                    None,
                ));
                let (result, error) = gateway.call("call_mcp_tool", tool_call("echo")).await;
                assert!(!error, "{method} {variant}: {result}");
                assert_eq!(provider.next_request().await["rejectedCredentialId"], "one");
            }
            let mut failures = 0;
            while let Ok((name, token)) = downstream.try_recv() {
                if name == method && token == "Bearer bad-secret-token" {
                    failures += 1;
                }
            }
            assert_eq!(
                failures, 1,
                "rejected values must not be dispatched again: {method} {variant}"
            );
        }
    }
}

#[tokio::test]
async fn leases_expiring_during_drain_or_setup_are_replaced() {
    for during_setup in [false, true] {
        let mut provider = serve_policy().await;
        let mut server = serve_echo().await;
        if during_setup {
            server.open.send_replace(false);
        }
        provider.credentials(credential_lease(
            "one",
            json!({"token":"test-token"}),
            if during_setup { None } else { Some(400) },
            if during_setup { Some(500) } else { None },
        ));
        let gateway=credential_gateway(&provider,json!({"s":leased(json!({"url":server.url,"headers":{"Authorization":"Bearer ${credential:token}"}}))}),"keep",2).await;
        provider.next_request().await;
        let running = if during_setup {
            None
        } else {
            gateway.wait_for_status("s", "ready").await;
            let running = gateway
                .start_call(
                    "call_mcp_tool",
                    json!({"serverId":"s","toolName":"held","arguments":{"n":1}}),
                )
                .await;
            assert_eq!(server.next_event().await, Event::Started);
            provider.credentials(credential_lease(
                "two",
                json!({"token":"test-token","v":"short-lived"}),
                None,
                Some(650),
            ));
            provider.next_request().await;
            gateway.wait_for_status("s", "connecting").await;
            Some(running)
        };
        sleep(Duration::from_millis(750)).await;
        provider.credentials(credential_lease(
            "three",
            json!({"token":"test-token","v":"fresh"}),
            None,
            None,
        ));
        server.open.send_replace(true);
        server.release.send_replace(true);
        if let Some(running) = running {
            assert!(!finish_call(running).await.1);
        }
        provider.next_request().await;
        gateway.wait_for_status("s", "ready").await;
        assert!(!gateway.call("call_mcp_tool", tool_call("echo")).await.1);
    }
}

#[tokio::test]
async fn refresh_denial_cancels_running_calls_and_waits_for_demand() {
    let mut provider = serve_policy().await;
    provider.credentials(credential_lease("one", json!({}), Some(400), None));
    let mut server = serve_echo().await;
    let gateway =
        credential_gateway(&provider, json!({"s":leased(echo(&server.url))}), "keep", 0).await;
    gateway.wait_for_status("s", "ready").await;
    provider.next_request().await;
    let running = gateway
        .start_call("call_mcp_tool", tool_call("sleep"))
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    provider.credentials(json!({"outcome":"denied"}));
    provider.next_request().await;
    assert!(finish_call(running).await.1);
    assert_eq!(server.next_event().await, Event::Cancelled);
    gateway.wait_for_status("s", "failed").await;
    sleep(Duration::from_millis(100)).await;
    assert!(provider.requests.try_recv().is_err());
    provider.credentials(credential_lease("two", json!({}), None, None));
    assert!(!gateway.call("call_mcp_tool", tool_call("echo")).await.1);
    provider.next_request().await;
}

#[tokio::test]
async fn same_value_refresh_replaces_matchers_and_rejected_values_cannot_be_renamed() {
    let (url, mut downstream) = auth_server("tools/call", "matcher").await;
    let mut provider = serve_policy().await;
    provider.credentials(credential_lease(
        "one",
        json!({"token":"bad-secret-token"}),
        Some(500),
        None,
    ));
    let gateway=credential_gateway(&provider,json!({"s":leased(json!({"url":url,"headers":{"Authorization":"Bearer ${credential:token}"}})),"other":leased(nested_gate())}),"keep",0).await;
    gateway.wait_for_status("s", "ready").await;
    gateway.wait_for_status("other", "ready").await;
    while provider.requests.try_recv().is_ok() {}
    // This vendor error is not recognized until the lease supplies its detector.
    gateway.call("call_mcp_tool", tool_call("echo")).await;
    assert_eq!(
        gateway
            .servers()
            .await
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["serverId"] == "s")
            .unwrap()["status"],
        "ready"
    );
    let mut replacement = credential_lease("one", json!({"token":"bad-secret-token"}), None, None);
    replacement["authFailures"] =
        json!([{"isError":true,"text":"^vendor authentication failure$"}]);
    provider.credentials(replacement);
    loop {
        if provider.next_request().await["server"] == "s" {
            break;
        }
    }
    sleep(Duration::from_millis(100)).await;
    provider.credentials(credential_lease(
        "renamed",
        json!({"token":"bad-secret-token"}),
        None,
        None,
    ));
    gateway.call("call_mcp_tool", tool_call("echo")).await;
    let failed = gateway.wait_for_status("s", "failed").await;
    assert!(
        failed["error"]
            .as_str()
            .unwrap()
            .contains("no other credential")
    );
    // Rejection memory is per server, even with the same provider and values.
    assert!(
        !gateway
            .call(
                "call_mcp_tool",
                json!({"serverId":"other","toolName":"list_mcp_servers","arguments":{}})
            )
            .await
            .1
    );
    let mut calls = 0;
    while let Ok((method, _)) = downstream.try_recv() {
        if method == "tools/call" {
            calls += 1;
        }
    }
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn concurrent_rejections_join_one_recovery() {
    let mut provider = serve_policy().await;
    let mut initial = credential_lease("one", json!({"generation":"one"}), None, None);
    initial["authFailures"] = json!([{"isError":false,"text":"^9$"}]);
    provider.credentials(initial);
    let mut server = serve_echo().await;
    let gateway =
        credential_gateway(&provider, json!({"s":leased(echo(&server.url))}), "keep", 2).await;
    gateway.wait_for_status("s", "ready").await;
    provider.next_request().await;
    let mut calls = Vec::new();
    for _ in 0..3 {
        calls.push(
            gateway
                .start_call(
                    "call_mcp_tool",
                    json!({"serverId":"s","toolName":"held","arguments":{"n":9}}),
                )
                .await,
        );
        assert_eq!(server.next_event().await, Event::Started);
    }
    provider.credentials(credential_lease(
        "two",
        json!({"generation":"two"}),
        None,
        None,
    ));
    server.release.send_replace(true);
    for call in calls {
        assert!(!finish_call(call).await.1);
    }
    assert_eq!(provider.next_request().await["rejectedCredentialId"], "one");
    gateway.wait_for_status("s", "ready").await;
    sleep(Duration::from_millis(100)).await;
    assert!(provider.requests.try_recv().is_err());
    assert!(server.events.try_recv().is_err());
}

#[tokio::test]
async fn rotation_waiters_recheck_access_before_dispatch() {
    let mut provider = serve_policy().await;
    let mut policy = serve_policy().await;
    policy.allow(&["s"]);
    provider.credentials(credential_lease(
        "one",
        json!({"generation":"one"}),
        Some(400),
        None,
    ));
    let mut server = serve_echo().await;
    let mut downstream = leased(echo(&server.url));
    downstream["access"] = json!({"policy":"p"});
    let gateway=Gateway::spawn_config(json!({
        "credentialProviders":{"p":{"http":{"url":provider.url,"headers":{"Authorization":"Bearer policy-token"}},"drainSeconds":2}},
        "accessPolicies":{"p":{"http":{"url":policy.url,"headers":{"Authorization":"Bearer policy-token"}}}},
        "mcpServers":{"s":downstream}
    }),&[TOKEN],&[],Stdio::inherit()).await.0;
    gateway.wait_for_status("s", "ready").await;
    provider.next_request().await;
    let running = gateway
        .start_call(
            "call_mcp_tool",
            json!({"serverId":"s","toolName":"held","arguments":{"n":1}}),
        )
        .await;
    assert_eq!(server.next_event().await, Event::Started);
    provider.credentials(credential_lease(
        "two",
        json!({"generation":"two"}),
        None,
        None,
    ));
    provider.next_request().await;
    gateway.wait_for_status("s", "connecting").await;
    while policy.requests.try_recv().is_ok() {}
    let waiting = gateway.start_call("call_mcp_tool", tool_call("echo")).await;
    policy.next_request().await;
    sleep(Duration::from_millis(100)).await;
    policy.allow(&[]);
    server.release.send_replace(true);
    assert!(!finish_call(running).await.1);
    let (result, error) = finish_call(waiting).await;
    assert!(
        error && result.to_string().contains("unknown server"),
        "{result}"
    );
}

#[tokio::test]
async fn leased_secrets_are_redacted_from_connection_errors_and_merged_keys_are_validated() {
    for invalid_key in [None, Some("HOOK_ONLY"), Some("BAD-NAME")] {
        let provider = serve_policy().await;
        let values = match invalid_key {
            Some(key) => json!({key:"secret-that-must-not-leak"}),
            None => json!({"TOKEN":"secret-that-must-not-leak"}),
        };
        provider.credentials(credential_lease("one", values, None, None));
        let configuration = json!({
            "hookEnv":["HOOK_ONLY"],
            "credentialProviders":{"p":{"http":{"url":provider.url,"headers":{"Authorization":"Bearer policy-token"}}}},
            "mcpServers":{"s":{"command":"/bin/sh","args":["-c","printf '%s\\n' \"$TOKEN\" >&2; exit 1"],"credentials":{"provider":"p","mergeEnv":true}}}
        });
        let gateway = Gateway::spawn_config(configuration, &[], &[], Stdio::null())
            .await
            .0;
        let state = gateway.wait_for_status("s", "failed").await;
        let error = state["error"].as_str().unwrap();
        assert!(!error.contains("secret-that-must-not-leak"), "{error}");
        if let Some(key) = invalid_key {
            assert!(error.contains(key), "{error}");
        } else {
            assert!(error.contains("<redacted>"), "{error}");
        }
    }
}

#[tokio::test]
async fn expired_refresh_is_transient_and_keeps_a_healthy_lease() {
    let mut provider = serve_policy().await;
    provider.credentials(credential_lease("one", json!({}), Some(400), Some(2000)));
    let gateway =
        credential_gateway(&provider, json!({"s":leased(nested_gate())}), "keep", 0).await;
    gateway.wait_for_status("s", "ready").await;
    provider.next_request().await;
    provider.credentials(credential_lease(
        "old",
        json!({"v":"expired"}),
        None,
        Some(-1000),
    ));
    provider.next_request().await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(gateway.servers().await[0]["status"], "ready");
    assert!(
        !gateway
            .call("call_mcp_tool", tool_call("list_mcp_servers"))
            .await
            .1
    );
    provider.credentials(credential_lease("one", json!({}), None, None));
    while provider.requests.try_recv().is_ok() {}
    provider.next_request().await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(gateway.servers().await[0]["status"], "ready");
}

#[tokio::test]
async fn ordinary_reconnect_reuses_an_unexpired_lease() {
    let mut provider = serve_policy().await;
    provider.credentials(credential_lease("one", json!({}), None, None));
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let mut server = script("echo $$ > \"$PID_FILE\"; exec \"$GATE\" --config \"$EMPTY_CONFIG\"");
    server["env"] = json!({"PID_FILE":pid_file});
    let gateway = credential_gateway(&provider, json!({"s":leased(server)}), "keep", 0).await;
    gateway.wait_for_status("s", "ready").await;
    provider.next_request().await;
    let old = std::fs::read_to_string(&pid_file).unwrap();
    provider.credentials(json!({"outcome":"denied"}));
    kill(&old);
    wait_for_new_pid(&pid_file, &old).await;
    gateway.wait_for_status("s", "ready").await;
    assert!(provider.requests.try_recv().is_err());
}

#[tokio::test]
async fn advertises_configured_gateway_metadata() {
    let gateway=Gateway::spawn_config(json!({
        "gateway":{"name":"tembo-mcp-gateway","version":"test","title":"Tembo tools","description":"Tembo gateway ${TOKEN}","instructions":"Use the search tool first. ${TOKEN}","toolDescriptions":{"execute_mcp_code":"Run a Tembo script"}},
        "mcpServers":{}
    }), &[], &[], Stdio::inherit()).await.0;
    let info = gateway.client.peer_info().unwrap();
    assert_eq!(info.server_info.as_ref().unwrap().name, "tembo-mcp-gateway");
    assert_eq!(info.server_info.as_ref().unwrap().version, "test");
    assert_eq!(
        info.server_info.as_ref().unwrap().title.as_deref(),
        Some("Tembo tools")
    );
    assert_eq!(
        info.server_info.as_ref().unwrap().description.as_deref(),
        Some("Tembo gateway ${TOKEN}")
    );
    assert_eq!(
        info.instructions.as_deref(),
        Some("Use the search tool first. ${TOKEN}")
    );
    let tools = gateway.client.list_all_tools().await.unwrap();
    assert_eq!(tools.len(), 5);
    let execute = tools
        .iter()
        .find(|tool| tool.name == "execute_mcp_code")
        .unwrap();
    assert_eq!(execute.description.as_deref(), Some("Run a Tembo script"));
    assert_eq!(execute.input_schema["properties"]["code"]["type"], "string");
    assert_eq!(gateway.run_code("return 7").await.0, json!({"result":7}));
    let default = Gateway::start(json!({}), &[]).await;
    assert_eq!(
        default
            .client
            .peer_info()
            .unwrap()
            .server_info
            .as_ref()
            .unwrap()
            .name,
        "gate"
    );
}

#[tokio::test]
async fn runs_typescript_and_web_api_compatibility_fixtures() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/code-compat.json")).unwrap();
    let gateway = Gateway::start(json!({}), &[]).await;
    for fixture in fixtures {
        let (result, error) = gateway.run_code(fixture["code"].as_str().unwrap()).await;
        assert!(!error, "{}: {result}", fixture["name"]);
        assert_eq!(result["result"], fixture["expected"], "{}", fixture["name"]);
    }
}

#[tokio::test]
async fn timers_are_bounded_and_do_not_keep_finished_scripts_alive() {
    let gateway = Gateway::spawn(json!({}), &[], &["--code-timeout", "1"], Stdio::inherit())
        .await
        .0;
    let (result, error) = timeout(
        Duration::from_secs(2),
        gateway.run_code("setTimeout(() => console.log('late'), 30000); return 1;"),
    )
    .await
    .unwrap();
    assert!(!error, "{result}");
    assert_eq!(result, json!({"result":1}));
    let error = gateway
        .code_error("await new Promise(resolve => setTimeout(resolve, 30000));")
        .await;
    assert!(error.contains("timed out"), "{error}");
    let error = gateway
        .code_error("for (let i=0; i<257; i++) setTimeout(() => {}, 30000);")
        .await;
    assert!(error.contains("256 timers"), "{error}");
    let (result, error) = gateway
        .run_code("for (let i=0; i<500; i++) clearTimeout(setTimeout(() => {}, 30000)); return 1;")
        .await;
    assert!(!error, "{result}");
}

#[tokio::test]
async fn typescript_errors_refer_to_the_snippet_and_calls_still_work() {
    let server = serve_echo().await;
    let gateway = Gateway::start(json!({"echo":echo(&server.url)}), &[TOKEN]).await;
    gateway.wait_for_status("echo", "ready").await;
    let (result, error) = gateway
        .run_code("const result: unknown = await mcp.echo.echo(); return result;")
        .await;
    assert!(!error, "{result}");
    assert_eq!(result["result"]["content"][0]["text"], "echo");
    let error = gateway
        .code_error("const x: number = 1;\nconst y: = 2; return x;")
        .await;
    assert!(error.contains("line 2"), "{error}");
}

#[tokio::test]
async fn plain_http_servers_reuse_tcp_connections() {
    use axum::extract::ConnectInfo;
    let peers = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let observed = peers.clone();
    let router=axum::Router::new().route("/mcp",axum::routing::post(move |ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>, axum::Json(body): axum::Json<Value>| {
        observed.lock().unwrap().insert(peer);
        async move {
            if body.get("id").is_none() { return StatusCode::ACCEPTED.into_response(); }
            let result=match body["method"].as_str().unwrap_or("") {
                "initialize" => json!({"protocolVersion":body["params"]["protocolVersion"],"capabilities":{"tools":{}},"serverInfo":{"name":"pool-test","version":"1"}}),
                "tools/list" => json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
                "tools/call" => json!({"content":[{"type":"text","text":"ok"}]}),
                _ => return axum::Json(json!({"jsonrpc":"2.0","id":body["id"],"error":{"code":-32601,"message":"unknown method"}})).into_response(),
            };
            axum::Json(json!({"jsonrpc":"2.0","id":body["id"],"result":result})).into_response()
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let gateway = Gateway::start(json!({"s":{"url":url}}), &[]).await;
    gateway.wait_for_status("s", "ready").await;
    peers.lock().unwrap().clear();
    for _ in 0..5 {
        assert!(!gateway.call("call_mcp_tool", tool_call("echo")).await.1);
    }
    assert_eq!(peers.lock().unwrap().len(), 1);
    task.abort();
}
