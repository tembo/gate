//! Runs the `gate` binary against real downstream servers:
//! 1. another `gate` over stdio
//! 2. in-process server over Streamable HTTP

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::{
    RoleClient, ServerHandler, ServiceExt,
    model::CallToolRequestParams,
    service::RunningService,
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
    time::timeout,
};

const GATE: &str = env!("CARGO_BIN_EXE_gate");

struct Gateway {
    client: RunningService<RoleClient, ()>,
    _dir: TempDir,
}

impl Gateway {
    /// Starts `gate` with these servers. `${EMPTY_CONFIG}` points at a config with no servers,
    /// for running `gate` itself as a downstream server
    async fn start(servers: Value, env: &[(&str, &str)]) -> Self {
        Self::spawn(servers, env, Stdio::inherit()).await.0
    }

    /// Like `start`, with control over the gateway's stderr
    async fn spawn(
        servers: Value,
        env: &[(&str, &str)],
        stderr: Stdio,
    ) -> (Self, Option<ChildStderr>) {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("mcp.json");
        let empty_config = dir.path().join("empty.json");
        std::fs::write(&config, json!({ "mcpServers": servers }).to_string()).unwrap();
        std::fs::write(&empty_config, r#"{ "mcpServers": {} }"#).unwrap();

        let mut command = Command::new(GATE);
        command
            .arg("--config")
            .arg(&config)
            .env("EMPTY_CONFIG", &empty_config)
            .envs(env.iter().copied());
        let (transport, stderr) = TokioChildProcess::builder(command)
            .stderr(stderr)
            .spawn()
            .unwrap();
        let client = ().serve(transport).await.unwrap();
        (Self { client, _dir: dir }, stderr)
    }

    async fn call(&self, tool: &str, arguments: Value) -> (Value, bool) {
        let params = CallToolRequestParams::new(tool.to_owned())
            .with_arguments(arguments.as_object().unwrap().clone());
        let result = self.client.call_tool(params).await.unwrap();
        let text = result.content[0].as_text().unwrap().text.clone();
        let output = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (output, result.is_error == Some(true))
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

#[derive(Clone)]
struct Echo;

#[tool_router]
impl Echo {
    #[tool(description = "Echoes")]
    async fn echo(&self) -> String {
        "echo".into()
    }
}

#[tool_handler]
impl ServerHandler for Echo {}

/// Serves `Echo` over Streamable HTTP, only to requests with `Authorization: Bearer test-token`
async fn serve_echo() -> String {
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

    let service: StreamableHttpService<Echo, LocalSessionManager> =
        StreamableHttpService::new(|| Ok(Echo), Default::default(), Default::default());
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(require_token));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });
    url
}

#[tokio::test]
async fn discovers_http_tools_with_headers() {
    let url = serve_echo().await;
    let gateway = Gateway::start(
        json!({
            "echo": { "url": url, "headers": { "Authorization": "Bearer ${TOKEN}" } },
            "denied": { "url": url },
        }),
        &[("TOKEN", "test-token")],
    )
    .await;

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
    assert_eq!(servers[1]["toolCount"], 1);
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
    let url = serve_echo().await;
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
