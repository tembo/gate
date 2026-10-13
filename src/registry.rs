use std::{collections::BTreeMap, collections::HashMap, process::Stdio, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient, ServiceExt,
    model::{ClientConfig, Implementation, Tool},
    service::{Peer, RunningService},
    transport::{
        IntoTransport, StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{ChildStderr, Command},
    sync::watch,
    time::timeout,
};
use url::Url;

use crate::config::{Config, ServerId, Transport};

/// Max. time we'll spend waiting for a server to connect and list its tools
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// How much of a stdio server's stderr is kept to explain a failed connection
const STDERR_TAIL_BYTES: usize = 4096;

/// Max. read from a stderr line before we save it into gate's own stderr.
/// E.g. suppose an MCP server named `noisy` was buggy and emitted 10KB to stderr, gate would display it as:
///
/// ```text
/// [mcp:noisy] xxxx…   (4096 bytes)
/// [mcp:noisy] xxxx…   (4096 bytes)
/// [mcp:noisy] xxxx…   (1808 bytes)
/// ```
const STDERR_LINE_BYTES: usize = 4096;

#[derive(Clone)]
pub enum Status {
    Connecting,
    Ready(Arc<Connection>),
    Failed(Arc<str>),
}

pub struct Connection {
    // will eventually be used by `call_mcp_tool`
    #[allow(dead_code)]
    pub peer: Peer<RoleClient>,
    pub tools: Vec<Tool>,
}

pub struct Registry {
    servers: BTreeMap<ServerId, watch::Receiver<Status>>,
}

impl Registry {
    /// Starts connecting to every configured server in the background
    pub fn start(config: &Config) -> Self {
        let servers = config
            .servers
            .iter()
            .map(|(id, server)| {
                let (status, receiver) = watch::channel(Status::Connecting);
                tokio::spawn(run(id.clone(), server.transport.clone(), status));
                (id.clone(), receiver)
            })
            .collect();

        Self { servers }
    }

    pub fn status(&self, server_id: &str) -> Option<Status> {
        Some(self.servers.get(server_id)?.borrow().clone())
    }

    /// Looks up a tool's definition, waiting for its server if it's still connecting
    pub async fn tool(&self, server_id: &str, tool_name: &str) -> Result<Tool, String> {
        let mut status = self
            .servers
            .get(server_id)
            .ok_or_else(|| format!("unknown server `{server_id}`"))?
            .clone();
        let status = status
            .wait_for(|status| !matches!(status, Status::Connecting))
            .await
            .map_err(|_| format!("server `{server_id}` stopped connecting"))?
            .clone();
        match status {
            Status::Ready(connection) => connection
                .tools
                .iter()
                .find(|tool| tool.name == tool_name)
                .cloned()
                .ok_or_else(|| format!("server `{server_id}` has no tool `{tool_name}`")),
            Status::Failed(error) => Err(format!("server `{server_id}` failed: {error}")),
            Status::Connecting => unreachable!("waited for the server to stop connecting"),
        }
    }
}

/// Connects to a server and keeps its status up to date
async fn run(id: ServerId, transport: Transport, status: watch::Sender<Status>) {
    let (client, tools) = match connect(&id, &transport).await {
        Ok(connected) => connected,
        Err(error) => {
            let error = format!("{error:#}");
            eprintln!("[gate] failed to connect to `{id}`: {error}");
            status.send_replace(Status::Failed(error.into()));
            return;
        }
    };
    status.send_replace(Status::Ready(Arc::new(Connection {
        peer: client.peer().clone(),
        tools,
    })));

    let _ = client.waiting().await;
    eprintln!("[gate] connection to `{id}` closed");
    status.send_replace(Status::Failed("connection closed".into()));
}

type Client = RunningService<RoleClient, ClientConfig>;

async fn connect(id: &str, transport: &Transport) -> Result<(Client, Vec<Tool>)> {
    match transport {
        Transport::Stdio { command, args, env } => {
            let mut cmd = Command::new(command);
            // The child inherits our environment, plus its own `env`
            cmd.args(args).envs(env).kill_on_drop(true);
            let (child, stderr) = TokioChildProcess::builder(cmd)
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| format!("failed to start `{command}`"))?;
            let stderr = stderr.map(|stderr| tokio::spawn(forward_stderr(id.to_owned(), stderr)));

            match handshake(child).await {
                Ok(connected) => Ok(connected),
                Err(error) => {
                    // The child is killed once the handshake fails, so its stderr ends shortly.
                    // Its last lines usually explain the failure.
                    let tail = match stderr {
                        Some(task) => timeout(Duration::from_secs(1), task)
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .unwrap_or_default(),
                        None => String::new(),
                    };
                    match tail.trim() {
                        "" => Err(error),
                        tail => Err(anyhow!("{error:#}\nstderr:\n{tail}")),
                    }
                }
            }
        }
        Transport::Http { url, headers } => {
            let custom_headers = headers
                .iter()
                .map(|(name, value)| {
                    // Header values may hold secrets, so errors only name the header
                    let parsed = (HeaderName::try_from(name), HeaderValue::try_from(value));
                    match parsed {
                        (Ok(name), Ok(value)) => Ok((name, value)),
                        _ => Err(anyhow!("invalid header `{name}`")),
                    }
                })
                .collect::<Result<HashMap<_, _>>>()?;
            let transport = StreamableHttpClientTransport::from_config(
                StreamableHttpClientTransportConfig::with_uri(url.as_str())
                    .custom_headers(custom_headers),
            );
            handshake(transport)
                .await
                .map_err(|error| anyhow!("{}", redact_urls(&format!("{error:#}"))))
        }
    }
}

/// Initializes the session and fetches every page of the server's tools
async fn handshake<T, E, A>(transport: T) -> Result<(Client, Vec<Tool>)>
where
    T: IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let attempt = async {
        let client = client_config()
            .serve(transport)
            .await
            .context("failed to initialize")?;
        let tools = client
            .list_all_tools()
            .await
            .context("failed to list tools")?;
        Ok((client, tools))
    };
    timeout(CONNECT_TIMEOUT, attempt)
        .await
        .map_err(|_| anyhow!("timed out after {}s", CONNECT_TIMEOUT.as_secs()))?
}

/// Transport errors can include request URLs, which may hold secrets from placeholders. The
/// HTTP client reformats them, for example dropping userinfo, so rather than looking for the
/// configured URL this replaces every URL in the message with its origin.
fn redact_urls(message: &str) -> String {
    let mut redacted = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| rest.find(scheme))
        .min()
    {
        redacted.push_str(&rest[..start]);
        rest = &rest[start..];
        // Serialized URLs never contain whitespace
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        // Punctuation around a URL in prose, like "(url),", isn't part of it
        let url = rest[..end].trim_end_matches([')', ']', ',', '.', ';', ':', '"', '\'']);
        match Url::parse(url) {
            Ok(parsed) => {
                redacted.push_str(&parsed.origin().ascii_serialization());
                redacted.push_str(&rest[url.len()..end]);
            }
            Err(_) => redacted.push_str("<url>"),
        }
        rest = &rest[end..];
    }
    redacted.push_str(rest);
    redacted
}

fn client_config() -> ClientConfig {
    let mut info = ClientConfig::default();
    info.client_info = Implementation::new("gate", env!("CARGO_PKG_VERSION"));
    info
}

/// Copies a server's stderr to ours, prefixed with its name, and returns the end of it
async fn forward_stderr(id: ServerId, stderr: ChildStderr) -> String {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::new();
    let mut tail = String::new();
    // Reads raw bytes so non-UTF-8 output can't stop us from draining the pipe
    loop {
        let chunk = match reader.fill_buf().await {
            Ok([]) | Err(_) => break,
            // Takes no more than fits in the current piece of the line
            Ok(chunk) => &chunk[..chunk.len().min(STDERR_LINE_BYTES - line.len())],
        };
        let (read, ends_line) = match chunk.iter().position(|&byte| byte == b'\n') {
            Some(newline) => (newline + 1, true),
            None => (chunk.len(), false),
        };
        line.extend_from_slice(&chunk[..read]);
        reader.consume(read);
        if ends_line || line.len() >= STDERR_LINE_BYTES {
            forward_line(&id, &line, &mut tail);
            line.clear();
        }
    }
    if !line.is_empty() {
        forward_line(&id, &line, &mut tail);
    }
    tail
}

fn forward_line(id: &str, line: &[u8], tail: &mut String) {
    let text = String::from_utf8_lossy(line);
    let text = text.trim_end();
    eprintln!("[mcp:{id}] {text}");
    tail.push_str(text);
    tail.push('\n');
    if tail.len() > STDERR_TAIL_BYTES {
        let start = tail.ceil_char_boundary(tail.len() - STDERR_TAIL_BYTES);
        tail.drain(..start);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_urls() {
        let cases = [
            (
                "error sending request for url (http://127.0.0.1:1/mcp?key=secret), when",
                "error sending request for url (http://127.0.0.1:1), when",
            ),
            (
                "at https://user:secret@example.com/secret",
                "at https://example.com",
            ),
            (
                "http://a.com/x and http://b.com:8080/y.",
                "http://a.com and http://b.com:8080.",
            ),
            ("broken http://[secret", "broken <url>"),
            ("no urls here", "no urls here"),
        ];
        for (message, expected) in cases {
            assert_eq!(redact_urls(message), expected);
        }
    }
}
