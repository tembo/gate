use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use futures::future::join_all;
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{
        CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotificationParam,
        ClientConfig, ClientRequest, Implementation, JsonObject, ServerResult, Tool,
    },
    service::{Peer, PeerRequestOptions, RunningService},
    transport::{
        IntoTransport, StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{ChildStderr, Command},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    config::{Config, ServerId, Transport},
    search::{CatalogServer, ServerState},
    spawn,
};

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
const HTTP_CONCURRENT_REQUESTS: usize = 256;

/// How long to wait before reconnecting to a server whose connection closed, like the
/// original gateway. The retry delays then apply as usual.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How long to wait before the second and third connection attempts
pub type RetryDelays = [Duration; 2];

/// The background connection concurrency limit: how many background attempts (at startup,
/// retrying, or reconnecting) run at once, like the original gateway's four workers.
/// Attempts made for a request skip it, and a server holds a permit only while attempting,
/// not once connected.
const MAX_BACKGROUND_ATTEMPTS: usize = 4;

#[derive(Clone)]
pub enum Status {
    /// An attempt is in progress. Requests wait for it.
    Attempting(Attempt),
    /// Waiting to retry or reconnect, or for a background permit. Requests fail fast instead
    /// of waiting.
    Pending,
    Ready(Arc<Connection>),
    /// Every attempt failed. The next request that needs the server makes another.
    Failed(Arc<str>),
}

type AttemptResult = Result<Arc<Connection>, Arc<str>>;
/// Sets an attempt's result, once it's done
type AttemptSender = watch::Sender<Option<AttemptResult>>;

/// One connection attempt's result, shared by every request waiting on it. Each attempt has
/// its own handle, so a request gets the result of the attempt it waited on, even if the
/// server has moved on to later attempts by the time it looks.
#[derive(Clone)]
pub struct Attempt(watch::Receiver<Option<AttemptResult>>);

impl Attempt {
    fn new() -> (AttemptSender, Self) {
        let (result, receiver) = watch::channel(None);
        (result, Self(receiver))
    }

    async fn result(mut self) -> AttemptResult {
        // The result is set once and never changes, so we can't miss it
        match self.0.wait_for(Option::is_some).await {
            Ok(result) => result.clone().expect("waited for the result"),
            Err(_) => Err("the connection attempt was abandoned".into()),
        }
    }
}

pub struct Connection {
    pub peer: Peer<RoleClient>,
    pub tools: Arc<[Tool]>,
}

struct ServerHandle {
    description: Option<String>,
    status: Arc<watch::Sender<Status>>,
    /// Asks the server's task to make an attempt for a request, once the server has failed
    retry: mpsc::UnboundedSender<AttemptSender>,
}

pub struct Registry {
    servers: BTreeMap<ServerId, ServerHandle>,
    /// Lets tests hold every permit
    #[cfg(test)]
    background_permits: Arc<Semaphore>,
    call_timeout: Duration,
}

/// What a request does about a server, decided from its status
enum Plan {
    Use(Arc<Connection>),
    Wait(Attempt),
    Fail(String),
}

impl Registry {
    /// Starts connecting to every configured server in the background
    pub fn start(config: &Config, call_timeout: Duration, retry_delays: RetryDelays) -> Self {
        let background_permits = Arc::new(Semaphore::new(MAX_BACKGROUND_ATTEMPTS));
        let servers = config
            .servers
            .iter()
            .map(|(id, server)| {
                // Servers that get a permit now start with their first attempt in place, so
                // requests wait for it from the start. The rest are queued.
                let first_attempt =
                    background_permits
                        .clone()
                        .try_acquire_owned()
                        .ok()
                        .map(|permit| {
                            let (result, attempt) = Attempt::new();
                            (permit, result, attempt)
                        });
                let status = match &first_attempt {
                    Some((_, _, attempt)) => Status::Attempting(attempt.clone()),
                    None => Status::Pending,
                };
                let status = Arc::new(watch::Sender::new(status));
                let (retry, retries) = mpsc::unbounded_channel();
                let supervisor = Supervisor {
                    id: id.clone(),
                    transport: server.transport.clone(),
                    status: status.clone(),
                    retry_delays,
                    background_permits: background_permits.clone(),
                };
                let first_attempt = first_attempt.map(|(permit, result, _)| (permit, result));
                tokio::spawn(supervisor.run(first_attempt, retries));
                (
                    id.clone(),
                    ServerHandle {
                        description: server.description.clone(),
                        status,
                        retry,
                    },
                )
            })
            .collect();

        Self {
            servers,
            #[cfg(test)]
            background_permits,
            call_timeout,
        }
    }

    /// Each server's status, to watch for changes
    pub fn subscribe(&self) -> Vec<watch::Receiver<Status>> {
        self.servers
            .values()
            .map(|server| server.status.subscribe())
            .collect()
    }

    /// Captures each server's status once, for a batch of searches. Never waits for a
    /// connection or starts a retry.
    pub fn catalog(&self) -> Vec<CatalogServer> {
        self.servers
            .iter()
            .map(|(id, server)| CatalogServer {
                id: id.clone(),
                description: server.description.clone(),
                state: match &*server.status.borrow() {
                    Status::Ready(connection) => ServerState::Ready(connection.tools.clone()),
                    Status::Attempting(_) | Status::Pending => ServerState::Connecting,
                    Status::Failed(error) => ServerState::Failed(error.clone()),
                },
            })
            .collect()
    }

    pub fn status(&self, server_id: &str) -> Option<Status> {
        Some(self.servers.get(server_id)?.status.borrow().clone())
    }

    /// Looks up tools' definitions as `(server_id, tool_name)` pairs, returning results in
    /// the same order.
    ///
    /// Gets each server's connection once, and every server's at the same time, like the
    /// original gateway. Getting them per tool would make a failed server retry once for each
    /// of its tools, one after another.
    pub async fn tools(
        &self,
        tools: &[(&str, &str)],
        ct: &CancellationToken,
    ) -> Vec<Result<Tool, String>> {
        let server_ids: BTreeSet<&str> = tools.iter().map(|&(server_id, _)| server_id).collect();
        let connections = join_all(server_ids.iter().map(|id| self.connection(id, ct))).await;
        let connections: HashMap<_, _> = server_ids.into_iter().zip(connections).collect();
        tools
            .iter()
            .map(|&(server_id, tool_name)| {
                connections[server_id]
                    .as_ref()
                    .map_err(Clone::clone)?
                    .tools
                    .iter()
                    .find(|tool| tool.name == tool_name)
                    .cloned()
                    .ok_or_else(|| format!("server `{server_id}` has no tool `{tool_name}`"))
            })
            .collect()
    }

    /// Calls a tool, waiting for its server if it's still connecting.
    ///
    /// Timeouts and cancellation are passed on to the server, but only as requests: neither
    /// guarantees that the server stopped, or that it undid what it had done.
    pub async fn call(
        &self,
        server_id: &str,
        tool_name: &str,
        arguments: JsonObject,
        ct: &CancellationToken,
    ) -> Result<CallToolResult, String> {
        let connection = self.connection(server_id, ct).await?;
        let cancelled = || format!("calling `{tool_name}` on `{server_id}` was cancelled");
        // The caller may have cancelled just as the server finished connecting
        if ct.is_cancelled() {
            return Err(cancelled());
        }

        let failed = |error| match error {
            ServiceError::Timeout { timeout } => format!(
                "calling `{tool_name}` on `{server_id}` timed out after {}s; the downstream outcome is unknown",
                timeout.as_secs()
            ),
            // Transport errors can include the server's URL
            error => redact_urls(&format!(
                "calling `{tool_name}` on `{server_id}` failed: {error}"
            )),
        };
        let params = CallToolRequestParams::new(tool_name.to_owned()).with_arguments(arguments);
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(params));
        // When rmcp's timeout fires, it also tells the server to cancel the request
        let request = connection
            .peer
            .send_request_with_option(request, PeerRequestOptions::with_timeout(self.call_timeout))
            .await
            .map_err(failed)?;

        let request_id = request.id.clone();
        let response = tokio::select! {
            biased;
            response = request.await_response() => response.map_err(failed)?,
            () = ct.cancelled() => {
                let _ = connection
                    .peer
                    .notify_cancelled(CancelledNotificationParam::new(
                        Some(request_id),
                        Some("cancelled by the caller".into()),
                    ))
                    .await;
                return Err(cancelled());
            }
        };
        match response {
            ServerResult::CallToolResult(result) => Ok(result),
            _ => Err(format!(
                "`{server_id}` sent an unexpected response to calling `{tool_name}`"
            )),
        }
    }

    /// Gets a server's connection. If an attempt to connect is in progress, waits for that
    /// attempt, but not for any retries after it. If the server has failed, makes one more
    /// attempt and waits for it.
    async fn connection(
        &self,
        server_id: &str,
        ct: &CancellationToken,
    ) -> Result<Arc<Connection>, String> {
        let server = self
            .servers
            .get(server_id)
            .ok_or_else(|| format!("unknown server `{server_id}`"))?;
        let failed = |error| format!("server `{server_id}` failed: {error}");

        // Decides what to do and captures what to wait on in one step, so the server can't
        // change status in between. Otherwise, an attempt that failed quickly could make this
        // request start another.
        let mut plan = None;
        server.status.send_if_modified(|status| {
            let (next, modified) = match status {
                Status::Ready(connection) => (Plan::Use(connection.clone()), false),
                Status::Attempting(attempt) => (Plan::Wait(attempt.clone()), false),
                Status::Pending => (
                    Plan::Fail(format!("server `{server_id}` is still connecting")),
                    false,
                ),
                Status::Failed(error) if ct.is_cancelled() => {
                    (Plan::Fail(failed(error.clone())), false)
                }
                Status::Failed(_) => {
                    let (result, attempt) = Attempt::new();
                    // Hands the attempt to the server's task before publishing it, so the
                    // server can't be left `Attempting` with nobody making the attempt. The
                    // task can't change the status until this closure returns.
                    match server.retry.send(result) {
                        Ok(()) => {
                            *status = Status::Attempting(attempt.clone());
                            (Plan::Wait(attempt), true)
                        }
                        Err(_) => (
                            Plan::Fail(format!(
                                "server `{server_id}` can't retry: its task stopped"
                            )),
                            false,
                        ),
                    }
                }
            };
            plan = Some(next);
            modified
        });

        let attempt = match plan.expect("decided while checking the status") {
            Plan::Use(connection) => return Ok(connection),
            Plan::Fail(error) => return Err(error),
            Plan::Wait(attempt) => attempt,
        };
        // Cancelling only stops this request from waiting. The attempt belongs to the server's
        // task, and carries on for other requests.
        tokio::select! {
            result = attempt.result() => result.map_err(failed),
            () = ct.cancelled() => {
                Err(format!("cancelled while waiting for `{server_id}` to connect"))
            }
        }
    }
}

/// A background permit, and the result sender for the attempt it was taken for
type FirstAttempt = (OwnedSemaphorePermit, AttemptSender);

/// Keeps a server connected, and its status up to date, for as long as the gateway runs
struct Supervisor {
    id: ServerId,
    transport: Transport,
    status: Arc<watch::Sender<Status>>,
    retry_delays: RetryDelays,
    background_permits: Arc<Semaphore>,
}

impl Supervisor {
    /// Unlike the original gateway, which retried servers in rounds (finishing every first
    /// attempt before the first delay, and so on), each server retries on its own schedule.
    async fn run(
        self,
        mut first_attempt: Option<FirstAttempt>,
        mut retries: mpsc::UnboundedReceiver<AttemptSender>,
    ) {
        loop {
            let client = match self.connect_with_retries(first_attempt.take()).await {
                Some(client) => client,
                // Once failed, makes one attempt each time a request asks for one, until one
                // connects
                None => loop {
                    let Some(result) = retries.recv().await else {
                        return;
                    };
                    eprintln!("[gate] retrying `{}` for a request", self.id);
                    match self.connect().await {
                        Ok((client, connection)) => {
                            self.ready(result, connection);
                            break client;
                        }
                        Err(error) => {
                            eprintln!("[gate] retrying `{}` failed: {error}", self.id);
                            let error: Arc<str> = error.into();
                            result.send_replace(Some(Err(error.clone())));
                            self.status.send_replace(Status::Failed(error));
                        }
                    }
                },
            };
            let _ = client.waiting().await;
            eprintln!(
                "[gate] connection to `{}` closed; reconnecting in {RECONNECT_DELAY:?}",
                self.id
            );
            self.status.send_replace(Status::Pending);
            sleep(RECONNECT_DELAY).await;
        }
    }

    /// Makes up to three background attempts, leaving the server `Ready` or `Failed`. Uses
    /// `first_attempt` for the first, if it's already in the server's status.
    async fn connect_with_retries(
        &self,
        mut first_attempt: Option<FirstAttempt>,
    ) -> Option<Client> {
        let [first_delay, second_delay] = self.retry_delays;
        let delays = [Some(first_delay), Some(second_delay), None];
        for (attempt_number, retry_delay) in (1..).zip(delays) {
            let (permit, result) = match first_attempt.take() {
                Some(first_attempt) => first_attempt,
                None => {
                    // The server stays `Pending` while it waits for a permit
                    let permit = self
                        .background_permits
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("the semaphore is never closed");
                    let (result, attempt) = Attempt::new();
                    self.status.send_replace(Status::Attempting(attempt));
                    (permit, result)
                }
            };
            let connected = self.connect().await;
            drop(permit);
            let error = match connected {
                Ok((client, connection)) => {
                    self.ready(result, connection);
                    return Some(client);
                }
                Err(error) => error,
            };

            let id = &self.id;
            let Some(retry_delay) = retry_delay else {
                eprintln!("[gate] attempt {attempt_number}/3 to connect to `{id}` failed: {error}");
                let error: Arc<str> = error.into();
                result.send_replace(Some(Err(error.clone())));
                self.status.send_replace(Status::Failed(error));
                return None;
            };
            let error = format!("{error}; retrying in {retry_delay:?}");
            eprintln!("[gate] attempt {attempt_number}/3 to connect to `{id}` failed: {error}");
            result.send_replace(Some(Err(error.into())));
            self.status.send_replace(Status::Pending);
            sleep(retry_delay).await;
        }
        unreachable!("the last attempt either connects or fails")
    }

    async fn connect(&self) -> Result<(Client, Arc<Connection>), String> {
        let (client, tools) = connect(&self.id, &self.transport)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let peer = client.peer().clone();
        let tools = tools.into();
        Ok((client, Arc::new(Connection { peer, tools })))
    }

    fn ready(&self, result: AttemptSender, connection: Arc<Connection>) {
        result.send_replace(Some(Ok(connection.clone())));
        self.status.send_replace(Status::Ready(connection));
    }
}

type Client = RunningService<RoleClient, ClientConfig>;

async fn connect(id: &str, transport: &Transport) -> Result<(Client, Vec<Tool>)> {
    match transport {
        Transport::Stdio { command, args, env } => {
            let mut cmd = Command::new(command);
            // The child inherits our environment, plus its own `env`
            cmd.args(args).envs(env).kill_on_drop(true);
            let spawned = {
                let _spawning = spawn::lock();
                TokioChildProcess::builder(cmd)
                    .stderr(Stdio::piped())
                    .spawn()
            };
            let (child, stderr) =
                spawned.with_context(|| format!("failed to start `{command}`"))?;
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
            let transport = StreamableHttpClientTransport::from_config({
                let mut config = StreamableHttpClientTransportConfig::with_uri(url.as_str())
                    .custom_headers(custom_headers)
                    .max_concurrent_requests(HTTP_CONCURRENT_REQUESTS);

                config.channel_buffer_capacity = HTTP_CONCURRENT_REQUESTS;
                config
            });
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

    /// A registry with one server, `s`, whose status the test controls. Its retry requests
    /// arrive on the returned receiver.
    fn registry() -> (
        Registry,
        Arc<watch::Sender<Status>>,
        mpsc::UnboundedReceiver<AttemptSender>,
    ) {
        let status = Arc::new(watch::Sender::new(Status::Pending));
        let (retry, retries) = mpsc::unbounded_channel();
        let server = ServerHandle {
            description: None,
            status: status.clone(),
            retry,
        };
        let registry = Registry {
            servers: BTreeMap::from([("s".to_owned(), server)]),
            background_permits: Arc::new(Semaphore::new(MAX_BACKGROUND_ATTEMPTS)),
            call_timeout: Duration::from_secs(1),
        };
        (registry, status, retries)
    }

    #[tokio::test]
    async fn requests_get_the_result_of_the_attempt_they_waited_on() {
        let (registry, status, _retries) = registry();
        let ct = CancellationToken::new();
        let (first_result, first) = Attempt::new();
        status.send_replace(Status::Attempting(first));

        let mut request = std::pin::pin!(registry.connection("s", &ct));
        // Polls the request once, so it starts waiting on the first attempt
        assert!(timeout(Duration::ZERO, &mut request).await.is_err());

        // The server moves through another attempt before the request looks
        first_result.send_replace(Some(Err("first".into())));
        status.send_replace(Status::Pending);
        let (second_result, second) = Attempt::new();
        status.send_replace(Status::Attempting(second));
        second_result.send_replace(Some(Err("second".into())));
        status.send_replace(Status::Failed("second".into()));

        assert_eq!(request.await.err().unwrap(), "server `s` failed: first");
    }

    #[tokio::test]
    async fn fails_fast_between_attempts() {
        let (registry, _status, _retries) = registry();
        let error = registry.connection("s", &CancellationToken::new()).await;
        assert_eq!(error.err().unwrap(), "server `s` is still connecting");
    }

    #[tokio::test]
    async fn cancelling_stops_waiting_but_not_the_attempt() {
        let (registry, status, _retries) = registry();
        let (result, attempt) = Attempt::new();
        status.send_replace(Status::Attempting(attempt));

        let ct = CancellationToken::new();
        let mut cancelled = std::pin::pin!(registry.connection("s", &ct));
        assert!(timeout(Duration::ZERO, &mut cancelled).await.is_err());
        let other = CancellationToken::new();
        let mut other = std::pin::pin!(registry.connection("s", &other));
        assert!(timeout(Duration::ZERO, &mut other).await.is_err());

        ct.cancel();
        assert_eq!(
            cancelled.await.err().unwrap(),
            "cancelled while waiting for `s` to connect"
        );
        result.send_replace(Some(Err("refused".into())));
        assert_eq!(other.await.err().unwrap(), "server `s` failed: refused");
    }

    #[tokio::test]
    async fn retries_failed_servers_on_request() {
        let (registry, status, mut retries) = registry();
        status.send_replace(Status::Failed("old".into()));
        let ct = CancellationToken::new();

        let mut first = std::pin::pin!(registry.connection("s", &ct));
        assert!(timeout(Duration::ZERO, &mut first).await.is_err());
        let mut second = std::pin::pin!(registry.connection("s", &ct));
        assert!(timeout(Duration::ZERO, &mut second).await.is_err());

        // The first request started one attempt, and the second joined it
        assert!(matches!(*status.borrow(), Status::Attempting(_)));
        let result = retries.try_recv().unwrap();
        assert!(retries.try_recv().is_err());
        result.send_replace(Some(Err("new".into())));
        assert_eq!(first.await.err().unwrap(), "server `s` failed: new");
        assert_eq!(second.await.err().unwrap(), "server `s` failed: new");
    }

    #[tokio::test]
    async fn cancelled_requests_do_not_retry() {
        let (registry, status, mut retries) = registry();
        status.send_replace(Status::Failed("old".into()));
        let ct = CancellationToken::new();
        ct.cancel();

        let error = registry.connection("s", &ct).await.err().unwrap();
        assert_eq!(error, "server `s` failed: old");
        assert!(retries.try_recv().is_err());
        assert!(matches!(*status.borrow(), Status::Failed(_)));
    }

    #[tokio::test]
    async fn stays_failed_if_the_task_stopped() {
        let (registry, status, retries) = registry();
        status.send_replace(Status::Failed("old".into()));
        drop(retries);

        let error = registry.connection("s", &CancellationToken::new()).await;
        assert_eq!(
            error.err().unwrap(),
            "server `s` can't retry: its task stopped"
        );
        assert!(matches!(*status.borrow(), Status::Failed(_)));
    }

    #[tokio::test]
    async fn request_retries_skip_the_background_limit() {
        let server = crate::config::Server {
            description: None,
            transport: Transport::Stdio {
                command: "sh".into(),
                args: vec!["-c".into(), "echo boom >&2; exit 1".into()],
                env: BTreeMap::new(),
            },
        };
        let config = Config {
            servers: BTreeMap::from([("s".to_owned(), server)]),
        };
        let delays = [Duration::from_millis(10); 2];
        let registry = Registry::start(&config, Duration::from_secs(1), delays);
        let mut status = registry.servers["s"].status.subscribe();
        timeout(
            Duration::from_secs(10),
            status.wait_for(|status| matches!(status, Status::Failed(_))),
        )
        .await
        .unwrap()
        .unwrap();

        // With every permit held, only an attempt that skips the limit can run
        let _permits = registry
            .background_permits
            .clone()
            .acquire_many_owned(MAX_BACKGROUND_ATTEMPTS as u32)
            .await
            .unwrap();
        let ct = CancellationToken::new();
        let error = timeout(Duration::from_secs(10), registry.connection("s", &ct))
            .await
            .expect("the request's attempt shouldn't wait for a permit")
            .err()
            .unwrap();
        assert!(error.contains("boom"), "{error}");
    }

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
