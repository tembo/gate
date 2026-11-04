use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    process::Stdio,
    sync::{Arc, Mutex},
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
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch},
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
    Rotating(Attempt),
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
    calls: Mutex<Calls>,
    drained: Notify,
    interrupted: CancellationToken,
    events: mpsc::UnboundedSender<CredentialEvent>,
    redactor: crate::redaction::Redactor,
    auth: Arc<crate::credential_http::AuthResponses>,
}
struct Calls {
    accepting: bool,
    active: usize,
    lease: Option<Arc<crate::credentials::Lease>>,
}
enum CredentialEvent {
    Rejected(Arc<crate::credentials::Lease>),
    Expired,
}
struct CallGuard {
    connection: Arc<Connection>,
    lease: Option<Arc<crate::credentials::Lease>>,
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        let mut calls = self.connection.calls.lock().unwrap();
        calls.active -= 1;
        if calls.active == 0 {
            self.connection.drained.notify_one();
        }
    }
}
impl Connection {
    fn begin(self: &Arc<Self>) -> Result<Option<CallGuard>, String> {
        let mut calls = self.calls.lock().unwrap();
        if !calls.accepting {
            return Ok(None);
        }
        if calls.lease.as_ref().is_some_and(|l| l.expired()) {
            calls.accepting = false;
            let _ = self.events.send(CredentialEvent::Expired);
            return Err("the server's credentials expired".into());
        }
        calls.active += 1;
        Ok(Some(CallGuard {
            connection: self.clone(),
            lease: calls.lease.clone(),
        }))
    }
    fn stop(&self) {
        self.calls.lock().unwrap().accepting = false;
    }
    async fn drain(&self, duration: Duration) {
        let drained = async {
            loop {
                let notified = self.drained.notified();
                if self.calls.lock().unwrap().active == 0 {
                    break;
                }
                notified.await;
            }
        };
        if timeout(duration, drained).await.is_err() {
            self.interrupted.cancel();
            // Let request futures send notifications before closing the transport.
            let _ = timeout(Duration::from_secs(1), async {
                while self.calls.lock().unwrap().active != 0 {
                    self.drained.notified().await;
                }
            })
            .await;
        }
    }
}

struct ServerHandle {
    description: Option<String>,
    status: Arc<watch::Sender<Status>>,
    /// Asks the server's task to make an attempt for a request, once the server has failed
    retry: mpsc::UnboundedSender<AttemptSender>,
}

pub struct Registry {
    access: crate::access::AccessPolicies,
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
    pub fn start(config: Arc<Config>, call_timeout: Duration, retry_delays: RetryDelays) -> Self {
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
                let (events, credential_events) = mpsc::unbounded_channel();
                let supervisor = Supervisor {
                    config: config.clone(),
                    binding: server.credentials.clone(),
                    events,
                    rejected: VecDeque::new(),
                    rejected_id: None,
                    next_lease: None,
                    denied: false,
                    call_timeout,
                    id: id.clone(),
                    transport: server.transport.clone(),
                    status: status.clone(),
                    retry_delays,
                    background_permits: background_permits.clone(),
                };
                let first_attempt = first_attempt.map(|(permit, result, _)| (permit, result));
                tokio::spawn(supervisor.run(first_attempt, retries, credential_events));
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
            access: crate::access::AccessPolicies::new(config),
            servers,
            #[cfg(test)]
            background_permits,
            call_timeout,
        }
    }

    pub async fn allowed(
        &self,
        ids: Option<&BTreeSet<String>>,
        ct: &CancellationToken,
    ) -> BTreeSet<String> {
        self.access.allowed(ids, ct).await
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
                    Status::Attempting(_) | Status::Rotating(_) | Status::Pending => {
                        ServerState::Connecting
                    }
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
        let requested = server_ids.iter().map(|id| (*id).to_owned()).collect();
        let allowed = self.allowed(Some(&requested), ct).await;
        let allowed = &allowed;
        let connections = join_all(server_ids.iter().map(|id| async move {
            if !allowed.contains(*id) {
                return Err(format!("unknown server `{id}`"));
            }
            self.connection(id, ct).await
        }))
        .await;
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
        let requested = BTreeSet::from([server_id.to_owned()]);
        if !self.allowed(Some(&requested), ct).await.contains(server_id) {
            return Err(format!("unknown server `{server_id}`"));
        }
        let (connection, guard) = loop {
            let connection = self.connection(server_id, ct).await?;
            if let Some(guard) = connection.begin()? {
                break (connection, guard);
            }
            tokio::task::yield_now().await;
        };
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
            error => connection.redactor.text(&redact_urls(&format!(
                "calling `{tool_name}` on `{server_id}` failed: {error}"
            ))),
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
        let response_future = request.await_response();
        tokio::pin!(response_future);
        let response = loop {
            tokio::select! {
                biased;
                response = &mut response_future => break response,
                () = ct.cancelled() => {
                    let _ = connection.peer.notify_cancelled(CancelledNotificationParam::new(
                        Some(request_id), Some("cancelled by the caller".into()),
                    )).await;
                    return Err(cancelled());
                }
                () = connection.interrupted.cancelled(), if !connection.auth.contains(&request_id.to_string()) => {
                    // A 401 may have arrived after select evaluated its guards. Its
                    // own call must return that response, not a rotation error.
                    if connection.auth.contains(&request_id.to_string()) { continue; }
                    let _ = connection.peer.notify_cancelled(CancelledNotificationParam::new(
                        Some(request_id), Some("credentials replaced".into()),
                    )).await;
                    return Err(format!("calling `{tool_name}` on `{server_id}` was interrupted while the server's credentials were replaced; the downstream outcome is unknown"));
                }
            }
        };
        connection.auth.remove(&request_id.to_string());
        if let Some(lease) = &guard.lease {
            let rejected = match &response {
                Err(error) => crate::credentials::service_auth(error),
                Ok(ServerResult::CallToolResult(result)) => lease.rejects(tool_name, result),
                _ => false,
            };
            if rejected {
                let _ = connection
                    .events
                    .send(CredentialEvent::Rejected(lease.clone()));
            }
        }
        let response = response.map_err(failed)?;
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
                Status::Attempting(attempt) | Status::Rotating(attempt) => {
                    (Plan::Wait(attempt.clone()), false)
                }
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
        let result = tokio::select! {
            result = attempt.result() => result.map_err(failed),
            () = ct.cancelled() => {
                Err(format!("cancelled while waiting for `{server_id}` to connect"))
            }
        };
        let requested = BTreeSet::from([server_id.to_owned()]);
        if !ct.is_cancelled() && !self.allowed(Some(&requested), ct).await.contains(server_id) {
            return Err(format!("unknown server `{server_id}`"));
        }
        result
    }
}

/// A background permit, and the result sender for the attempt it was taken for
type FirstAttempt = (OwnedSemaphorePermit, AttemptSender);

/// Keeps a server connected, and its status up to date, for as long as the gateway runs
struct Supervisor {
    config: Arc<Config>,
    binding: Option<crate::credentials::Binding>,
    events: mpsc::UnboundedSender<CredentialEvent>,
    rejected: VecDeque<[u8; 32]>,
    rejected_id: Option<String>,
    next_lease: Option<crate::credentials::Lease>,
    denied: bool,
    call_timeout: Duration,
    id: ServerId,
    transport: Transport,
    status: Arc<watch::Sender<Status>>,
    retry_delays: RetryDelays,
    background_permits: Arc<Semaphore>,
}

impl Supervisor {
    async fn run(
        mut self,
        mut first_attempt: Option<FirstAttempt>,
        mut retries: mpsc::UnboundedReceiver<AttemptSender>,
        mut events: mpsc::UnboundedReceiver<CredentialEvent>,
    ) {
        let mut connected = self.connect_with_retries(first_attempt.take()).await;
        loop {
            let (mut client, connection) = match connected.take() {
                Some(connected) => connected,
                None => {
                    let Some(result) = retries.recv().await else {
                        return;
                    };
                    match self.connect().await {
                        Ok((client, connection)) => {
                            self.ready(result, connection.clone());
                            (client, connection)
                        }
                        Err(error) => {
                            let error: Arc<str> = error.into();
                            result.send_replace(Some(Err(error.clone())));
                            self.status.send_replace(Status::Failed(error));
                            continue;
                        }
                    }
                }
            };
            let change = self.maintain(&client, &connection, &mut events).await;
            connection.stop();
            match change {
                Change::Closed => {
                    self.status.send_replace(Status::Pending);
                    let _ = client.close().await;
                    sleep(RECONNECT_DELAY).await;
                    connected = self.connect_with_retries(None).await;
                }
                Change::Revoke(reason) => {
                    self.status.send_replace(Status::Failed(reason.into()));
                    connection.drain(self.drain_time()).await;
                    let _ = client.close().await;
                }
                Change::Rotate(lease) => {
                    let (result, attempt) = Attempt::new();
                    self.status.send_replace(Status::Rotating(attempt));
                    connection.drain(self.drain_time()).await;
                    let _ = client.close().await;
                    self.next_lease = lease.filter(|lease| !lease.expired());
                    match self.connect().await {
                        Ok((client, next)) => {
                            self.ready(result, next.clone());
                            connected = Some((client, next));
                        }
                        Err(error) => {
                            let denied = self.denied;
                            let error: Arc<str> = error.into();
                            result.send_replace(Some(Err(error.clone())));
                            if denied {
                                self.status.send_replace(Status::Failed(error));
                            } else {
                                self.status.send_replace(Status::Pending);
                                sleep(self.retry_delays[0]).await;
                                connected = self.connect_with_retries(None).await;
                            }
                        }
                    }
                }
            }
        }
    }

    fn drain_time(&self) -> Duration {
        self.binding.as_ref().map_or(Duration::ZERO, |b| {
            self.config.credential_providers[&b.provider]
                .drain
                .min(self.call_timeout)
        })
    }
    fn reject(&mut self, lease: &crate::credentials::Lease) {
        if !self.rejected.contains(&lease.fingerprint) {
            if self.rejected.len() == 16 {
                self.rejected.pop_front();
            }
            self.rejected.push_back(lease.fingerprint);
        }
        self.rejected_id = Some(lease.id.clone());
    }

    async fn maintain(
        &mut self,
        client: &Client,
        connection: &Arc<Connection>,
        events: &mut mpsc::UnboundedReceiver<CredentialEvent>,
    ) -> Change {
        use crate::credentials::{Outcome, RefreshFailure};
        let mut lease = connection.calls.lock().unwrap().lease.clone();
        let mut refresh_at = lease.as_ref().and_then(|l| l.refresh);
        let mut refresh: Option<futures::future::BoxFuture<'_, Outcome>> = None;
        let mut failures = 0usize;
        loop {
            if lease.as_ref().is_some_and(|l| l.expired()) {
                return Change::Revoke("its credentials expired");
            }
            // A failed HTTP exchange may close the transport too. Attribute its
            // rejection before treating closure as an ordinary reconnect.
            while let Ok(event) = events.try_recv() {
                if let CredentialEvent::Rejected(rejected) = event
                    && lease
                        .as_ref()
                        .is_some_and(|l| l.fingerprint == rejected.fingerprint)
                {
                    self.reject(lease.as_ref().expect("matched the current lease"));
                    return Change::Rotate(None);
                }
            }
            if client.is_transport_closed() {
                return Change::Closed;
            }
            if refresh.is_none() && refresh_at.is_some_and(|t| chrono::Utc::now() >= t) {
                let config = self.config.clone();
                let binding = self.binding.clone().expect("lease has a provider");
                let id = self.id.clone();
                refresh = Some(Box::pin(async move {
                    config.credential_providers[&binding.provider]
                        .request(&id, &binding, None, &CancellationToken::new())
                        .await
                }));
            }
            tokio::select! {
                biased;
                event=events.recv() => match event {
                    Some(CredentialEvent::Expired) if lease.as_ref().is_some_and(|l|l.expired()) => return Change::Revoke("its credentials expired"),
                    Some(CredentialEvent::Rejected(rejected)) if lease.as_ref().is_some_and(|l|l.fingerprint==rejected.fingerprint) => {
                        self.reject(lease.as_ref().expect("matched the current lease"));
                        return Change::Rotate(None);
                    }
                    _ => {}
                },
                outcome=async { match &mut refresh { Some(f) => f.await, None => std::future::pending().await } } => {
                    refresh=None;
                    match outcome {
                        Outcome::Denied => return Change::Revoke("its credentials were denied"),
                        Outcome::Transient => {
                            let provider=&self.config.credential_providers[&self.binding.as_ref().unwrap().provider];
                            if provider.refresh_failure==RefreshFailure::Revoke { return Change::Revoke("its credential refresh failed"); }
                            refresh_at=Some(chrono::Utc::now()+chrono::Duration::from_std(self.retry_delays[failures.min(1)].max(Duration::from_millis(10))).unwrap());
                            failures+=1;
                        }
                        Outcome::Lease(next) => {
                            if next.expired() || self.rejected.contains(&next.fingerprint) { return Change::Revoke("the provider has no usable credential"); }
                            if lease.as_ref().is_some_and(|l|l.fingerprint==next.fingerprint) {
                                // Same material, new expiry and detectors. In-flight calls keep
                                // the snapshot they captured when dispatched.
                                refresh_at=next.refresh.map(|t|t.max(chrono::Utc::now()+chrono::Duration::milliseconds(10)));
                                lease=Some(Arc::new(next));
                                connection.calls.lock().unwrap().lease=lease.clone();
                                failures=0;
                            } else { return Change::Rotate(Some(next)); }
                        }
                    }
                },
                ()=sleep(Duration::from_millis(50)) => {}
            }
        }
    }
    /// Makes up to three background attempts, leaving the server `Ready` or `Failed`. Uses
    /// `first_attempt` for the first, if it's already in the server's status.
    async fn connect_with_retries(
        &mut self,
        mut first_attempt: Option<FirstAttempt>,
    ) -> Option<(Client, Arc<Connection>)> {
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
                    self.ready(result, connection.clone());
                    return Some((client, connection));
                }
                Err(error) => error,
            };

            let id = &self.id;
            let Some(retry_delay) = retry_delay.filter(|_| !self.denied) else {
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

    async fn connect(&mut self) -> Result<(Client, Arc<Connection>), String> {
        use crate::credentials::Outcome;
        self.denied = false;
        let lease = if let Some(binding) = &self.binding {
            let lease = match self.next_lease.take() {
                Some(lease) if !lease.expired() => lease,
                _ => match self.config.credential_providers[&binding.provider]
                    .request(
                        &self.id,
                        binding,
                        self.rejected_id.as_deref(),
                        &CancellationToken::new(),
                    )
                    .await
                {
                    Outcome::Lease(lease) => lease,
                    Outcome::Denied => {
                        self.denied = true;
                        return Err("its credentials were denied".into());
                    }
                    Outcome::Transient => {
                        return Err("its credentials are temporarily unavailable".into());
                    }
                },
            };
            if self.rejected.contains(&lease.fingerprint) {
                return Err("the provider has no other credential".into());
            }
            if lease.expired() {
                return Err("its credentials expired".into());
            }
            self.rejected_id = None;
            Some(Arc::new(lease))
        } else {
            None
        };
        let redactor = lease
            .as_ref()
            .map_or_else(crate::redaction::Redactor::default, |l| {
                crate::redaction::Redactor::new(&l.values)
            });
        let transport = match (&lease, &self.binding) {
            (Some(lease), Some(binding)) => lease
                .transport(&self.transport, binding)
                .map_err(|e| redactor.text(&format!("{e:#}")))?,
            _ => self.transport.clone(),
        };
        let events = self.events.clone();
        let attempt_lease = lease.clone();
        let auth = Arc::new(match attempt_lease {
            Some(lease) => crate::credential_http::AuthResponses::new(move || {
                let _ = events.send(CredentialEvent::Rejected(lease.clone()));
            }),
            None => crate::credential_http::AuthResponses::default(),
        });
        let (mut client, tools) =
            match connect(&self.id, &transport, redactor.clone(), auth.clone()).await {
                Ok(connected) => connected,
                Err(error) => {
                    if let Some(lease) = &lease
                        && (auth.during_setup() || setup_auth(&error))
                    {
                        self.reject(lease);
                    }
                    return Err(redactor.text(&redact_urls(&format!("{error:#}"))));
                }
            };
        if lease.as_ref().is_some_and(|l| l.expired()) {
            let _ = client.close().await;
            return Err("its credentials expired while connecting".into());
        }
        let connection = Arc::new(Connection {
            peer: client.peer().clone(),
            tools: tools.into(),
            calls: Mutex::new(Calls {
                accepting: true,
                active: 0,
                lease,
            }),
            drained: Notify::new(),
            interrupted: CancellationToken::new(),
            events: self.events.clone(),
            redactor,
            auth,
        });
        Ok((client, connection))
    }

    fn ready(&self, result: AttemptSender, connection: Arc<Connection>) {
        result.send_replace(Some(Ok(connection.clone())));
        self.status.send_replace(Status::Ready(connection));
    }
}

enum Change {
    Closed,
    Revoke(&'static str),
    Rotate(Option<crate::credentials::Lease>),
}
#[derive(Debug)]
struct SetupFailure {
    text: String,
    auth: bool,
}
impl std::fmt::Display for SetupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}
impl std::error::Error for SetupFailure {}
fn setup_auth(error: &anyhow::Error) -> bool {
    fn initialize(error: &rmcp::service::ClientInitializeError) -> bool {
        use rmcp::service::ClientInitializeError;
        match error {
            ClientInitializeError::JsonRpcError(data) => data
                .data
                .as_ref()
                .is_some_and(crate::credentials::status_401),
            ClientInitializeError::LegacyFallbackFailed { discover, fallback } => {
                initialize(discover) || initialize(fallback)
            }
            _ => false,
        }
    }
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<SetupFailure>() {
            return error.auth;
        }
        if let Some(error) = cause.downcast_ref::<ServiceError>() {
            return crate::credentials::service_auth(error);
        }
        cause
            .downcast_ref::<rmcp::service::ClientInitializeError>()
            .is_some_and(initialize)
    })
}
type Client = RunningService<RoleClient, ClientConfig>;

async fn connect(
    id: &str,
    transport: &Transport,
    redactor: crate::redaction::Redactor,
    auth: Arc<crate::credential_http::AuthResponses>,
) -> Result<(Client, Vec<Tool>)> {
    match transport {
        Transport::Stdio {
            command,
            args,
            env,
            environment,
        } => {
            let mut cmd = Command::new(command);
            cmd.args(args).kill_on_drop(true);
            environment.apply(&mut cmd, env);
            let spawned = {
                let _spawning = spawn::lock();
                TokioChildProcess::builder(cmd)
                    .stderr(Stdio::piped())
                    .spawn()
            };
            let (child, stderr) =
                spawned.with_context(|| format!("failed to start `{command}`"))?;
            let stderr =
                stderr.map(|stderr| tokio::spawn(forward_stderr(id.to_owned(), stderr, redactor)));

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
                        tail => Err(anyhow::Error::new(SetupFailure {
                            auth: setup_auth(&error),
                            text: format!("{error:#}\nstderr:\n{tail}"),
                        })),
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
            let config = {
                let mut config = StreamableHttpClientTransportConfig::with_uri(url.as_str())
                    .custom_headers(custom_headers)
                    .max_concurrent_requests(HTTP_CONCURRENT_REQUESTS);

                config.channel_buffer_capacity = HTTP_CONCURRENT_REQUESTS;
                config
            };
            if auth.enabled() {
                handshake(StreamableHttpClientTransport::with_client(
                    crate::credential_http::HttpClient::new(auth)?,
                    config,
                ))
                .await
            } else {
                handshake(StreamableHttpClientTransport::from_config(config)).await
            }
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
        let mut client = client_config()
            .serve(transport)
            .await
            .context("failed to initialize")?;
        let tools = match client.list_all_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                let _ = client.close().await;
                return Err(anyhow::Error::new(error).context("failed to list tools"));
            }
        };
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
async fn forward_stderr(
    id: ServerId,
    stderr: ChildStderr,
    redactor: crate::redaction::Redactor,
) -> String {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::new();
    let mut tail = String::new();
    let mut pending = Vec::new();
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
            pending.extend_from_slice(&line);
            let safe = redactor.take(&mut pending, false);
            if !safe.is_empty() {
                forward_line(&id, &safe, &mut tail);
            }
            line.clear();
        }
    }
    pending.extend_from_slice(&line);
    let safe = redactor.take(&mut pending, true);
    if !safe.is_empty() {
        forward_line(&id, &safe, &mut tail);
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
            access: crate::access::AccessPolicies::new(Arc::new(Config {
                access_policies: BTreeMap::new(),
                credential_providers: BTreeMap::new(),
                servers: BTreeMap::from([(
                    "s".into(),
                    crate::config::Server {
                        access: None,
                        credentials: None,
                        description: None,
                        transport: Transport::Http {
                            url: "http://localhost".into(),
                            headers: BTreeMap::new(),
                        },
                    },
                )]),
            })),
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
            access: None,
            credentials: None,
            transport: Transport::Stdio {
                command: "sh".into(),
                args: vec!["-c".into(), "echo boom >&2; exit 1".into()],
                env: BTreeMap::new(),
                environment: Default::default(),
            },
        };
        let config = Config {
            access_policies: BTreeMap::new(),
            credential_providers: BTreeMap::new(),
            servers: BTreeMap::from([("s".to_owned(), server)]),
        };
        let delays = [Duration::from_millis(10); 2];
        let registry = Registry::start(Arc::new(config), Duration::from_secs(1), delays);
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
