//! rmcp HTTP adapter that preserves 401 attribution before decoding the body.
use futures::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use rmcp::{
    model::{ClientJsonRpcMessage, ClientRequest, ErrorData, ServerJsonRpcMessage},
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};
use sse_stream::{Error as SseError, Sse};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub struct AuthResponses {
    rejected: AtomicBool,
    requests: Mutex<HashSet<String>>,
    on_rejected: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl AuthResponses {
    pub fn new(on_rejected: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            rejected: AtomicBool::new(false),
            requests: Mutex::new(HashSet::new()),
            on_rejected: Some(Arc::new(on_rejected)),
        }
    }
    pub fn enabled(&self) -> bool {
        self.on_rejected.is_some()
    }
    pub fn during_setup(&self) -> bool {
        self.rejected.load(Ordering::Acquire)
    }
    pub fn contains(&self, id: &str) -> bool {
        self.requests.lock().unwrap().contains(id)
    }
    pub fn remove(&self, id: &str) {
        self.requests.lock().unwrap().remove(id);
    }
    fn reject(&self, message: Option<&ClientJsonRpcMessage>) {
        if self.on_rejected.is_none() {
            return;
        }
        if let Some(ClientJsonRpcMessage::Request(request)) = message {
            self.requests.lock().unwrap().insert(request.id.to_string());
        }
        if !self.rejected.swap(true, Ordering::AcqRel)
            && let Some(notify) = &self.on_rejected
        {
            notify();
        }
    }
}
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    pub auth: Arc<AuthResponses>,
}
impl HttpClient {
    pub fn new(auth: Arc<AuthResponses>) -> Result<Self, reqwest::Error> {
        Ok(Self {
            inner: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            auth,
        })
    }
    fn observe_stream<T>(&self, result: &Result<T, StreamableHttpError<reqwest::Error>>) {
        if matches!(result, Err(StreamableHttpError::AuthRequired(_)))
            || matches!(result, Err(StreamableHttpError::Client(error)) if error.status() == Some(http::StatusCode::UNAUTHORIZED))
        {
            self.auth.reject(None);
        }
    }
}
impl StreamableHttpClient for HttpClient {
    type Error = reqwest::Error;
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        let result = self
            .inner
            .get_stream(uri, session_id, last_event_id, auth, headers)
            .await;
        self.observe_stream(&result);
        result
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        let result = self
            .inner
            .get_stream_with_max_sse_event_size(uri, session_id, last_event_id, auth, headers, max)
            .await;
        self.observe_stream(&result);
        result
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        self.inner.delete_session(uri, session, auth, headers).await
    }
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session,
            auth,
            headers,
            16 * 1024 * 1024,
        )
        .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let mut request = self
            .inner
            .post(uri.as_ref())
            .header("accept", "text/event-stream, application/json");
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        for (name, value) in headers {
            if matches!(name.as_str(), "accept" | "mcp-session-id" | "last-event-id") {
                return Err(StreamableHttpError::ReservedHeaderConflict(
                    name.to_string(),
                ));
            }
            request = request.header(name, value);
        }
        let attached = session.is_some();
        if let Some(session) = session {
            request = request.header("mcp-session-id", session.as_ref());
        }
        let mut response = request.json(&message).send().await?;
        let status = response.status();
        if status.as_u16() == 401 {
            self.auth.reject(Some(&message));
        }
        if matches!(status.as_u16(), 202 | 204) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status.as_u16() == 404 && attached {
            return Err(StreamableHttpError::SessionExpired);
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|s| s.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let session = response
            .headers()
            .get("mcp-session-id")
            .and_then(|s| s.to_str().ok())
            .map(str::to_owned);
        if status.is_success() && content_type.starts_with("text/event-stream") {
            let mut limiter = EventLimit::new(max);
            let mut failed = false;
            let stream = response.bytes_stream().scan((), move |_, chunk| {
                let item = if failed {
                    None
                } else {
                    Some(match chunk {
                        Ok(bytes) if limiter.observe(&bytes).is_ok() => Ok(bytes),
                        Ok(_) => {
                            failed = true;
                            Err(std::io::Error::other("SSE event exceeds size limit"))
                        }
                        Err(_) => {
                            failed = true;
                            Err(std::io::Error::other("HTTP stream failed"))
                        }
                    })
                };
                std::future::ready(item)
            });
            return Ok(StreamableHttpPostResponse::Sse(
                sse_stream::SseStream::from_bytes_stream(stream).boxed(),
                session,
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > max.saturating_sub(body.len()) {
                return Err(StreamableHttpError::UnexpectedServerResponse(
                    "HTTP body exceeds size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let parsed = serde_json::from_slice::<ServerJsonRpcMessage>(&body);
        // Preserve rmcp's legacy discovery fallback without exposing raw HTTP bodies.
        if !attached
            && status.is_client_error()
            && !matches!(status.as_u16(), 401 | 403)
            && let ClientJsonRpcMessage::Request(request) = &message
            && matches!(request.request, ClientRequest::DiscoverRequest(_))
        {
            let error = match parsed {
                Ok(ServerJsonRpcMessage::Error(error)) => error.error,
                _ => ErrorData::invalid_request("server rejected discovery", None),
            };
            return Ok(StreamableHttpPostResponse::Json(
                ServerJsonRpcMessage::error(error, Some(request.id.clone())),
                None,
            ));
        }
        if content_type.starts_with("application/json") {
            match parsed {
                Ok(message)
                    if status.is_success() || matches!(message, ServerJsonRpcMessage::Error(_)) =>
                {
                    return Ok(StreamableHttpPostResponse::Json(message, session));
                }
                _ => {}
            }
        }
        if status.is_success() && !matches!(message, ClientJsonRpcMessage::Request(_)) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        Err(StreamableHttpError::UnexpectedServerResponse(
            if status.as_u16() == 401 {
                "HTTP 401"
            } else {
                "invalid HTTP response"
            }
            .into(),
        ))
    }
}

// Size accounting follows rmcp 3.5.1's client_side_sse limiter (MIT licensed).
// Counts retained event data before the SSE parser allocates an unbounded event.
struct EventLimit {
    max: usize,
    retained: usize,
    line: usize,
    comment: bool,
    cr: bool,
}
impl EventLimit {
    fn new(max: usize) -> Self {
        Self {
            max,
            retained: 0,
            line: 0,
            comment: false,
            cr: false,
        }
    }
    fn observe(&mut self, bytes: &[u8]) -> Result<(), ()> {
        for &byte in bytes {
            if self.cr {
                self.cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                if self.line == 0 {
                    self.retained = 0;
                } else if !self.comment {
                    self.retained = self.retained.saturating_add(self.line).saturating_add(1);
                }
                self.line = 0;
                self.comment = false;
                self.cr = byte == b'\r';
            } else {
                if self.line == 0 {
                    self.comment = byte == b':';
                }
                self.line = self.line.saturating_add(1);
            }
            if self.retained.saturating_add(self.line) > self.max {
                return Err(());
            }
        }
        Ok(())
    }
}
