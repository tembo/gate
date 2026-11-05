use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;

const MAX_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHook {
    http: Option<Http>,
    command: Option<String>,
    args: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    timeout_seconds: Option<u64>,
    #[serde(default)]
    stderr: HookStderr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Http {
    url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum HookStderr {
    #[default]
    Drop,
    Forward,
}

#[derive(Debug)]
pub struct Hook {
    transport: HookTransport,
    timeout: Duration,
}

#[derive(Debug)]
enum HookTransport {
    Http {
        client: reqwest::Client,
        url: reqwest::Url,
        headers: reqwest::header::HeaderMap,
    },
    Command {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        stderr: HookStderr,
    },
}

impl RawHook {
    pub fn resolve(
        self,
        env: &impl Fn(&str) -> Option<String>,
        hook_env: &mut BTreeSet<String>,
    ) -> Result<Hook> {
        self.resolve_with_timeout(env, hook_env, 5)
    }

    pub fn resolve_with_timeout(
        self,
        env: &impl Fn(&str) -> Option<String>,
        hook_env: &mut BTreeSet<String>,
        default_timeout: u64,
    ) -> Result<Hook> {
        let names = RefCell::new(BTreeSet::new());
        let expand = |text: &str| {
            crate::config::expand(
                text,
                |name| {
                    names.borrow_mut().insert(name.to_owned());
                    env(name)
                },
                &BTreeSet::new(),
            )
        };
        let timeout = self.timeout_seconds.unwrap_or(default_timeout);
        if timeout == 0 || timeout > 3600 {
            bail!("timeoutSeconds must be between 1 and 3600");
        }
        let transport = match (self.http, self.command) {
            (Some(http), None) => {
                if self.args.is_some() || self.env.is_some() {
                    bail!("HTTP hooks cannot set args or env");
                }
                let url = reqwest::Url::parse(&expand(&http.url)?).context("invalid hook URL")?;
                if !matches!(url.scheme(), "http" | "https") {
                    bail!("hook URL must use http or https");
                }
                let mut headers = reqwest::header::HeaderMap::new();
                for (name, value) in http.headers {
                    let name = reqwest::header::HeaderName::try_from(name.as_str())
                        .context("invalid hook header name")?;
                    let value = reqwest::header::HeaderValue::try_from(expand(&value)?)
                        .context("invalid hook header value")?;
                    headers.insert(name, value);
                }
                let client = reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?;
                HookTransport::Http {
                    client,
                    url,
                    headers,
                }
            }
            (None, Some(command)) => {
                let command = expand(&command)?;
                if command.is_empty() {
                    bail!("hook command must not be empty");
                }
                let args = self
                    .args
                    .unwrap_or_default()
                    .iter()
                    .map(|arg| expand(arg))
                    .collect::<Result<_>>()?;
                let env = self
                    .env
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(name, value)| Ok((name, expand(&value)?)))
                    .collect::<Result<_>>()?;
                HookTransport::Command {
                    command,
                    args,
                    env,
                    stderr: self.stderr,
                }
            }
            _ => bail!("a hook needs exactly one of http or command"),
        };
        hook_env.extend(names.into_inner());
        Ok(Hook {
            transport,
            timeout: Duration::from_secs(timeout),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HookError {
    HttpStatus(u16),
    Failure(&'static str),
}
impl From<&'static str> for HookError {
    fn from(message: &'static str) -> Self {
        Self::Failure(message)
    }
}
impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HttpStatus(_) => f.write_str("HTTP request was rejected"),
            Self::Failure(message) => f.write_str(message),
        }
    }
}

impl Hook {
    pub async fn request(
        &self,
        name: &str,
        request: &Value,
        ct: &CancellationToken,
    ) -> Result<Vec<u8>, HookError> {
        if ct.is_cancelled() {
            return Err(HookError::Failure("cancelled"));
        }
        let operation = async {
            match &self.transport {
                HookTransport::Http {
                    client,
                    url,
                    headers,
                } => {
                    let mut response = client
                        .post(url.clone())
                        .headers(headers.clone())
                        .json(request)
                        .send()
                        .await
                        .map_err(|_| "HTTP request failed")?;
                    if !response.status().is_success() {
                        return Err(HookError::HttpStatus(response.status().as_u16()));
                    }
                    let mut bytes = Vec::new();
                    while let Some(chunk) =
                        response.chunk().await.map_err(|_| "HTTP body failed")?
                    {
                        if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
                            return Err(HookError::Failure("response exceeds 64 KiB"));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    Ok(bytes)
                }
                HookTransport::Command {
                    command,
                    args,
                    env,
                    stderr,
                } => {
                    let mut command = Command::new(command);
                    command
                        .args(args)
                        .envs(env)
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(match stderr {
                            HookStderr::Drop => Stdio::null(),
                            HookStderr::Forward => Stdio::piped(),
                        })
                        .kill_on_drop(true);
                    let mut child = {
                        let _lock = crate::spawn::lock();
                        command.spawn().map_err(|_| "could not start command")?
                    };
                    let mut stdin = child.stdin.take().expect("piped stdin");
                    let stdout = child.stdout.take().expect("piped stdout");
                    let stderr = child.stderr.take();
                    let body =
                        serde_json::to_vec(request).map_err(|_| "could not encode request")?;
                    let exchange = async {
                        let write = async {
                            if let Err(error) = stdin.write_all(&body).await
                                && error.kind() != std::io::ErrorKind::BrokenPipe
                            {
                                return Err(HookError::Failure("could not write request"));
                            }
                            if let Err(error) = stdin.shutdown().await
                                && error.kind() != std::io::ErrorKind::BrokenPipe
                            {
                                return Err(HookError::Failure("could not close stdin"));
                            }
                            drop(stdin);
                            Ok::<_, HookError>(())
                        };
                        let read = async {
                            let mut bytes = Vec::new();
                            stdout
                                .take((MAX_RESPONSE_BYTES + 1) as u64)
                                .read_to_end(&mut bytes)
                                .await
                                .map_err(|_| "could not read response")?;
                            if bytes.len() > MAX_RESPONSE_BYTES {
                                return Err(HookError::Failure("response exceeds 64 KiB"));
                            }
                            Ok(bytes)
                        };
                        let errors = async {
                            if let Some(mut stderr) = stderr {
                                let mut buffer = [0; 4096];
                                loop {
                                    let count = stderr
                                        .read(&mut buffer)
                                        .await
                                        .map_err(|_| "could not read stderr")?;
                                    if count == 0 {
                                        break;
                                    }
                                    eprintln!(
                                        "[hook:{name}] {}",
                                        String::from_utf8_lossy(&buffer[..count])
                                    );
                                }
                            }
                            Ok::<_, HookError>(())
                        };
                        let (_, bytes, ()) = tokio::try_join!(write, read, errors)?;
                        Ok::<_, HookError>(bytes)
                    };
                    let outcome = tokio::select! {
                        biased;
                        () = ct.cancelled() => Err(HookError::Failure("cancelled")),
                        result = tokio::time::timeout(self.timeout, async {
                            let bytes = exchange.await?;
                            let status = child.wait().await.map_err(|_| "could not wait for command")?;
                            if !status.success() { return Err(HookError::Failure("command failed")); }
                            Ok(bytes)
                        }) => result.unwrap_or(Err(HookError::Failure("timed out"))),
                    };
                    if outcome.is_err() {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                    outcome
                }
            }
        };
        // Command cleanup owns its deadline; dropping HTTP requests cancels their I/O.
        if matches!(self.transport, HookTransport::Command { .. }) {
            operation.await
        } else {
            tokio::select! {
                biased;
                () = ct.cancelled() => Err(HookError::Failure("cancelled")),
                result = tokio::time::timeout(self.timeout, operation) => result.unwrap_or(Err(HookError::Failure("timed out"))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hook(value: Value) -> Hook {
        serde_json::from_value::<RawHook>(value)
            .unwrap()
            .resolve(&|_| None, &mut BTreeSet::new())
            .unwrap()
    }

    #[tokio::test]
    async fn helpers_may_ignore_large_requests_but_must_exit_successfully() {
        for status in [0, 1] {
            let script = format!("printf '{{\"allowed\":[]}}'; exit {status}");
            let hook = hook(json!({"command":"/bin/sh", "args":["-c",script]}));
            let result = hook
                .request(
                    "test",
                    &json!({"request":"x".repeat(1024*1024)}),
                    &CancellationToken::new(),
                )
                .await;
            if status == 0 {
                assert_eq!(result.unwrap(), br#"{"allowed":[]}"#);
            } else {
                assert_eq!(result.unwrap_err(), HookError::Failure("command failed"));
            }
        }
    }

    #[tokio::test]
    async fn command_transport_bounds_output_and_requires_success() {
        for (script, expected) in [
            (
                "cat >/dev/null; printf '{\"allowed\":[]}'; exit 1",
                "command failed",
            ),
            (
                "cat >/dev/null; head -c 65537 /dev/zero",
                "response exceeds 64 KiB",
            ),
        ] {
            let hook = hook(json!({"command":"/bin/sh", "args":["-c",script]}));
            assert_eq!(
                hook.request("test", &json!({}), &CancellationToken::new())
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
        let hook = hook(
            json!({"command":"/bin/sh", "args":["-c", "cat >/dev/null; head -c 65536 /dev/zero"]}),
        );
        assert_eq!(
            hook.request("test", &json!({}), &CancellationToken::new())
                .await
                .unwrap()
                .len(),
            65536
        );
    }

    #[tokio::test]
    async fn command_timeout_and_cancellation_stop_the_process() {
        for cancel in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let pid = dir.path().join("pid");
            let hook = hook(
                json!({"command":"/bin/sh", "args":["-c", "echo $$ > \"$1\"; exec sleep 30", "test", pid], "timeoutSeconds":1}),
            );
            let ct = CancellationToken::new();
            let request = hook.request("test", &Value::Null, &ct);
            let stop = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !pid.exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                if cancel {
                    ct.cancel();
                }
            };
            let (outcome, ()) = tokio::join!(request, stop);
            assert_eq!(
                outcome.unwrap_err().to_string(),
                if cancel { "cancelled" } else { "timed out" }
            );
            let pid = std::fs::read_to_string(pid).unwrap();
            assert!(
                !std::process::Command::new("kill")
                    .args(["-0", pid.trim()])
                    .stderr(Stdio::null())
                    .status()
                    .unwrap()
                    .success()
            );
        }
    }

    #[tokio::test]
    async fn http_transport_times_out_and_cancels() {
        let (sent, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let router = axum::Router::new().route(
            "/",
            axum::routing::post(move || {
                let sent = sent.clone();
                async move {
                    let _ = sent.send(());
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    "{}"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let hook = hook(json!({"http":{"url":url},"timeoutSeconds":1}));
        for cancel in [false, true] {
            let ct = CancellationToken::new();
            let request = hook.request("test", &Value::Null, &ct);
            let stop = async {
                tokio::time::timeout(Duration::from_secs(5), requests.recv())
                    .await
                    .unwrap()
                    .unwrap();
                if cancel {
                    ct.cancel();
                }
            };
            let (outcome, ()) = tokio::join!(request, stop);
            assert_eq!(
                outcome.unwrap_err().to_string(),
                if cancel { "cancelled" } else { "timed out" }
            );
        }
        server.abort();
    }
}
