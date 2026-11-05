//! Credential protocol and lease validation. Connection ownership stays in the registry.
use crate::{
    config::Transport,
    hooks::{Hook, RawHook},
};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use regex::Regex;
use rmcp::{ServiceError, model::CallToolResult};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Binding {
    pub provider: String,
    #[serde(default)]
    pub params: serde_json::Map<String, Value>,
    #[serde(default)]
    pub merge_env: bool,
}

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RefreshFailure {
    #[default]
    Keep,
    Revoke,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawProvider {
    #[serde(flatten)]
    hook: RawHook,
    #[serde(default)]
    refresh_failure: RefreshFailure,
    drain_seconds: Option<u64>,
}
#[derive(Debug)]
pub struct Provider {
    hook: Hook,
    pub refresh_failure: RefreshFailure,
    pub drain: Duration,
}
impl RawProvider {
    pub fn resolve(
        self,
        env: &impl Fn(&str) -> Option<String>,
        reserved: &mut BTreeSet<String>,
    ) -> Result<Provider> {
        let drain = self.drain_seconds.unwrap_or(30);
        if drain > 3600 {
            bail!("drainSeconds must be at most 3600");
        }
        Ok(Provider {
            hook: self.hook.resolve_with_timeout(env, reserved, 10)?,
            refresh_failure: self.refresh_failure,
            drain: Duration::from_secs(drain),
        })
    }
}
pub enum Outcome {
    Lease(Lease),
    Denied,
    Transient,
}
impl Provider {
    pub async fn request(
        &self,
        server: &str,
        binding: &Binding,
        rejected: Option<&str>,
        ct: &CancellationToken,
    ) -> Outcome {
        let mut request =
            json!({"version":1,"kind":"credentials","server":server,"params":binding.params});
        if let Some(id) = rejected {
            request["rejectedCredentialId"] = id.into();
        }
        match self.hook.request(&binding.provider, &request, ct).await {
            Err(crate::hooks::HookError::HttpStatus(401 | 403 | 404 | 410)) => Outcome::Denied,
            Err(_) => Outcome::Transient,
            Ok(bytes) => parse(&bytes).unwrap_or(Outcome::Transient),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "outcome", rename_all = "lowercase")]
enum Answer {
    #[serde(rename_all = "camelCase")]
    Lease {
        credential_id: String,
        values: BTreeMap<String, String>,
        refresh_after: Option<DateTime<Utc>>,
        expires_at: Option<DateTime<Utc>>,
        #[serde(default)]
        auth_failures: Vec<RawMatcher>,
    },
    Denied,
    Transient,
}
#[derive(Clone)]
pub struct Lease {
    pub id: String,
    pub values: BTreeMap<String, String>,
    pub fingerprint: [u8; 32],
    pub expires: Option<DateTime<Utc>>,
    pub refresh: Option<DateTime<Utc>>,
    matchers: Vec<Matcher>,
}
fn parse(bytes: &[u8]) -> Result<Outcome> {
    Ok(match serde_json::from_slice::<Answer>(bytes)? {
        Answer::Denied => Outcome::Denied,
        Answer::Transient => Outcome::Transient,
        Answer::Lease {
            credential_id,
            values,
            refresh_after,
            expires_at,
            auth_failures,
        } => {
            if credential_id.is_empty() || auth_failures.len() > 32 {
                bail!("invalid lease");
            }
            let matchers = auth_failures
                .into_iter()
                .map(Matcher::compile)
                .collect::<Result<_>>()?;
            let fingerprint = Sha256::digest(serde_json::to_vec(&values)?).into();
            let refresh = match (refresh_after, expires_at) {
                (Some(refresh), Some(expires)) if refresh < expires => Some(refresh),
                (_, Some(expires)) => {
                    let lifetime = (expires - Utc::now()).max(chrono::Duration::zero());
                    Some(expires - (lifetime / 10).min(chrono::Duration::seconds(60)))
                }
                (refresh, None) => refresh,
            };
            Outcome::Lease(Lease {
                id: credential_id,
                values,
                fingerprint,
                expires: expires_at,
                refresh,
                matchers,
            })
        }
    })
}
impl Lease {
    pub fn expired(&self) -> bool {
        self.expires.is_some_and(|time| Utc::now() >= time)
    }
    pub fn rejects(&self, tool: &str, result: &CallToolResult) -> bool {
        if result.is_error == Some(true)
            && result.structured_content.as_ref().is_some_and(status_401)
        {
            return true;
        }
        self.matchers.iter().any(|m| m.matches(tool, result))
    }
    pub fn transport(&self, template: &Transport, binding: &Binding) -> Result<Transport> {
        let mut transport = template.clone();
        let fields = match &mut transport {
            Transport::Stdio {
                env, environment, ..
            } => {
                if binding.merge_env {
                    for key in self.values.keys() {
                        if !crate::config::is_var_name(key) || environment.hook_env.contains(key) {
                            bail!("invalid or reserved leased environment key `{key}`");
                        }
                    }
                }
                env
            }
            Transport::Http { headers, .. } => headers,
        };
        for value in fields.values_mut() {
            *value = expand(value, Some(&self.values))?;
        }
        if binding.merge_env {
            fields.extend(self.values.clone());
        }
        Ok(transport)
    }
}
pub fn status_401(data: &Value) -> bool {
    data.get("status").and_then(Value::as_u64) == Some(401)
}
pub fn service_auth(error: &ServiceError) -> bool {
    matches!(error, ServiceError::McpError(data) if data.data.as_ref().is_some_and(status_401))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMatcher {
    is_error: Option<bool>,
    tool_name: Option<String>,
    text: Option<String>,
    json: Option<JsonMatcher>,
}
#[derive(Deserialize)]
struct JsonMatcher {
    pointer: String,
    text: String,
}
#[derive(Clone)]
struct Matcher {
    is_error: Option<bool>,
    tool_name: Option<Regex>,
    text: Option<Regex>,
    json: Option<(String, Regex)>,
}
impl Matcher {
    fn compile(raw: RawMatcher) -> Result<Self> {
        fn regex(text: String) -> Result<Regex> {
            if text.len() > 1024 {
                bail!("matcher too long");
            }
            Ok(Regex::new(&text)?)
        }
        if raw.is_error.is_none()
            && raw.tool_name.is_none()
            && raw.text.is_none()
            && raw.json.is_none()
        {
            bail!("empty matcher");
        }
        let json = raw
            .json
            .map(|m| {
                if !m.pointer.is_empty() && !m.pointer.starts_with('/') {
                    bail!("invalid JSON pointer");
                }
                Ok((m.pointer, regex(m.text)?))
            })
            .transpose()?;
        Ok(Self {
            is_error: raw.is_error,
            tool_name: raw.tool_name.map(regex).transpose()?,
            text: raw.text.map(regex).transpose()?,
            json,
        })
    }
    fn matches(&self, tool: &str, result: &CallToolResult) -> bool {
        if self
            .is_error
            .is_some_and(|v| result.is_error.unwrap_or(false) != v)
            || self.tool_name.as_ref().is_some_and(|r| !r.is_match(tool))
        {
            return false;
        }
        let text = match result.content.as_slice() {
            [content] => content.as_text().map(|t| t.text.as_str()),
            _ => None,
        };
        if self
            .text
            .as_ref()
            .is_some_and(|r| !text.is_some_and(|t| r.is_match(t)))
        {
            return false;
        }
        if let Some((pointer, regex)) = &self.json {
            return text
                .and_then(|t| serde_json::from_str::<Value>(t).ok())
                .and_then(|v| {
                    v.pointer(pointer)
                        .and_then(Value::as_str)
                        .map(|s| regex.is_match(s))
                })
                .unwrap_or(false);
        }
        true
    }
}

// Validation and replacement share the same scanner; replacements aren't scanned again.
fn expand(text: &str, values: Option<&BTreeMap<String, String>>) -> Result<String> {
    let mut rest = text;
    let mut out = String::new();
    while let Some(start) = rest.find("${credential:") {
        out.push_str(&rest[..start]);
        let suffix = &rest[start + "${credential:".len()..];
        let end = suffix
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("unterminated credential placeholder"))?;
        let name = &suffix[..end];
        if name.is_empty() {
            bail!("empty credential placeholder");
        }
        if let Some(values) = values {
            out.push_str(
                values
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("missing credential value `{name}`"))?,
            );
        }
        rest = &suffix[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}
pub fn validate_transport(transport: &Transport, binding: Option<&Binding>) -> Result<()> {
    let (fields, forbidden) = match transport {
        Transport::Stdio {
            command, args, env, ..
        } => (
            env,
            std::iter::once(command).chain(args).collect::<Vec<_>>(),
        ),
        Transport::Http { url, headers } => {
            if binding.is_some_and(|b| b.merge_env) {
                bail!("mergeEnv is only valid for stdio servers");
            }
            (headers, vec![url])
        }
    };
    if forbidden.iter().any(|v| v.contains("${credential:")) {
        bail!("credential placeholders are only allowed in env and headers");
    }
    for value in fields.values() {
        if value.contains("${credential:") && binding.is_none() {
            bail!("credential placeholder requires a provider");
        }
        expand(value, None)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn lease(value: Value) -> Lease {
        match parse(&serde_json::to_vec(&value).unwrap()).unwrap() {
            Outcome::Lease(l) => l,
            _ => panic!("expected lease"),
        }
    }
    #[test]
    fn validates_leases_and_fingerprints_sorted_values() {
        let a =
            lease(json!({"outcome":"lease","credentialId":"a","values":{"z":"last","a":"first"}}));
        let b =
            lease(json!({"outcome":"lease","credentialId":"b","values":{"a":"first","z":"last"}}));
        assert_eq!(a.fingerprint, b.fingerprint);
        for answer in [
            json!({"outcome":"lease","credentialId":"","values":{}}),
            json!({"outcome":"lease","credentialId":"a","values":{},"authFailures":[{}]}),
            json!({"outcome":"lease","credentialId":"a","values":{},"authFailures":[{"text":"["}]}),
            json!({"outcome":"lease","credentialId":"a","values":{},"authFailures":[{"text":"x".repeat(1025)}]}),
            json!({"outcome":"lease","credentialId":"a","values":{},"authFailures":vec![json!({"isError":true});33]}),
            json!({"outcome":"lease","credentialId":"a","values":{},"expiresAt":"invalid"}),
        ] {
            assert!(parse(&serde_json::to_vec(&answer).unwrap()).is_err());
        }
    }
    #[test]
    fn detectors_require_every_field_and_exactly_one_text_block() {
        use rmcp::model::ContentBlock as Content;
        let l = lease(
            json!({"outcome":"lease","credentialId":"a","values":{},"authFailures":[{"isError":true,"toolName":"^read$","json":{"pointer":"/error","text":"^unauthorized$"}}]}),
        );
        let result = CallToolResult::error(vec![Content::text(r#"{"error":"unauthorized"}"#)]);
        assert!(l.rejects("read", &result));
        assert!(!l.rejects("write", &result));
        let mut multiple = result.clone();
        multiple.content.push(Content::text("extra"));
        assert!(!l.rejects("read", &multiple));
        let mut success = result.clone();
        success.is_error = Some(false);
        assert!(!l.rejects("read", &success));
        let mut structured = success;
        structured.structured_content = Some(json!({"status":401}));
        assert!(!l.rejects("read", &structured));
        structured.is_error = Some(true);
        assert!(l.rejects("anything", &structured));
    }
    #[test]
    fn refresh_defaults_to_tenth_of_lifetime_and_is_capped() {
        for seconds in [100, 1000] {
            let expires = Utc::now() + chrono::Duration::seconds(seconds);
            let l = lease(
                json!({"outcome":"lease","credentialId":"a","values":{},"expiresAt":expires}),
            );
            let early = (expires - l.refresh.unwrap()).num_seconds();
            assert!((seconds / 10).min(60) - 1 <= early && early <= (seconds / 10).min(60));
        }
    }
}
