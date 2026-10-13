use std::{
    collections::{BTreeMap, btree_map::Entry},
    env, fmt, fs,
    path::Path,
};

use anyhow::{Context, Result, bail};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, Visitor},
};
use url::Url;

pub type ServerId = String;

#[derive(Debug)]
pub struct Config {
    pub servers: BTreeMap<ServerId, Server>,
}

#[derive(Debug, PartialEq)]
pub struct Server {
    pub description: Option<String>,
    pub transport: Transport,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>,
    },
}

impl Transport {
    pub fn kind(&self) -> Kind {
        match self {
            Transport::Stdio { .. } => Kind::Stdio,
            Transport::Http { .. } => Kind::Http,
        }
    }
}

/// A server's `type` in the config file
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Stdio,
    Http,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Kind::Stdio => "stdio",
            Kind::Http => "http",
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawConfig {
    #[serde(deserialize_with = "unique_servers")]
    mcp_servers: BTreeMap<String, RawServer>,
}

/// Deserializes `mcpServers`, rejecting duplicate names instead of keeping the last one
fn unique_servers<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, RawServer>, D::Error> {
    struct Servers;

    impl<'de> Visitor<'de> for Servers {
        type Value = BTreeMap<String, RawServer>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map of servers")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut servers = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                match servers.entry(name) {
                    Entry::Occupied(entry) => {
                        return Err(de::Error::custom(format!(
                            "duplicate server `{}`",
                            entry.key()
                        )));
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(map.next_value()?);
                    }
                }
            }
            Ok(servers)
        }
    }

    deserializer.deserialize_map(Servers)
}

#[derive(Deserialize)]
struct RawServer {
    #[serde(rename = "type")]
    kind: Option<Kind>,
    command: Option<String>,
    args: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    url: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    description: Option<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read config `{}`", path.display()))?;
        Self::parse(&text, |name| env::var(name).ok())
            .with_context(|| format!("invalid config `{}`", path.display()))
    }

    /// Parses a config, resolving placeholders through the `env` closure
    fn parse(text: &str, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let raw: RawConfig = serde_json::from_str(text)?;
        let servers = raw
            .mcp_servers
            .into_iter()
            .map(|(name, raw)| {
                if name.is_empty() {
                    bail!("server names must not be empty");
                }
                let server = Server::from_raw(raw, &env)
                    .with_context(|| format!("invalid server `{name}`"))?;
                Ok((name, server))
            })
            .collect::<Result<_>>()?;

        Ok(Self { servers })
    }
}

impl Server {
    fn from_raw(raw: RawServer, env: &impl Fn(&str) -> Option<String>) -> Result<Self> {
        let kind = match (raw.kind, &raw.command, &raw.url) {
            (Some(kind), _, _) => kind,
            (None, Some(_), None) => Kind::Stdio,
            (None, None, Some(_)) => Kind::Http,
            (None, Some(_), Some(_)) => bail!("has both `command` and `url`"),
            (None, None, None) => bail!("needs either `command` or `url`"),
        };

        let expand = |field: &str, text: &str| {
            expand(text, env).with_context(|| format!("failed to expand `{field}`"))
        };
        let expand_map = |field: &str, map: Option<BTreeMap<String, String>>| {
            map.unwrap_or_default()
                .into_iter()
                .map(|(key, value)| Ok((key.clone(), expand(&format!("{field}.{key}"), &value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
        };

        let transport = match kind {
            Kind::Stdio => {
                reject(kind, "url", &raw.url)?;
                reject(kind, "headers", &raw.headers)?;
                let command = expand("command", &raw.command.context("needs `command`")?)?;
                if command.is_empty() {
                    bail!("`command` must not be empty");
                }
                let args = raw
                    .args
                    .unwrap_or_default()
                    .iter()
                    .map(|arg| expand("args", arg))
                    .collect::<Result<_>>()?;
                let env = expand_map("env", raw.env)?;
                Transport::Stdio { command, args, env }
            }
            Kind::Http => {
                reject(kind, "command", &raw.command)?;
                reject(kind, "args", &raw.args)?;
                reject(kind, "env", &raw.env)?;
                let url = expand("url", &raw.url.context("needs `url`")?)?;
                // The URL may contain expanded secrets, so errors don't include it
                let parsed = Url::parse(&url).context("invalid `url`")?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    bail!("`url` must be http or https");
                }
                let headers = expand_map("headers", raw.headers)?;
                Transport::Http { url, headers }
            }
        };

        Ok(Self {
            description: raw.description,
            transport,
        })
    }
}

fn reject<T>(kind: Kind, field: &str, value: &Option<T>) -> Result<()> {
    if value.is_some() {
        bail!("`{field}` is not valid for a `{kind}` server");
    }
    Ok(())
}

fn expand(text: &str, env: impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        let Some(len) = rest[start + 2..].find('}') else {
            break;
        };
        let end = start + 2 + len + 1;
        let inner = &rest[start + 2..end - 1];
        let (name, default) = match inner.split_once(":-") {
            Some((name, default)) => (name, Some(default)),
            None => (inner, None),
        };

        out.push_str(&rest[..start]);
        if is_var_name(name) {
            match (env(name), default) {
                (Some(value), Some(default)) if value.is_empty() => out.push_str(default),
                (Some(value), _) => out.push_str(&value),
                (None, Some(default)) => out.push_str(default),
                (None, None) => bail!("environment variable `{name}` is not set"),
            }
        } else {
            out.push_str(&rest[start..end]);
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    Ok(out)
}

fn is_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        match name {
            "TOKEN" => Some("secret".into()),
            "EMPTY" => Some(String::new()),
            _ => None,
        }
    }

    fn parse(text: &str) -> Result<Config> {
        Config::parse(text, env)
    }

    fn parse_one(server: serde_json::Value) -> Result<Server> {
        let text = serde_json::json!({ "mcpServers": { "s": server } }).to_string();
        Ok(parse(&text)?.servers.remove("s").unwrap())
    }

    fn error(server: serde_json::Value) -> String {
        format!("{:#}", parse_one(server).unwrap_err())
    }

    #[test]
    fn infers_stdio_from_command() {
        let server =
            parse_one(serde_json::json!({ "command": "run", "description": "Runs" })).unwrap();
        assert_eq!(
            server,
            Server {
                description: Some("Runs".into()),
                transport: Transport::Stdio {
                    command: "run".into(),
                    args: vec![],
                    env: BTreeMap::new(),
                },
            }
        );
    }

    #[test]
    fn infers_http_from_url() {
        let server = parse_one(serde_json::json!({ "url": "https://example.com/mcp" })).unwrap();
        assert_eq!(server.transport.kind(), Kind::Http);
    }

    #[test]
    fn ignores_unknown_fields() {
        let config = parse(
            r#"{ "other": 1, "mcpServers": { "s": { "command": "run", "disabled": false } } }"#,
        );
        assert!(config.is_ok());
    }

    #[test]
    fn requires_mcp_servers() {
        assert!(parse("{}").is_err());
    }

    #[test]
    fn rejects_invalid_servers() {
        let cases = [
            (serde_json::json!({}), "needs either `command` or `url`"),
            (
                serde_json::json!({ "command": "run", "url": "http://x" }),
                "has both",
            ),
            (serde_json::json!({ "type": "stdio" }), "needs `command`"),
            (serde_json::json!({ "type": "http" }), "needs `url`"),
            (serde_json::json!({ "command": "" }), "must not be empty"),
            (
                serde_json::json!({ "command": "run", "headers": {} }),
                "`headers` is not valid for a `stdio` server",
            ),
            (
                serde_json::json!({ "url": "http://x", "args": [] }),
                "`args` is not valid for a `http` server",
            ),
            (serde_json::json!({ "url": "not a url" }), "invalid `url`"),
            (
                serde_json::json!({ "url": "ftp://x" }),
                "must be http or https",
            ),
        ];
        for (server, expected) in cases {
            let error = error(server.clone());
            assert!(
                error.contains(expected),
                "{server}: `{error}` lacks `{expected}`"
            );
            assert!(error.contains("invalid server `s`"), "{error}");
        }
    }

    #[test]
    fn rejects_unknown_types() {
        // rmcp has no client for the deprecated SSE transport
        let error = parse(r#"{ "mcpServers": { "s": { "type": "sse", "url": "http://x" } } }"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown variant `sse`"), "{error}");
    }

    #[test]
    fn rejects_duplicate_names() {
        let error =
            parse(r#"{ "mcpServers": { "s": { "command": "a" }, "s": { "command": "b" } } }"#)
                .unwrap_err()
                .to_string();
        assert!(error.contains("duplicate server `s`"), "{error}");
    }

    #[test]
    fn url_errors_do_not_leak_secrets() {
        for url in ["${TOKEN} not a url", "ftp://host/${TOKEN}"] {
            let error = error(serde_json::json!({ "url": url }));
            assert!(!error.contains("secret"), "{error}");
        }
    }

    #[test]
    fn rejects_empty_names() {
        assert!(parse(r#"{ "mcpServers": { "": { "command": "run" } } }"#).is_err());
    }

    #[test]
    fn expands_every_field() {
        let stdio = parse_one(serde_json::json!({
            "command": "${TOKEN}/bin",
            "args": ["--key=${TOKEN}"],
            "env": { "KEY": "${TOKEN}" },
        }))
        .unwrap();
        assert_eq!(
            stdio.transport,
            Transport::Stdio {
                command: "secret/bin".into(),
                args: vec!["--key=secret".into()],
                env: BTreeMap::from([("KEY".into(), "secret".into())]),
            }
        );

        let http = parse_one(serde_json::json!({
            "url": "https://example.com/${TOKEN}",
            "headers": { "Authorization": "Bearer ${TOKEN}" },
        }))
        .unwrap();
        assert_eq!(
            http.transport,
            Transport::Http {
                url: "https://example.com/secret".into(),
                headers: BTreeMap::from([("Authorization".into(), "Bearer secret".into())]),
            }
        );
    }

    #[test]
    fn reports_missing_variables() {
        let error = error(serde_json::json!({ "command": "run", "env": { "KEY": "${MISSING}" } }));
        assert!(error.contains("`env.KEY`"), "{error}");
        assert!(error.contains("`MISSING` is not set"), "{error}");
    }

    #[test]
    fn expands_placeholders() {
        let cases = [
            ("${TOKEN}", "secret"),
            ("a${TOKEN}b${TOKEN}c", "asecretbsecretc"),
            ("${MISSING:-fallback}", "fallback"),
            ("${EMPTY:-fallback}", "fallback"),
            ("${EMPTY}", ""),
            ("${TOKEN:-fallback}", "secret"),
            ("${MISSING:-}", ""),
            ("$TOKEN", "$TOKEN"),
            ("${not a name}", "${not a name}"),
            ("${1ABC}", "${1ABC}"),
            ("${TOKEN", "${TOKEN"),
            ("${}", "${}"),
            ("ünï${TOKEN}cödé", "ünïsecretcödé"),
        ];
        for (text, expected) in cases {
            assert_eq!(expand(text, env).unwrap(), expected, "{text}");
        }
    }
}
