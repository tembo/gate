use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
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
    pub gateway: GatewaySettings,
    pub servers: BTreeMap<ServerId, Server>,
    pub credential_providers: BTreeMap<String, crate::credentials::Provider>,
    pub access_policies: BTreeMap<String, crate::hooks::Hook>,
    pub pre_connect: BTreeMap<String, std::sync::Arc<crate::pre_connect::PreConnect>>,
}

/// Agent-facing metadata; these strings are literal and never expand secrets.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewaySettings {
    pub name: String,
    pub version: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub instructions: Option<String>,
    pub tool_descriptions: BTreeMap<String, String>,
}
impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            name: "gate".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            title: None,
            description: None,
            instructions: None,
            tool_descriptions: BTreeMap::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Server {
    pub description: Option<String>,
    pub access: Option<Access>,
    pub credentials: Option<crate::credentials::Binding>,
    /// The pre-connect step to run before every connection attempt
    pub pre_connect: Option<String>,
    /// How to reach the server, or why its definition is invalid. An invalid server fails on
    /// its own instead of rejecting the whole config
    pub transport: Result<Transport, Invalid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Invalid {
    /// The server's transport, when it's known
    pub kind: Option<Kind>,
    pub error: std::sync::Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Access {
    pub policy: String,
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        environment: Environment,
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>,
        /// Forward proxy for every request to the server, for servers only reachable through one
        proxy: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum InheritEnv {
    All(bool),
    Names(BTreeSet<String>),
}

impl Default for InheritEnv {
    fn default() -> Self {
        Self::All(true)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Environment {
    inherit: InheritEnv,
    pub(crate) hook_env: BTreeSet<String>,
    /// The names listed in `hookEnv`, whose values aren't inherited under any name.
    /// Variables hooks merely reference, like a URL, aren't included
    secret_env: BTreeSet<String>,
}

impl Environment {
    pub fn apply(
        &self,
        command: &mut tokio::process::Command,
        explicit: &BTreeMap<String, String>,
    ) {
        command.env_clear();
        // A `hookEnv` variable's value stays with the hooks under any name, so a copy of
        // a token in another variable isn't passed on either
        let hook_values: Vec<_> = env::vars_os()
            .filter(|(name, value)| {
                !value.is_empty()
                    && name
                        .to_str()
                        .is_some_and(|name| self.secret_env.contains(name))
            })
            .map(|(_, value)| value)
            .collect();
        command.envs(env::vars_os().filter(|(name, value)| {
            if name
                .to_str()
                .is_some_and(|name| self.hook_env.contains(name))
                || hook_values.contains(value)
            {
                return false;
            }
            match &self.inherit {
                InheritEnv::All(inherit) => *inherit,
                InheritEnv::Names(names) => name.to_str().is_some_and(|name| names.contains(name)),
            }
        }));
        command.envs(
            explicit
                .iter()
                .filter(|(name, _)| !self.hook_env.contains(*name)),
        );
    }
}

impl Server {
    pub fn kind(&self) -> Option<Kind> {
        match &self.transport {
            Ok(transport) => Some(transport.kind()),
            Err(invalid) => invalid.kind,
        }
    }
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
    #[serde(default)]
    gateway: GatewaySettings,
    #[serde(default)]
    access_policies: BTreeMap<String, crate::hooks::RawHook>,
    #[serde(default)]
    credential_providers: BTreeMap<String, crate::credentials::RawProvider>,
    #[serde(default)]
    pre_connect: BTreeMap<String, crate::hooks::RawHook>,
    #[serde(default)]
    hook_env: BTreeSet<String>,
    #[serde(default)]
    inherit_env: InheritEnv,
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
    credentials: Option<crate::credentials::Binding>,
    access: Option<Access>,
    #[serde(rename = "preConnect")]
    pre_connect: Option<String>,
    #[serde(rename = "inheritEnv")]
    inherit_env: Option<InheritEnv>,
    /// Read as a string, so an unsupported type only fails its server
    #[serde(rename = "type")]
    kind: Option<String>,
    command: Option<String>,
    args: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    url: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    proxy: Option<String>,
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
        if raw.gateway.name.trim().is_empty() || raw.gateway.version.trim().is_empty() {
            bail!("gateway name and version must not be empty");
        }
        for name in raw.gateway.tool_descriptions.keys() {
            if crate::Gate::tool_router().get(name).is_none() {
                bail!("unknown gateway tool `{name}`");
            }
        }
        validate_environment_names(&raw.hook_env)?;
        validate_inheritance(&raw.inherit_env)?;
        let raw_inheritance = raw.inherit_env;
        let secret_env = raw.hook_env;
        let mut hook_env = secret_env.clone();
        let mut access_policies = BTreeMap::new();
        for (name, hook) in raw.access_policies {
            if name.is_empty() {
                bail!("policy names must not be empty");
            }
            let hook = hook
                .resolve(&env, &mut hook_env)
                .with_context(|| format!("invalid access policy `{name}`"))?;
            access_policies.insert(name, hook);
        }
        let mut credential_providers = BTreeMap::new();
        for (name, provider) in raw.credential_providers {
            if name.is_empty() {
                bail!("provider names must not be empty");
            }
            credential_providers.insert(
                name.clone(),
                provider
                    .resolve(&env, &mut hook_env)
                    .with_context(|| format!("invalid credential provider `{name}`"))?,
            );
        }
        let mut pre_connect = BTreeMap::new();
        for (name, hook) in raw.pre_connect {
            if name.is_empty() {
                bail!("pre-connect step names must not be empty");
            }
            // A first run may install or start things, so the default timeout is generous
            let hook = hook
                .resolve_command(&env, &mut hook_env, 120)
                .with_context(|| format!("invalid pre-connect step `{name}`"))?;
            pre_connect.insert(
                name.clone(),
                std::sync::Arc::new(crate::pre_connect::PreConnect::new(name, hook)),
            );
        }
        let servers = raw
            .mcp_servers
            .into_iter()
            .map(|(name, raw)| {
                if name.is_empty() {
                    bail!("server names must not be empty");
                }
                if let Some(access) = &raw.access
                    && !access_policies.contains_key(&access.policy)
                {
                    bail!(
                        "server `{name}` references unknown access policy `{}`",
                        access.policy
                    );
                }
                if let Some(step) = &raw.pre_connect
                    && !pre_connect.contains_key(step)
                {
                    bail!("server `{name}` references unknown pre-connect step `{step}`");
                }
                if let Some(credentials) = &raw.credentials
                    && !credential_providers.contains_key(&credentials.provider)
                {
                    bail!(
                        "server `{name}` references unknown credential provider `{}`",
                        credentials.provider
                    );
                }
                let server = Server::from_raw(raw, &env, &raw_inheritance, &hook_env, &secret_env);
                Ok((name, server))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            gateway: raw.gateway,
            servers,
            access_policies,
            credential_providers,
            pre_connect,
        })
    }
}

impl Server {
    fn from_raw(
        mut raw: RawServer,
        env: &impl Fn(&str) -> Option<String>,
        inherited: &InheritEnv,
        hook_env: &BTreeSet<String>,
        secret_env: &BTreeSet<String>,
    ) -> Self {
        let kind = raw.kind();
        let known = kind.as_ref().ok().copied();
        let transport = kind
            .and_then(|kind| Self::transport(kind, &mut raw, env, inherited, hook_env, secret_env))
            .map_err(|error| Invalid {
                kind: known,
                error: format!("{error:#}").into(),
            });
        Self {
            credentials: raw.credentials,
            description: raw.description,
            access: raw.access,
            pre_connect: raw.pre_connect,
            transport,
        }
    }

    fn transport(
        kind: Kind,
        raw: &mut RawServer,
        env: &impl Fn(&str) -> Option<String>,
        inherited: &InheritEnv,
        hook_env: &BTreeSet<String>,
        secret_env: &BTreeSet<String>,
    ) -> Result<Transport> {
        if let Some(inherit) = &raw.inherit_env {
            validate_inheritance(inherit)?;
        }

        let expand = |field: &str, text: &str| {
            if text.contains("${credential:")
                && !field.starts_with("env.")
                && !field.starts_with("headers.")
            {
                bail!("credential placeholders are only allowed in env and headers");
            }
            expand(text, env, hook_env).with_context(|| format!("failed to expand `{field}`"))
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
                reject(kind, "proxy", &raw.proxy)?;
                let command = expand("command", &raw.command.take().context("needs `command`")?)?;
                if command.is_empty() {
                    bail!("`command` must not be empty");
                }
                let args = raw
                    .args
                    .take()
                    .unwrap_or_default()
                    .iter()
                    .map(|arg| expand("args", arg))
                    .collect::<Result<_>>()?;
                let env = expand_map("env", raw.env.take())?;
                for name in env.keys() {
                    if hook_env.contains(name) {
                        bail!("environment variable `{name}` is reserved for hooks");
                    }
                }
                let environment = Environment {
                    inherit: raw.inherit_env.take().unwrap_or_else(|| inherited.clone()),
                    hook_env: hook_env.clone(),
                    secret_env: secret_env.clone(),
                };
                Transport::Stdio {
                    command,
                    args,
                    env,
                    environment,
                }
            }
            Kind::Http => {
                reject(kind, "inheritEnv", &raw.inherit_env)?;
                reject(kind, "command", &raw.command)?;
                reject(kind, "args", &raw.args)?;
                reject(kind, "env", &raw.env)?;
                let url = expand("url", &raw.url.take().context("needs `url`")?)?;
                // The URL may contain expanded secrets, so errors don't include it
                let parsed = Url::parse(&url).context("invalid `url`")?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    bail!("`url` must be http or https");
                }
                let headers = expand_map("headers", raw.headers.take())?;
                let proxy = raw
                    .proxy
                    .take()
                    .map(|proxy| {
                        let proxy = expand("proxy", &proxy)?;
                        let parsed = Url::parse(&proxy).context("invalid `proxy`")?;
                        if !matches!(parsed.scheme(), "http" | "https") {
                            bail!("`proxy` must be http or https");
                        }
                        Ok(proxy)
                    })
                    .transpose()?;
                Transport::Http {
                    url,
                    headers,
                    proxy,
                }
            }
        };

        crate::credentials::validate_transport(&transport, raw.credentials.as_ref())?;
        Ok(transport)
    }
}

impl RawServer {
    fn kind(&self) -> Result<Kind> {
        Ok(match (self.kind.as_deref(), &self.command, &self.url) {
            (Some("stdio"), _, _) => Kind::Stdio,
            (Some("http"), _, _) => Kind::Http,
            // rmcp has no client for the deprecated SSE transport
            (Some("sse"), _, _) => bail!("the SSE transport isn't supported"),
            (Some(kind), _, _) => bail!("unknown type `{kind}`"),
            (None, Some(_), None) => Kind::Stdio,
            (None, None, Some(_)) => Kind::Http,
            (None, Some(_), Some(_)) => bail!("has both `command` and `url`"),
            (None, None, None) => bail!("needs either `command` or `url`"),
        })
    }
}

fn reject<T>(kind: Kind, field: &str, value: &Option<T>) -> Result<()> {
    if value.is_some() {
        bail!("`{field}` is not valid for a `{kind}` server");
    }
    Ok(())
}

fn validate_environment_names(names: &BTreeSet<String>) -> Result<()> {
    for name in names {
        if !is_var_name(name) {
            bail!("invalid environment variable name `{name}`");
        }
    }
    Ok(())
}

fn validate_inheritance(inherit: &InheritEnv) -> Result<()> {
    if let InheritEnv::Names(names) = inherit {
        validate_environment_names(names)?;
    }
    Ok(())
}

pub(crate) fn expand(
    text: &str,
    env: impl Fn(&str) -> Option<String>,
    hook_env: &BTreeSet<String>,
) -> Result<String> {
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
            if hook_env.contains(name) {
                bail!("environment variable `{name}` is reserved for hooks");
            }
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

pub(crate) fn is_var_name(name: &str) -> bool {
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

    /// Why the server is invalid
    fn error(server: serde_json::Value) -> String {
        parse_one(server)
            .unwrap()
            .transport
            .unwrap_err()
            .error
            .to_string()
    }

    /// Why server `s` is invalid, in a config that's otherwise valid
    fn server_error(config: &serde_json::Value) -> String {
        let mut config = parse(&config.to_string()).unwrap();
        config
            .servers
            .remove("s")
            .unwrap()
            .transport
            .unwrap_err()
            .error
            .to_string()
    }

    #[test]
    fn pre_connect_steps_are_commands_that_servers_reference() {
        let config = parse(
            &serde_json::json!({
                "preConnect": { "net": { "command": "join", "env": { "KEY": "${TOKEN}" } } },
                "mcpServers": { "s": { "url": "https://example.com", "preConnect": "net" } },
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(config.servers["s"].pre_connect.as_deref(), Some("net"));
        assert!(config.pre_connect.contains_key("net"));

        let step =
            serde_json::json!({ "net": { "command": "join", "env": { "KEY": "${TOKEN}" } } });
        for (config, message) in [
            (
                serde_json::json!({
                    "preConnect": { "net": { "http": { "url": "https://example.com" } } },
                    "mcpServers": {},
                }),
                "must be a command",
            ),
            (
                serde_json::json!({
                    "mcpServers": { "s": { "url": "https://example.com", "preConnect": "missing" } },
                }),
                "unknown pre-connect step `missing`",
            ),
        ] {
            let error = format!("{:#}", parse(&config.to_string()).unwrap_err());
            assert!(error.contains(message), "{error}");
        }
        // Like other hooks', a step's variables are reserved for hooks
        let error = server_error(&serde_json::json!({
            "preConnect": step,
            "mcpServers": { "s": { "command": "run", "env": { "T": "${TOKEN}" } } },
        }));
        assert!(error.contains("reserved for hooks"), "{error}");
    }

    #[test]
    fn validates_gateway_metadata() {
        for gateway in [
            serde_json::json!({"name":" "}),
            serde_json::json!({"version":""}),
            serde_json::json!({"toolDescriptions":{"missing":"description"}}),
            serde_json::json!({"instruction":"typo"}),
        ] {
            let config = serde_json::json!({"gateway":gateway,"mcpServers":{}});
            assert!(parse(&config.to_string()).is_err(), "{config}");
        }
    }

    #[test]
    fn validates_credential_configuration() {
        let provider =
            serde_json::json!({"command":"run", "refreshFailure":"revoke", "drainSeconds":0});
        for (server, message) in [
            (
                serde_json::json!({"command":"${credential:token}"}),
                "only allowed in env and headers",
            ),
            (
                serde_json::json!({"command":"run","args":["${credential:token}"]}),
                "only allowed in env and headers",
            ),
            (
                serde_json::json!({"url":"https://example.com/${credential:token}"}),
                "only allowed in env and headers",
            ),
            (
                serde_json::json!({"command":"run","env":{"T":"${credential:token}"}}),
                "requires a provider",
            ),
            (
                serde_json::json!({"url":"https://example.com","credentials":{"provider":"p","mergeEnv":true}}),
                "only valid for stdio servers",
            ),
        ] {
            let value =
                serde_json::json!({"credentialProviders":{"p":provider},"mcpServers":{"s":server}});
            let error = server_error(&value);
            assert!(error.contains(message), "{value}: {error}");
        }
        // An unknown provider is a mistake in the config as a whole
        let value = serde_json::json!({"credentialProviders":{"p":provider},"mcpServers":{"s":{"command":"run","credentials":{"provider":"missing"}}}});
        assert!(parse(&value.to_string()).is_err());
        let value = serde_json::json!({"credentialProviders":{"p":provider},"mcpServers":{"s":{"command":"run","env":{"T":"Bearer ${credential:token}"},"credentials":{"provider":"p"}}}});
        assert!(parse(&value.to_string()).is_ok());
        let value = serde_json::json!({"credentialProviders":{"p":{"command":"run","env":{"T":"${TOKEN}"}}},"mcpServers":{"s":{"command":"run","env":{"T":"${TOKEN}"}}}});
        assert!(server_error(&value).contains("reserved for hooks"));
    }

    #[test]
    fn infers_stdio_from_command() {
        let server =
            parse_one(serde_json::json!({ "command": "run", "description": "Runs" })).unwrap();
        assert_eq!(
            server,
            Server {
                description: Some("Runs".into()),
                access: None,
                credentials: None,
                pre_connect: None,
                transport: Ok(Transport::Stdio {
                    command: "run".into(),
                    args: vec![],
                    env: BTreeMap::new(),
                    environment: Environment::default(),
                }),
            }
        );
    }

    #[test]
    fn infers_http_from_url() {
        let server = parse_one(serde_json::json!({ "url": "https://example.com/mcp" })).unwrap();
        assert_eq!(server.kind(), Some(Kind::Http));
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
        }
    }

    #[test]
    fn rejects_unknown_types() {
        let sse = error(serde_json::json!({ "type": "sse", "url": "http://x" }));
        assert!(sse.contains("SSE transport isn't supported"), "{sse}");
        let unknown = error(serde_json::json!({ "type": "ws", "url": "ws://x" }));
        assert!(unknown.contains("unknown type `ws`"), "{unknown}");
    }

    #[test]
    fn an_invalid_server_does_not_affect_the_others() {
        let config = parse(
            &serde_json::json!({
                "mcpServers": {
                    "unset": { "url": "${MISSING:-}" },
                    "sse": { "type": "sse", "url": "https://example.com/sse", "description": "Old" },
                    "ok": { "url": "https://example.com/mcp" },
                },
            })
            .to_string(),
        )
        .unwrap();
        assert!(config.servers["ok"].transport.is_ok());
        let unset = config.servers["unset"].transport.as_ref().unwrap_err();
        assert_eq!(unset.kind, Some(Kind::Http));
        assert!(unset.error.contains("invalid `url`"), "{}", unset.error);
        // Its other fields still apply, like its description
        assert_eq!(config.servers["sse"].description.as_deref(), Some("Old"));
        assert_eq!(config.servers["sse"].kind(), None);
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
            stdio.transport.unwrap(),
            Transport::Stdio {
                command: "secret/bin".into(),
                args: vec!["--key=secret".into()],
                env: BTreeMap::from([("KEY".into(), "secret".into())]),
                environment: Environment::default(),
            }
        );

        let http = parse_one(serde_json::json!({
            "url": "https://example.com/${TOKEN}",
            "headers": { "Authorization": "Bearer ${TOKEN}" },
        }))
        .unwrap();
        assert_eq!(
            http.transport.unwrap(),
            Transport::Http {
                url: "https://example.com/secret".into(),
                headers: BTreeMap::from([("Authorization".into(), "Bearer secret".into())]),
                proxy: None,
            }
        );
    }

    #[test]
    fn http_servers_take_a_proxy() {
        let server = parse_one(serde_json::json!({
            "url": "https://tools.internal/mcp",
            "proxy": "${PROXY:-http://localhost:1056}",
        }))
        .unwrap();
        assert!(matches!(
            server.transport.unwrap(),
            Transport::Http { proxy: Some(ref proxy), .. } if proxy == "http://localhost:1056"
        ));
        for (server, message) in [
            (
                serde_json::json!({ "url": "https://example.com", "proxy": "socks5://localhost:1055" }),
                "`proxy` must be http or https",
            ),
            (
                serde_json::json!({ "url": "https://example.com", "proxy": "not a url" }),
                "invalid `proxy`",
            ),
            (
                serde_json::json!({ "command": "run", "proxy": "http://localhost:1056" }),
                "`proxy` is not valid for a `stdio` server",
            ),
        ] {
            let error = error(server);
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn reports_missing_variables() {
        let error = error(serde_json::json!({ "command": "run", "env": { "KEY": "${MISSING}" } }));
        assert!(error.contains("`env.KEY`"), "{error}");
        assert!(error.contains("`MISSING` is not set"), "{error}");
    }

    #[test]
    fn rejects_hook_variables_in_server_fields() {
        for placeholder in ["${TOKEN}", "${TOKEN:-fallback}", "${MISSING:-fallback}"] {
            let servers = [
                serde_json::json!({"command": placeholder}),
                serde_json::json!({"command": "run", "args": [placeholder]}),
                serde_json::json!({"command": "run", "env": {"OTHER": placeholder}}),
                serde_json::json!({"url": format!("https://example.com/{placeholder}")}),
                serde_json::json!({"url": "https://example.com", "headers": {"Authorization": placeholder}}),
            ];
            for server in servers {
                let config = serde_json::json!({"hookEnv": ["TOKEN", "MISSING"], "mcpServers": {"s": server}});
                let error = server_error(&config);
                assert!(error.contains("reserved for hooks"), "{error}");
                assert!(!error.contains("secret"), "{error}");
            }
        }
        let config = serde_json::json!({"hookEnv": ["TOKEN"], "mcpServers": {"s": {"command": "run", "env": {"TOKEN": "literal"}}}});
        assert!(server_error(&config).contains("reserved for hooks"));
    }

    #[test]
    fn validates_access_policies_and_reserves_hook_placeholders() {
        for hook in [
            serde_json::json!({}),
            serde_json::json!({"command":"run", "http":{"url":"http://localhost"}}),
            serde_json::json!({"command":"run", "timeoutSeconds":0}),
            serde_json::json!({"http":{"url":"ftp://localhost"}}),
            serde_json::json!({"http":{"url":"http://localhost"},"env":{}}),
        ] {
            let config = serde_json::json!({"accessPolicies":{"p":hook},"mcpServers":{}});
            assert!(parse(&config.to_string()).is_err(), "{config}");
        }
        assert!(
            parse(r#"{"mcpServers":{"s":{"command":"run","access":{"policy":"missing"}}}}"#)
                .is_err()
        );
        for hook in [
            serde_json::json!({"command":"${TOKEN:-fallback}"}),
            serde_json::json!({"command":"run", "args":["${TOKEN}"]}),
            serde_json::json!({"command":"run", "env":{"X":"${TOKEN}"}}),
            serde_json::json!({"http":{"url":"https://example.com/${TOKEN}"}}),
            serde_json::json!({"http":{"url":"https://example.com", "headers":{"Authorization":"${TOKEN}"}}}),
        ] {
            let config = serde_json::json!({"accessPolicies":{"p":hook},"mcpServers":{"s":{"command":"run","args":["${TOKEN}"]}}});
            let error = server_error(&config);
            assert!(error.contains("reserved for hooks"), "{error}");
            assert!(!error.contains("secret"), "{error}");
        }
    }

    #[test]
    fn validates_environment_settings() {
        for config in [
            serde_json::json!({"hookEnv": ["BAD=NAME"], "mcpServers": {}}),
            serde_json::json!({"hookEnv": true, "mcpServers": {}}),
            serde_json::json!({"inheritEnv": "PATH", "mcpServers": {}}),
            serde_json::json!({"inheritEnv": [""], "mcpServers": {}}),
        ] {
            assert!(parse(&config.to_string()).is_err(), "{config}");
        }
        for server in [
            serde_json::json!({"command": "run", "inheritEnv": ["BAD=NAME"]}),
            serde_json::json!({"url": "https://example.com", "inheritEnv": false}),
        ] {
            assert!(parse_one(server).unwrap().transport.is_err());
        }
        assert!(parse(r#"{"inheritEnv": false, "hookEnv": ["TOKEN"], "mcpServers": {"s": {"url": "https://example.com"}}}"#).is_ok());
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
            assert_eq!(
                expand(text, env, &BTreeSet::new()).unwrap(),
                expected,
                "{text}"
            );
        }
    }
}
