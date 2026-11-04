# Design: credential providers and access policies

Status: draft

## Summary

gate takes every server's secrets from its config file and environment, and shows every
configured server to every request. That's enough for most users, but not for platforms that
run gate on someone else's behalf. Those platforms need to:

- hand servers short-lived credentials, refresh them before they expire, and replace them
  when a server rejects them
- decide, request by request, which servers the caller may see and use

This document proposes two optional hooks for that, **credential providers** and **access
policies**. gate defines their protocols and does the hard parts itself: caching, refresh,
rotation, telling auth failures apart from other errors, keeping secrets out of logs, and
failing closed. The platform-specific logic stays behind the hook, in the platform's own
code. A config that uses neither hook behaves exactly as gate does today.

## Goals

- No change for existing configs, and nothing new to learn for users who don't need this
- Nothing in gate that's specific to one platform, vendor or MCP server
- Hooks that can be written in any language, with no need to build gate from source
- Secrets that reach only the server they're for, and never logs or the agent's context
- The same guarantees gate gives today: no replayed calls, bounded output, cancellation
  that reaches downstream servers

## Non-goals

- Running OAuth flows for MCP servers. A credential provider can do that and hand gate the
  result, but gate won't redirect browsers or store refresh tokens.
- Policies that rewrite or filter tool arguments or results. Access is per server.
- Hooks that change a server's non-secret config, like its URL or command.

## Overview

```jsonc
{
  "credentialProviders": {
    "vault": { "command": "vault-mcp-lease", "args": ["--role", "agents"] }
  },
  "accessPolicies": {
    "platform": {
      "http": {
        "url": "https://platform.example/gate/access",
        "headers": { "Authorization": "Bearer ${file:/run/platform/token}" }
      }
    }
  },
  "mcpServers": {
    "github": {
      "title": "GitHub",
      "command": "github-mcp-server",
      "args": ["stdio"],
      "env": { "GITHUB_PERSONAL_ACCESS_TOKEN": "${credential:token}" },
      "credentials": { "provider": "vault", "params": { "path": "github/agents" } },
      "access": { "policy": "platform", "params": { "connection": "c_123" } }
    },
    "filesystem": { "command": "mcp-filesystem", "args": ["/srv"] }
  }
}
```

- `credentialProviders` and `accessPolicies` name hooks, each reached over HTTP or by running
  a command.
- A server opts into a hook with `credentials` or `access`. Each has a `params` object that
  gate passes to the hook as is and never reads. IDs, roles and paths mean whatever the
  hook wants them to.
- `${credential:NAME}` placeholders are filled from the server's current lease.
- `filesystem` uses neither hook, so it works exactly as it does today.

## Hooks

Both kinds of hook share their transports and their request envelope.

### Transports

**`http`**: gate sends `POST` with a JSON body and expects a `2xx` response with a JSON body.
Any other status, a body that isn't valid JSON, or a body that doesn't match the schema is an
error. `url` and `headers` support placeholders (see [Placeholders](#placeholders)), which are
expanded for every request, so a `${file:…}` header picks up a rotated token.

**`command`**: gate runs `command` with `args`, writes the request to its stdin, closes stdin,
and reads the response from stdout. A nonzero exit code is an error. One process runs per
request, like git credential helpers or AWS `credential_process`. The command inherits gate's
environment plus its own `env`, since it usually needs secrets that servers mustn't see
(see [Environment isolation](#environment-isolation)).

Both transports also accept:

| Field | Default | Meaning |
|---|---|---|
| `timeoutSeconds` | 10 for credentials, 5 for access | How long one request may take |

Responses are limited to 64 KiB. A command's stderr goes to gate's stderr, prefixed with the
hook's name, and never to the agent.

### Envelope

Every request has `version` and `kind`:

```json
{ "version": 1, "kind": "credentials", "...": "..." }
```

`version` changes only for incompatible changes. Hooks should ignore fields they don't
recognize, and so does gate.

### Errors

A hook that errors, times out or answers with something invalid counts as a failed attempt.
The agent sees a short, fixed message naming the server and the hook, never what the hook
returned. Hooks can't put secrets in front of the agent by accident, even in their errors.

## Credential providers

### Protocol

Request:

```json
{
  "version": 1,
  "kind": "credentials",
  "server": "github",
  "params": { "path": "github/agents" },
  "rejectedCredentialId": "lease-41"
}
```

`rejectedCredentialId` is present only after a server rejected that credential (see
[Auth failures](#auth-failures)). The provider should return a different one or fail.

Response:

```json
{
  "credentialId": "lease-42",
  "values": { "token": "ghs_…" },
  "refreshAfter": "2026-11-04T12:00:00Z",
  "expiresAt": "2026-11-04T12:10:00Z",
  "authFailures": [
    { "isError": true, "text": "^.*: 401 Bad credentials \\[\\]$" }
  ]
}
```

| Field | Required | Meaning |
|---|---|---|
| `credentialId` | yes | Identifies this credential, so gate can report it as rejected |
| `values` | yes | Fills the server's `${credential:NAME}` placeholders |
| `refreshAfter` | no | When gate should get a new lease. Without it, gate keeps the lease until it's rejected or expires. |
| `expiresAt` | no | When the lease stops being usable. Must not be before `refreshAfter`. |
| `authFailures` | no | Extra ways to recognize a rejected credential in tool results |

### Placeholders

`${credential:NAME}` is allowed in a server's `env` and `headers`, and nowhere else. Arguments
show up in process listings and URLs show up in logs, so config loading rejects
`${credential:…}` in `command`, `args` and `url`. If the lease has no value for a placeholder,
the connection attempt fails and names the missing value, not the lease's other values.

This document also adds `${file:PATH}`, allowed anywhere existing placeholders are. It's
replaced by the file's contents, minus one trailing newline, and read every time it's used:
at each connection for a server, and at each request for a hook. That's how tokens that
rotate while gate is running, like Kubernetes projected tokens or a platform's per-session
token, reach a hook without restarting gate.

### Lifecycle

Getting a lease is part of a connection attempt. A server with `credentials` connects like
this:

1. Get a lease, unless the current one is still usable.
2. Expand the server's placeholders with it.
3. Start the server or open the HTTP session, then list its tools, as today.

A provider that fails is a failed attempt like any other, so the retry delays, background
concurrency limit and request-triggered retries all apply unchanged. Each server's supervisor
makes at most one lease request at a time, and requests that arrive meanwhile wait for its
result, as they do for attempts today.

Once a server is ready, its supervisor keeps the lease current:

- **At `refreshAfter`**, it requests a new lease in the background. If the values haven't
  changed, it just keeps the new `credentialId` and times. If they have, it rotates (below).
  If the request fails, the current lease stays in use and the request is retried with the
  usual delays until `expiresAt`.
- **At `expiresAt`**, a server that still has no new lease stops being ready, and requests
  fail fast until a lease arrives.
- **Before each call**, gate checks `expiresAt` too, so a supervisor that wakes up late, for
  example after the machine was suspended, can't use an expired lease.

Rotating replaces the connection:

1. The server's status becomes `Attempting`, so new requests wait for the new connection
   instead of failing.
2. Calls already running on the old connection get up to the call timeout to finish.
3. The old connection is closed. Stdio servers are restarted, since their environment can't
   change.
4. The new connection is made with the new lease.

gate closes the old connection before making the new one. Some stdio servers can't run twice
at once, for example because of lock files, and a stale and a fresh connection should never
serve the same request. Making the new connection first, for HTTP servers only, is an open
question.

### Auth failures

gate recognizes a rejected credential in three ways:

1. **Transport**: the HTTP transport got a `401`, during connection or during a call. rmcp
   reports these as `StreamableHttpError::AuthRequired`.
2. **Result**: the result has `isError: true` and `structuredContent.status` is `401`.
3. **Matchers from the lease**: each entry in `authFailures` matches a result whose only
   content is one text block, optionally requiring `isError`, when its `text` regex matches
   that text. Regexes use the `regex` crate, so they run in linear time. A lease may have at
   most 32, each at most 1 KiB.

Matchers come with each lease, not from the config, because the provider is what knows which
server and version a credential is for. Vendor-specific patterns live with the provider, not
in gate.

When a credential is rejected:

- The call that saw it returns exactly what the server sent, or fails with the transport
  error. It isn't retried, since the call may have had an effect.
- The server's credential is marked rejected. The supervisor then rotates, requesting a new
  lease with `rejectedCredentialId`.
- If the provider returns the rejected `credentialId` again, the attempt fails with "the
  provider has no other credential". The usual retry delays limit how often that can happen.

### Keeping secrets contained

- Lease values are replaced with `<redacted>` in the server's forwarded stderr, in the stderr
  tail added to connection errors, and in every error message gate produces. Values shorter
  than 8 bytes aren't redacted, since they'd hide unrelated text and are rarely secrets.
- Values are never logged, never included in `list_mcp_servers`, and never available to
  `execute_mcp_code`.
- The `${credential:…}` restriction above keeps them out of process listings and URLs.

## Access policies

### Protocol

Request:

```json
{
  "version": 1,
  "kind": "access",
  "servers": [
    { "server": "github", "params": { "connection": "c_123" } },
    { "server": "linear", "params": { "connection": "c_456" } }
  ]
}
```

`servers` lists every server that uses this policy and is relevant to the request, at most
100 per call. If a policy covers more servers than that, gate makes several calls.

Response:

```json
{ "allowed": ["github"], "ttlSeconds": 0 }
```

`allowed` names the servers that may be used. Names that weren't in the request are ignored.
`ttlSeconds` lets gate reuse the answer for that long. The default, `0`, means gate asks again
for every request.

If a policy sets `"includeMeta": true`, requests also carry `meta`: the `_meta` object of the
MCP request being authorized. This is for gateways serving several callers, which identify
themselves there. It's opt-in because `_meta` may hold things a policy has no need for, and
answers are cached separately for each distinct `meta`.

### Where gate applies it

A server whose policy doesn't allow it is treated as if it weren't configured:

| Tool | Behavior |
|---|---|
| `list_mcp_servers` | Leaves it out |
| `search_mcp_tools` | Leaves its tools out. Naming it in `serverIds` gives the usual "unknown server" error. |
| `get_mcp_tool_details`, `call_mcp_tool` | "Unknown server". gate checks before waiting for a connection, and again before sending the call if it had to wait. |
| `execute_mcp_code` | gate asks every policy once when the script starts, and only allowed servers can be called. Each call is checked again before it's sent, using `ttlSeconds` as usual. |

Each request asks all its policies in parallel, once per request. A batch of 20 searches
makes one call per policy, not 20.

Hiding a server doesn't stop gate from connecting to it in the background. Connecting doesn't
need the caller's permission, and keeping it connected means the server is ready as soon as
it's allowed. The search index covers every ready server, and policies are applied when
results are selected, as `serverIds` is applied today, so they cost no rebuilds.

### Failing closed

If a policy errors or times out, gate treats every server it covers as not allowed for that
request, and writes the error to gate's stderr. Servers that don't use the policy are
unaffected. Agents don't see the error, since telling them a server exists but is hidden
would reveal what the policy is there to hide.

## Environment isolation

Servers inherit gate's whole environment today, which can include tokens meant only for
hooks. Two changes fix that:

- **Variables that hooks use are removed.** Any variable named in an `env` placeholder in
  `credentialProviders` or `accessPolicies` is removed from what servers inherit. A server
  can still set it explicitly in its own `env`. This needs no configuration and covers the
  common case.
- **`inheritEnv` on a server** controls what else it inherits:
  - `true` (the default) inherits everything, minus the variables above.
  - `false` inherits nothing.
  - a list of names, like `["PATH", "HOME"]`, inherits only those.

Code workers already start with an empty environment, and that doesn't change.

## Rust API

gate becomes a library with a thin binary, so Rust programs can embed it and implement hooks
directly:

```rust
pub trait CredentialProvider: Send + Sync + 'static {
    fn lease<'a>(
        &'a self,
        request: CredentialRequest<'a>,
        ct: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Lease, HookError>>;
}

pub trait AccessPolicy: Send + Sync + 'static {
    fn allowed<'a>(
        &'a self,
        request: AccessRequest<'a>,
        ct: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Allowed, HookError>>;
}

Gateway::builder(config)
    .credential_provider("vault", MyProvider::new())
    .access_policy("platform", MyPolicy::new())
    .serve(stdio())
    .await?;
```

The `http` and `command` transports implement these traits. A config entry names either a
built-in transport or a hook registered with the builder, and config loading fails if a name
is neither. The types mirror the JSON protocol, so moving a hook between a process and Rust
doesn't change its behavior.

## Alternatives considered

- **Building platform support into gate**, behind a cargo feature or not. That ties a public
  project to one platform's API and release schedule, and other users can't do the same.
- **Hooks as MCP servers** that expose `get_credentials` and `check_access` tools. This reuses
  gate's client code, but it mixes control traffic with tools agents can see, and makes hook
  authors implement MCP to answer two questions.
- **Dynamic plugins** (`.so` or WASM). A stable plugin ABI or WASM host is much more machinery
  than two small protocols, and the hooks aren't performance-sensitive enough to need it.
- **Long-lived command hooks** speaking JSON-RPC, so hooks don't pay for a process start each
  time. HTTP already covers hooks that must be fast, and per-request processes are simpler to
  write and can't get stuck in a bad state. This can be added later as a third transport.
- **Hooks that see every tool call**, for argument-level policies. That's a different, much
  larger feature, and per-server access covers the cases that motivated this.

## Plan

Each step can be released on its own:

1. **Groundwork**: split the library from the binary, add `${file:…}`, add environment
   isolation, and make an invalid server fail only itself instead of the whole config. Each of
   these is useful without hooks.
2. **Hook transports and access policies**: the smaller hook, which gets the shared transport
   code into use.
3. **Credential providers**: leases, refresh and rotation in the supervisor, auth-failure
   detection and redaction.
4. **Public Rust API**: document and stabilize the traits once both hooks have settled.

Tests use the same approach as today's integration tests: hook servers built with axum,
command hooks written as shell scripts, and echo MCP servers. They should cover at least:

- requests that arrive mid-rotation wait for the new connection, and running calls finish
- a rejected credential is never retried for the same call, and is reported on the next lease
  request
- a provider that keeps returning a rejected credential stops after the usual retries
- secrets are redacted from stderr and errors, including when a connection fails
- a policy that errors or times out hides only its own servers
- hidden servers are indistinguishable from servers that aren't configured
- scripts can't call servers that weren't allowed when they started
- variables used by hooks never reach servers

## Open questions

1. Should HTTP servers make their new connection before closing the old one when rotating? It
   avoids waiting, but briefly holds two sessions with two different credentials.
2. Some servers only take a token as an argument. Should `${credential:…}` be allowed in
   `args`, behind an explicit option, or should those servers be wrapped in a script instead?
3. Should `list_mcp_servers` say how many servers are hidden, without naming them? It helps
   explain why an expected server is missing, but it says something about the policy.
4. Should an access policy be able to answer "allowed, but read-only" or similar? Nothing needs
   it yet, and the response format leaves room to add it.

## Appendix: how a hosted platform would use this

The motivating case is an agent platform that runs gate in each sandbox (Tembo's, for one):

- Its API writes gate's config when the sandbox starts, with `params` carrying its own
  integration and connection IDs.
- Its credential endpoint implements the credentials protocol. The vendor-specific error
  patterns it keeps for each MCP server become `authFailures` in its leases.
- Its access endpoint implements the access protocol. The bearer token identifies the user who
  sent the current message, so the endpoint can allow only that user's personal connections.
  The token changes with each message, so the platform writes it to a file and the policy
  reads it with `${file:…}`.
- The tokens for its endpoints are only named in hook configs, so environment isolation keeps
  them away from servers without further setup.

None of this needs anything in gate that's specific to the platform.
