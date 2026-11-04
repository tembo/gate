# Design: credential providers and access policies

Status: draft, revision 3

## Summary

gate takes every server's secrets from its config file and environment, and shows every
configured server to every request. That's enough for most users, but not for platforms that
run gate on someone else's behalf. Those platforms need to:

- hand servers short-lived credentials, refresh them before they expire, replace them when a
  server rejects them, and stop using them when they're revoked
- decide, request by request, which servers the current caller may see and use

This document proposes two optional hooks for that, **credential providers** and **access
policies**. gate defines their protocols and owns everything to do with connections:
leases, rotation, telling auth failures apart from other errors, cleaning its own
diagnostics, and failing closed. Who the caller is, what they may use and where credentials
come from stay behind the hooks, in the platform's own code. A config that uses neither hook
behaves exactly as gate does today.

## Goals

- No change for existing configs, and nothing new to learn for users who don't need this
- Nothing in gate that's specific to one platform, vendor or MCP server
- Hooks that can be written in any language, with no need to build gate from source
- Rules precise enough to implement and test: what happens to every call when a credential
  is refreshed, rotated, rejected or revoked
- The same guarantees gate gives today: no replayed calls, bounded output, cancellation
  that reaches downstream servers

## Non-goals

- Running OAuth flows for MCP servers. A credential provider can do that and hand gate the
  result, but gate won't redirect browsers or store refresh tokens.
- Policies that rewrite or filter tool arguments or results. Access is per server.
- Hooks that change a server's non-secret config, like its URL or command.
- Isolating servers from each other or from gate. Servers run as gate's user, as they do
  today (see [Trust model](#trust-model)).
- The legacy HTTP+SSE transport. MCP deprecated it in favor of Streamable HTTP, and rmcp, the
  official Rust SDK, has no client for it. Servers that only speak SSE need to move to
  Streamable HTTP or sit behind a bridge.

## Trust model

| Party | Trusted with |
|---|---|
| Whoever writes the config | Everything: it chooses the servers, hooks and their secrets |
| Hooks | The secrets they're given, and the decisions they return |
| Downstream servers | The credentials gate gives them. A server can return its own credential in a tool result, and gate can't stop that. |
| The MCP client (the agent) | Nothing beyond tool results and gate's own messages |

gate has no notion of who the caller is. When identity matters, the access policy determines
it itself for every request, from credentials and state the platform controls. Data that the
caller sends, like a request's `_meta`, is not identity.

gate doesn't deliberately expose credentials. It cleans its own diagnostics and the server
stderr it forwards (see [Secrets in diagnostics](#secrets-in-diagnostics)). Hooks and
downstream servers remain responsible for the secrets they receive.

## Overview

```jsonc
{
  "hookEnv": ["PLATFORM_TOKEN"],
  "inheritEnv": ["PATH", "HOME", "LANG", "TMPDIR"],
  "credentialProviders": {
    "platform": { "http": {
      "url": "https://platform.example/gate/credentials",
      "headers": { "Authorization": "Bearer ${PLATFORM_TOKEN}" }
    } }
  },
  "accessPolicies": {
    "platform": { "http": {
      "url": "https://platform.example/gate/access",
      "headers": { "Authorization": "Bearer ${PLATFORM_TOKEN}" }
    } }
  },
  "mcpServers": {
    "github": {
      "title": "GitHub",
      "command": "github-mcp-server",
      "args": ["stdio"],
      "env": { "GITHUB_PERSONAL_ACCESS_TOKEN": "${credential:token}" },
      "credentials": { "provider": "platform", "params": { "connection": "c_123" } },
      "access": { "policy": "platform", "params": { "connection": "c_123" } }
    },
    "filesystem": { "command": "mcp-filesystem", "args": ["/srv"] }
  }
}
```

- `credentialProviders` and `accessPolicies` name hooks, each reached over HTTP or by running
  a command.
- A server opts into a hook with `credentials` or `access`. Each has a `params` object that
  gate passes to the hook as is and never reads.
- `${credential:NAME}` placeholders are filled from the server's current lease.
- `hookEnv` and `inheritEnv` control which variables servers get (see
  [Environment](#environment)).
- `filesystem` uses neither hook. Without `hookEnv` and `inheritEnv`, it would work exactly as
  it does today.

## Hooks

Both kinds of hook share their transports and their request envelope.

### Transports

**`http`**: gate sends `POST` with a JSON body. `url` and `headers` support the usual
placeholders, expanded when the config loads.

**`command`**: gate runs `command` with `args`, writes the request to its stdin, closes stdin,
and reads the response from stdout. One process runs per request, like git credential helpers
or AWS `credential_process`. The command inherits gate's environment, including `hookEnv`,
plus its own `env`.

Both transports also accept:

| Field | Default | Meaning |
|---|---|---|
| `timeoutSeconds` | 10 for credentials, 5 for access | How long one request may take |
| `stderr` | `"drop"` | `"forward"` copies a command hook's stderr to gate's, for debugging |

Responses are limited to 64 KiB.

### Envelope

Every request has `version` and `kind`:

```json
{ "version": 1, "kind": "credentials", "...": "..." }
```

`version` changes only for incompatible changes. Hooks should ignore fields they don't
recognize, and so does gate.

### What the agent sees

Whatever a hook returns or fails with, the agent sees a short, fixed message naming the
server and the outcome, like "server `github` is unavailable: its credentials were denied".
It never sees a hook's error text, status code or body.

## Credential providers

### Request

```json
{
  "version": 1,
  "kind": "credentials",
  "server": "github",
  "params": { "connection": "c_123" },
  "rejectedCredentialId": "lease-41"
}
```

`rejectedCredentialId` is present only after a server rejected that credential (see
[Auth failures](#auth-failures)).

### Response

A provider answers with one of three outcomes.

**`lease`**: here's a credential.

```json
{
  "outcome": "lease",
  "credentialId": "cred-42",
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
| `credentialId` | yes | Identifies the credential material, not this issuance. A provider that returns the same token again must return the same ID. gate reports it back as `rejectedCredentialId`. |
| `values` | yes | Fills the server's `${credential:NAME}` placeholders, or its environment (see [Leased environments](#leased-environments)) |
| `refreshAfter` | no | When gate should ask again |
| `expiresAt` | no | The lease's hard expiry: no call is sent with it from this time on |
| `authFailures` | no | Extra ways to recognize a rejected credential in tool results, for this lease only |

**`denied`**: the caller may no longer use this server's credentials, for example because the
platform deactivated the session or the connection was removed. gate stops using any lease it
has for the server.

**`transient`**: the provider can't answer right now. gate tries again later, and whether it
keeps using an unexpired lease meanwhile depends on the provider's `refreshFailure` setting
(see [Refreshing](#refreshing)).

```json
{ "outcome": "denied" }
```

gate decides the outcome by applying these rules in order, stopping at the first that applies.

For `http` hooks:

1. No response, because of a connection error or the timeout: `transient`.
2. Status `401`, `403`, `404` or `410`: `denied`, whatever the body says.
3. Any other status outside `2xx`, including `408`, `429` and `5xx`: `transient`, whatever the
   body says.
4. Status `2xx` with a valid answer: that answer's outcome.
5. Status `2xx` with anything else: `transient`.

For `command` hooks:

1. The command timed out or couldn't be started: `transient`.
2. It exited nonzero: `transient`. Its stdout is ignored, even if it holds a valid answer, since
   a crashing helper may have written only part of one. A helper that means to deny must exit
   with status 0.
3. It exited with status 0 and wrote a valid answer: that answer's outcome.
4. It exited with status 0 and wrote anything else: `transient`.

So an existing endpoint that uses `403` and `404` for revoked access works without changes,
and a rate-limited or overloaded provider never closes healthy connections. gate never treats
a malformed answer as a revocation, and never treats a revocation as a reason to retry.

### Fingerprints

gate also identifies credentials itself, by a SHA-256 fingerprint of `values` with sorted
keys. Fingerprints stay in memory and are never logged. They let gate:

- reconnect only when the values actually changed
- refuse a lease whose values were already rejected, whatever its `credentialId`

### Placeholders

`${credential:NAME}` is allowed in a server's `env` and `headers`, and nowhere else. Arguments
show up in process listings and URLs show up in logs, so config loading rejects
`${credential:…}` in `command`, `args` and `url`. If a lease has no value named `NAME`, the
connection attempt fails, naming the missing value but no other value.

### Leased environments

Some providers hand out a set of environment variables that depends on the credential, rather
than values the config can name in advance. A stdio server can opt into merging them:

```json
"credentials": { "provider": "platform", "params": {}, "mergeEnv": true }
```

With `mergeEnv`, every lease value is added to the server's environment and replaces any
variable of the same name from `env`. Names must be valid environment variable names, and must
not be in `hookEnv`, or the attempt fails, naming the bad key. The set of names can differ
between leases, since rotating always restarts a stdio server. `mergeEnv` isn't available for
HTTP servers, which must place values in headers explicitly.

### Server states

A server with `credentials` uses gate's existing states, plus one:

| State | Meaning | Requests |
|---|---|---|
| `Attempting` | Getting a lease and connecting | Wait for the attempt |
| `Pending` | Waiting to retry | Fail fast |
| `Ready(g)` | Connection `g` is up, made with lease `L(g)` | Use connection `g` |
| `Rotating(g)` | Replacing connection `g` | Wait for the new connection |
| `Failed` | No usable connection | Fail, and trigger one new attempt, as today |

Each server's connections are numbered: `g` starts at 1 and goes up by one with every new
connection. Each server has one supervisor task, and every transition below happens in that
task. A server makes at most one lease request at a time, and requests that arrive meanwhile
wait for its result.

### Connecting

Getting a lease is the first step of a connection attempt:

1. Ask the provider for a lease. If an earlier attempt's credential was rejected and the
   provider hasn't returned different values since, the request carries that credential's ID
   as `rejectedCredentialId`.
   - `lease`: if its fingerprint has been rejected for this server, the attempt fails with "the
     provider has no other credential". If it has already expired, the attempt fails.
     Otherwise, continue.
   - `transient`: the attempt fails, and the usual retry delays apply.
   - `denied`: the attempt fails, and the server skips any remaining background attempts.
     The next request that needs the server triggers one new attempt, as for any failed
     server.
2. Record the lease's ID and fingerprint for this attempt.
3. Expand the server's placeholders with the lease, and merge it into the environment if
   `mergeEnv` is set.
4. Start the server or open the HTTP session, then list its tools, as today.
5. If the lease's `expiresAt` has passed by the time the server is ready, close the new
   connection. The attempt fails, and the next one asks for a new lease.

A credential can also be rejected during setup, while initializing or listing tools. If step 4
fails with an auth failure (see [Auth failures](#auth-failures)), the fingerprint recorded in
step 2 is marked rejected, and the next attempt asks for a lease with that credential's ID as
`rejectedCredentialId`. The usual retry delays apply, so a provider that keeps returning the
same credential can't cause a tight loop.

### Refreshing

Two separate settings govern how long a lease is used:

- **Hard expiry.** `expiresAt` is absolute. No call is sent with a lease from that time on,
  whatever else happens.
- **What a failed refresh does.** The provider's `refreshFailure` setting decides whether a
  `transient` refresh failure lets gate keep using an unexpired lease:

  ```json
  "credentialProviders": { "platform": { "http": { "...": "..." }, "refreshFailure": "keep" } }
  ```

The supervisor of a `Ready(g)` server asks for a new lease at `refreshAfter`. If `refreshAfter`
is missing, or isn't before `expiresAt`, it asks shortly before `expiresAt` instead: a tenth of
the lease's lifetime early, but never more than a minute. A lease with neither time is kept
until it's rejected or denied.

| Outcome | What happens |
|---|---|
| `lease`, same fingerprint | Replace `L(g)`'s ID, times and matchers, and keep connection `g` |
| `lease`, new fingerprint | Rotate to the new lease |
| `transient`, with `refreshFailure: "keep"` (the default) | Keep `L(g)` until its `expiresAt`, asking again with the usual retry delays |
| `transient`, with `refreshFailure: "revoke"` | [Revoke](#revoking) |
| `denied` | [Revoke](#revoking) |

At `expiresAt`, if `L(g)` hasn't been replaced, the server revokes it. gate also checks
`expiresAt` before every call, so a supervisor that wakes up late, for example after the
machine was suspended, can't use an expired lease.

Keeping a lease through a `transient` failure rides out short provider outages, with
`expiresAt` as the bound. The TypeScript gateway instead stops a server whenever a refresh
fails. A provider that wants that sets `refreshFailure: "revoke"`. Setting `expiresAt` equal to
`refreshAfter` wouldn't do the same thing, since then the lease would expire while a healthy
refresh was still under way.

### Rotating

Rotating replaces connection `g` with connection `g + 1`:

1. The server becomes `Rotating(g)`. New requests wait instead of failing.
2. Calls already running on connection `g` get up to `drainSeconds` to finish. That's 30
   seconds by default, and never more than the call timeout. When the time is up, gate sends
   the remaining calls `notifications/cancelled` and fails them with "interrupted while the
   server's credentials were replaced; the downstream outcome is unknown".
3. Connection `g` is closed. Stdio servers are restarted, since their environment can't
   change.
4. Connection `g + 1` is made with the new lease, or with one the provider gives now if
   rotation started from a rejection. A lease obtained before the drain is checked again
   first: if it expired during the drain, gate asks for another. Connecting follows the steps
   in [Connecting](#connecting), and if it fails, the server follows the usual rules for failed
   attempts.
5. Requests that waited check access again before they're sent (see
   [Where gate applies it](#where-gate-applies-it)).

gate closes the old connection before making the new one, for every transport. Some stdio
servers can't run twice at once, for example because of lock files, and no request should be
served by a connection made with a credential that's being replaced.

### Revoking

When a lease is denied or expires:

1. The server becomes `Failed` at once, so no new call uses the lease.
2. Calls already running get the same `drainSeconds` as when rotating, then are cancelled.
3. The connection is closed.

### Auth failures

A credential can be rejected during setup or during a call. gate looks for this in three
built-in ways:

1. **HTTP status.** The downstream server answered a request with HTTP 401, whatever the body
   held.
2. **JSON-RPC error.** The error's `data.status` is 401.
3. **Tool result.** The result has `isError: true` and its `structuredContent.status` is 401.

The HTTP status has to be captured where the response arrives. rmcp 3.5.1 reports a 401 as
`StreamableHttpError::AuthRequired` only if the response has a `WWW-Authenticate` header.
Otherwise, it either turns the body into a JSON-RPC error, dropping the status, or reports
`UnexpectedServerResponse` with the status only as formatted text. Neither keeps the status in
a form gate can rely on, and gate never parses formatted errors. Instead, gate records each
response's status at the transport boundary:

- either gate implements rmcp's public `StreamableHttpClient` trait itself, wrapping `reqwest`,
  and associates each response's status with the JSON-RPC request it answers
- or rmcp is changed upstream to keep the status on these errors, and gate uses that

With either approach, a JSON-RPC error that arrived on an HTTP 401 response counts as an auth
failure even without `data.status`. The tests cover a 401 with `WWW-Authenticate`, one without
it and with a plain body, and one without it and with a JSON-RPC error lacking `data.status`.
Each is checked during initialization, during tool listing and during a call.

Stdio servers have no HTTP status, so during setup they're detected only by the JSON-RPC rule.

Each lease can add more ways in `authFailures`. A matcher matches when every field it has
matches:

| Field | Matches when |
|---|---|
| `isError` | The result's `isError` equals this |
| `toolName` | This regex matches the name of the tool called |
| `text` | The result has exactly one content block, it's text, and this regex matches it |
| `json` | That one text block parses as JSON, and the string at `json.pointer` (a JSON Pointer) matches the regex `json.text` |

Regexes use the `regex` crate, so they run in linear time. A lease may have at most 32
matchers, and each regex may be at most 1 KiB. Matchers come with each lease, not from the
config, because the provider knows which server and version a credential is for.

### Attributing failures

Calls can still be running when a lease changes, so a failure must be attributed to the
credential that call used, not to whichever one is current. Setup failures are attributed to
the lease their attempt recorded (see [Connecting](#connecting)). For calls:

1. Every call records, when it's sent, its connection `g`, the fingerprint of `L(g)`, and
   `L(g)`'s matchers.
2. A call's result or error is checked with the matchers it recorded.
3. A failure counts only if the call's fingerprint is still the current one, and only if no
   recovery for that fingerprint has started yet. Otherwise it's logged and ignored. A late
   failure can never invalidate a newer credential.
4. The first failure that counts marks the fingerprint as rejected, and the server rotates,
   asking for a lease with `rejectedCredentialId` set to `L(g)`'s ID. Any further failures for
   the same fingerprint join that recovery.
5. Each server remembers its last 16 rejected fingerprints for as long as gate runs. They're
   kept per server: a credential that one server rejected may still be valid for another, even
   from the same provider.

The call that saw the failure returns exactly what the server sent, or fails with the
transport's error. **Calls are never replayed**, whatever happens, since they may already
have taken effect.

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

Response:

```json
{ "allowed": ["github"] }
```

Names that weren't in the request are ignored. Anything other than a valid answer, including
errors, timeouts and HTTP statuses outside `2xx`, means nothing is allowed.

### No caching

gate asks the policy for every request and never reuses an answer from an earlier request.
The caller can change between requests without gate knowing, for example when a platform
decides who the caller is from state on its side. Reusing a yes could give one caller
another's access.

To decide what a request may use, gate asks each policy once, with every relevant server in a
single call, and uses that answer for the whole request. A batch of 20 searches makes one
call per policy, not 20, and is never split. A policy that can't handle that many servers
should fail, which hides them all.

The only other calls are re-checks just before a tool call is sent. They happen when the call
had to wait for a connection, and for every call a script makes (see below). Each re-check
asks about that one server, and its answer is used only for that call.

Caching across requests is deferred (see [Deferred](#deferred)).

### Where gate applies it

A server whose policy doesn't allow it is treated as if it weren't configured:

| Tool | Behavior |
|---|---|
| `list_mcp_servers` | Leaves it out |
| `search_mcp_tools` | Leaves its tools out. Naming it in `serverIds` gives the usual "unknown server" error. |
| `get_mcp_tool_details`, `call_mcp_tool` | "Unknown server". gate checks before waiting for a connection, and again before sending the call if it had to wait, for example during a rotation. |
| `execute_mcp_code` | See below |

For `execute_mcp_code`:

1. When the script starts, gate asks every policy once. The servers allowed then are the
   script's ceiling, and the script can never call any other.
2. Before each of its calls is sent, gate asks the server's policy again, for that server
   alone.

So a script can't gain access while it runs, and loses access as soon as the policy stops
allowing it. If the platform decides the script shouldn't finish at all, for example because
another caller took over, it can cancel the MCP request. gate then cancels the script and its
running calls, as today.

Hiding a server doesn't stop gate from connecting to it in the background. The search index
covers every ready server, and policies are applied when results are selected, as `serverIds`
is today, so they never cause rebuilds.

### Failing closed

If a policy errors or times out, every server it covers is hidden for that request, and the
error goes to gate's stderr. Servers that don't use the policy are unaffected. The agent
doesn't see the error, since telling it a server exists but is hidden reveals what the policy
is there to hide.

## Environment

### Variables for hooks only

`hookEnv` lists variables meant only for hooks, like the token a hook uses to reach the
platform. They:

- are available to command hooks and to placeholders in hook configs
- are never inherited by servers
- can't be named in a server's `env` or `headers` placeholders. Config loading fails if they
  are.

A variable named in a placeholder in `credentialProviders` or `accessPolicies` is added to
`hookEnv` automatically. That covers HTTP hooks without any setup. It doesn't cover command
hooks, which can read any variable without naming it, so configs with command hooks should
list theirs.

### What servers inherit

`inheritEnv`, at the top level or on a server, controls what servers inherit besides their own
`env`:

- `true` (the default) inherits everything except `hookEnv`.
- `false` inherits nothing.
- a list of names inherits only those.

A server's setting overrides the top-level one. Platforms that launch gate with secrets in its
environment should use a list, not rely on `hookEnv` alone. Code workers already start with an
empty environment, and that doesn't change.

### What this doesn't do

These controls decide which variables gate hands each server. They don't isolate servers:
every server runs as gate's user and can read whatever that user can, including gate's own
`/proc/<pid>/environ` and any token files. Isolating servers from each other needs separate
users or sandboxes, which are out of scope.

## Secrets in diagnostics

gate cleans its own output, in these ways:

- Lease values are replaced with `<redacted>` in forwarded server stderr, in the stderr tail
  added to connection errors, and in every error message gate writes.
- Redaction covers the values of every lease whose connection is still open, including one
  being drained during a rotation, until that connection closes.
- Forwarded stderr is redacted as a stream, so a value split across two chunks is still
  caught. gate holds back up to the longest value's length, minus one byte, before forwarding.
- Values shorter than 8 bytes aren't redacted, since they'd hide unrelated text.
- Hook stderr isn't forwarded unless a hook sets `"stderr": "forward"`.

gate never logs lease values, never includes them in `list_mcp_servers`, and never gives them
to `execute_mcp_code`. Tool results are passed through unchanged, so a server that returns its
own credential in a result exposes it. That's covered by the [Trust model](#trust-model), not
prevented.

## Rust API

gate becomes a library with a thin binary, so Rust programs can embed it and implement hooks
directly:

```rust
pub trait CredentialProvider: Send + Sync + 'static {
    fn lease<'a>(
        &'a self,
        request: CredentialRequest<'a>,
        ct: &'a CancellationToken,
    ) -> BoxFuture<'a, CredentialOutcome>;
}

pub trait AccessPolicy: Send + Sync + 'static {
    fn allowed<'a>(
        &'a self,
        request: AccessRequest<'a>,
        ct: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Allowed, HookError>>;
}

Gateway::builder(config)
    .credential_provider("platform", MyProvider::new())
    .access_policy("platform", MyPolicy::new())
    .code_worker(CodeWorker::Executable("/usr/libexec/gate".into()))
    .serve(stdio())
    .await?;
```

The `http` and `command` transports implement these traits. A config entry names either a
built-in transport or a hook registered with the builder, and config loading fails if a name
is neither. The types mirror the JSON protocol, so moving a hook between a process and Rust
doesn't change its behavior.

### Code workers

`execute_mcp_code` runs each script in a worker process. Today, gate starts the worker by
running its own executable with `--code-worker`, which only works for the `gate` binary. A
program that embeds gate has to choose:

- `CodeWorker::Executable(path)` runs a `gate` binary installed at `path`.
- `CodeWorker::CurrentExe` runs the host's own executable with `--code-worker`. The host must
  then start by calling `gate::code::worker_main_if_requested()`, which runs the worker and
  exits when that flag is first. This is what the `gate` binary does.
- With neither, `execute_mcp_code` isn't offered.

## Alternatives considered

- **Building platform support into gate**, behind a cargo feature or not. That ties a public
  project to one platform's API and release schedule, and other users can't do the same.
- **Hooks as MCP servers** that expose `get_credentials` and `check_access` tools. This reuses
  gate's client code, but it mixes control traffic with tools agents can see, and makes hook
  authors implement MCP to answer two questions.
- **Dynamic plugins** (`.so` or WASM). A stable plugin ABI or WASM host is much more machinery
  than two small protocols, and the hooks aren't performance-sensitive enough to need it.
- **Hooks that see every tool call**, for argument-level policies. That's a different, much
  larger feature, and per-server access covers the cases that motivated this.

## Decisions

| Question | Decision |
|---|---|
| Make the new connection before closing the old one, for HTTP servers? | No. Close first for every transport. |
| Allow `${credential:…}` in `args`? | No. Wrap servers that need it in a script that reads the variable. |
| Tell `list_mcp_servers` how many servers are hidden? | No. It says something about the policy. |
| Policies that answer "read-only" and similar? | Deferred. The response format leaves room for it. |
| Keep an unexpired lease when a refresh fails transiently? | By default, yes. Providers that don't want this set `refreshFailure: "revoke"`. |
| Does a nonzero exit override a valid answer from a command hook? | Yes. It's always `transient`. |
| Support HTTP+SSE servers? | No. rmcp has no client for this deprecated transport. |

## Deferred

These were considered and left out of the first version on purpose:

- **Caching access answers.** If added, a cached answer must only be reused for the same
  trusted caller generation, the same policy config and the same set of servers, and must
  never be visible to another caller. gate has no trusted notion of the caller today, so this
  needs one first, supplied by the policy.
- **Passing `_meta` to policies.** It's caller-supplied, so it can only ever be context for
  the policy to verify, never identity. It's worth adding when gate serves several callers
  over HTTP.
- **`${file:PATH}` placeholders** for tokens that rotate while gate runs. Nothing needs them
  yet, and they make the token a request uses depend on when the file was read.
- **Long-lived command hooks** speaking JSON-RPC, to avoid starting a process per request.
  HTTP already covers hooks that must be fast.

## Plan

Each step can be released on its own:

1. **Groundwork.** Split the library from the binary and make the code worker configurable,
   add `hookEnv` and `inheritEnv`, and make an invalid server fail only itself instead of the
   whole config. Each of these is useful without hooks.
2. **Hook transports and access policies.** The smaller hook, which gets the shared transport
   code into use.
3. **Credential providers.** Outcomes, fingerprints, the server states above, attributing
   failures, capturing HTTP status at the transport boundary, built-in and lease detectors,
   `mergeEnv` and redaction.
4. **Public Rust API.** Document and stabilize the traits once both hooks have settled.

Tests use the same approach as today's integration tests: hook servers built with axum,
command hooks written as shell scripts, and echo MCP servers. They should cover at least:

- each outcome, from each transport, while connecting, refreshing and rotating
- the classification rules, in order:
  - `401`, `403`, `404` and `410` deny, whatever the body
  - `408`, `429`, `5xx` and timeouts are transient, whatever the body
  - a malformed `2xx` answer is transient
  - a nonzero exit is transient, even after a valid answer on stdout
- `refreshFailure: "keep"` rides out a transient failure until `expiresAt`, and `"revoke"` stops
  the server at once
- a healthy refresh never interrupts calls, even shortly before `expiresAt`
- a replacement lease that expires during the drain, or before its connection is ready, is
  replaced
- a credential rejected during initialization or tool listing is reported as
  `rejectedCredentialId` on the next lease request, and the same values aren't used again
- requests that arrive during a rotation wait for the new connection, then check access again
- running calls finish within `drainSeconds`, or are cancelled and reported as interrupted
- a late failure from an old credential doesn't invalidate the new one
- several failures for the same credential cause one rotation
- a lease with rejected values is refused, even under a new `credentialId`
- a refreshed lease's matchers replace the old ones
- a credential rejected by one server isn't refused for another
- 401s are detected:
  - with `WWW-Authenticate`
  - without it, with a plain body
  - without it, with a JSON-RPC error lacking `data.status`
  - each during initialization, tool listing and calls
  - from JSON-RPC errors with `data.status`, and from structured results
- secrets are redacted from stderr, including across chunks and during a rotation
- the policy is asked once per request plus the specified re-checks, never answers from an
  earlier request, and hides only its own servers when it fails
- scripts can't call servers outside their ceiling, and lose servers the policy stops allowing
- `hookEnv` variables never reach servers, and naming one in a server's config is an error

## Appendix A: how a hosted platform uses this

The motivating case is an agent platform that runs gate in each sandbox, Tembo's among them:

- Its API writes gate's config when the sandbox starts, with `params` carrying its own
  integration and connection IDs, a `hookEnv` with its tokens, and an `inheritEnv` list for
  its servers.
- Its credential endpoint implements the credentials protocol. The vendor-specific error
  patterns it knows for each MCP server become `authFailures` in its leases. It already
  answers `403` and `404` when access is revoked, which gate classifies as `denied`.
- Its access endpoint implements the access protocol. The sandbox's token stays the same for
  the life of the agent process, and the platform decides on its side, for every request,
  which message is active and who sent it. That's why gate asks for every request and never
  caches the answer.

None of this needs anything in gate that's specific to the platform.

## Appendix B: replacing Tembo's TypeScript gateway

This design gives gate what it needs, but switching Tembo's sandboxes from the TypeScript
gateway to gate also needs work outside gate. Some of it is compatibility work, and some can
be a deliberate change in behavior:

| Work | What's needed |
|---|---|
| Hook endpoints | Adapt the existing credential and access endpoints, or put adapters in front of them. Their request and response formats differ from the ones here. |
| Config and launch | Generate gate's config instead of the current manifest, and launch gate instead of the current gateway command |
| Credential formats | Turn each existing auth-error format into lease `authFailures`, and use `mergeEnv` for stdio servers whose leased variables vary |
| Refresh failures | Choose `refreshFailure`. `"revoke"` matches today's gateway, while `"keep"` would be a deliberate change. |
| Code mode | gate runs JavaScript and returns `logs`, while the current gateway runs TypeScript and returns `stdout` and `stderr`. Either gate strips TypeScript types, or the tool's contract and agent prompts change. |
| SSE servers | Move any remaining SSE servers to Streamable HTTP |
| Gateway identity | Configure the server name, instructions and tool descriptions agents see today |

Before switching, an acceptance suite should run the same scenarios against both gateways and
check that they agree, except where a difference was chosen on purpose:

- the active message changes between requests, and during a script
- a sandbox or connection is revoked while servers are connected and calls are running
- credentials are refreshed and rotated with calls running
- a credential is rejected while connecting, and again while calls are running
- the provider is throttled or unavailable during a refresh
- every existing auth-error format, with both matching and nearly matching fixtures
- what personal servers each caller sees, in every tool
- which variables each server gets
- code-mode scripts and their output
