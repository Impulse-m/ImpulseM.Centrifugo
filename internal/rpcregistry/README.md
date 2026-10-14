# RPC method registry

The registry lets clients call existing gRPC methods of backend services through Centrifugo's RPC. A client calls a registry method name, and Centrifugo calls the gRPC method it maps to by full name, for example `/example.accounts.v1.AccountService/GetProfile`. There is no wrapper service: request and response bytes pass through unchanged, and the backend keeps its own protobuf contract. Without `rpc.registry.enabled` nothing changes.

```json
"rpc": {
  "registry": {
    "enabled": true,
    "file": "/etc/centrifugo/rpc_registry.yaml",
    "drain_timeout": "30s",
    "errors": {
      "unknown_method": {"code": 104, "message": "method not found", "app_code": 400, "app_type": "UNKNOWN_METHOD"},
      "auth_class": {"code": 103, "message": "permission denied", "app_code": 401, "app_type": "ACCESS_DENIED"},
      "request_too_large": {"code": 106, "message": "limit exceeded", "app_code": 400, "app_type": "REQUEST_TOO_LARGE"},
      "cancelled": {"code": 107, "message": "call cancelled", "app_code": 400, "app_type": "CALL_CANCELLED"}
    },
    "app_error": {"code_key": "app-code", "type_key": "app-type"},
    "grpc_status_to_code_transforms": []
  }
}
```

* `file`: path to the registry file, JSON or YAML, chosen by extension. Entries are read from this file only: combining `file` with inline `defaults`, `targets` or `methods` is an error.
* `poll_interval`: how often the registry file is checked for changes, for example `5s`. The default `0` turns polling off: the file is read at start and on a configuration reload (SIGHUP). A changed file is applied as a whole. If it fails to load or validate, the current registry stays active and the error is logged, and the same content is not tried again until it changes. If a target cannot be dialed, for example when its TLS files are being rotated, the error is logged and the poll tries the same content again on the next tick. The file is compared by its size, modification time and content, so a touch without a change does nothing. The check follows symlinks, so a symlink swap, as a Kubernetes ConfigMap update does, is seen. Replace the file atomically: write a temporary file in the same directory, then rename it over the registry file, as a ConfigMap update does. A file written in place can be read half-written, and then fail to load. A rewrite with the same size within the filesystem's mtime resolution can be missed, because the poll compares the size and modification time before it reads the content. The interval is read at start, so a change of `poll_interval` takes effect after a restart.
* `drain_timeout`: how long connections of removed or replaced targets stay open before they are closed, so calls in flight can finish. Default `30s`.
* `errors`: protocol errors returned for failures of the registry itself: unknown method, auth class mismatch, request too large, and a call cancelled by the client. Each has `code`, `message`, and optional `app_code` and `app_type`.
* `app_error`: names of the gRPC trailers which carry the application error code and type. Defaults are `app-code` and `app-type`.
* `grpc_status_to_code_transforms`: overrides for the default mapping of gRPC status codes to protocol errors, see [error mapping](#error-mapping).
* When the registry is enabled, the server advertises the RPC options capability (bit 32) in the connect reply, so SDKs can send per-call headers. Clients which do not ask for the capability are unaffected.

Registry file:

```yaml
version: 1                       # format version, must be 1
defaults:
  auth_class: authenticated      # used by methods which do not set auth_class
  timeout: 5s
targets:
  - name: accounts
    endpoint: dns:///accounts.example.internal:9000
    load_balancing: round_robin  # round_robin (default) or pick_first
    emulated_headers: [x-example-init]   # connect-time headers forwarded to this target
  - name: catalog
    endpoint: dns:///catalog.example.internal:9000
    timeout: 10s                 # target default, overrides defaults.timeout
    max_request_bytes: 1048576
methods:
  - method: account.getProfile                 # client RPC method name, unique
    grpc_method: /example.accounts.v1.AccountService/GetProfile
    target: accounts
    idempotent: true
  - method: account.signIn
    grpc_method: /example.accounts.v1.AccountService/SignIn
    target: accounts
    auth_class: anonymous
    error_metadata: [trace-ref]  # backend trailers copied into the protocol error metadata
  - method: account.confirmCode
    grpc_method: /example.accounts.v1.AccountService/ConfirmCode
    target: accounts
    auth_class: pending_2fa
  - method: catalog.search
    grpc_method: /example.catalog.v1.CatalogService/Search
    target: catalog
    auth_class: authenticated
    idempotent: true
    headers: [x-example-region]  # per-call client headers forwarded to the target
```

The same tree works as JSON. Each method and target field left unset inherits from the target, then from `defaults`, then from the built-in values: `timeout` 5s, `max_request_bytes` 64 KiB, `max_response_bytes` 1 MiB, `idempotent` false. Method names match `^[A-Za-z0-9_.:/-]{1,128}$`, gRPC method paths match `^/[\w.]+/\w+$`. Header allow-lists are lower-case. `authorization`, `traceparent`, `tracestate`, `x-forwarded-for` and names with the `x-centrifugo-` prefix are reserved and rejected in both allow-lists.

Auth classes say which connections can call a method. Every connection has one class:

* `anonymous`: a connection without a user ID.
* `pending_2fa`: a connection whose user has not finished signing in.
* `authenticated`: a connection with a user ID.
* `bot`: a connection of a bot account, with a user ID.

A method admits a connection by its class:

| Method class | Admitted connection classes |
|---|---|
| `anonymous` | all |
| `pending_2fa` | `pending_2fa`, `authenticated`, `bot` |
| `authenticated` | `authenticated`, `bot` |
| `bot` | `bot` |

The `anonymous` method class is open to everyone, and its connection token is never forwarded, even when the connection is authenticated. Other methods forward the connection token.

A connection gets its class from the connection token at connect, and a refresh replaces it. The claim named by `rpc.registry.auth_class_claim` holds the class, with one of the values `pending_2fa`, `bot`, `authenticated` or `anonymous`. Each value must agree with the user of the connection: `anonymous` only without a user, `authenticated` and `bot` only with one, and `pending_2fa` with or without one. A claim with any other value, a claim which is not a string, or a value which does not agree with the user, makes the token invalid, so the connect or the refresh is rejected. A refresh with a full token upgrades a pending connection in place, see [Connection identity](#connection-identity).

When `auth_class_claim` is empty, a connection is `anonymous` without a user and `authenticated` with one.

When `auth_class_claim` is set, issuers put the claim on every connection token which is not a full session, and leave it out of full sessions. A token without the claim is taken as a full session: `authenticated` with a user, `anonymous` without one. So a half-logged-in token must carry the claim.

A stored class is valid only under the claim it was derived under. After a reload which changes `auth_class_claim`, a connection which was connected under the old claim is `anonymous` for non-anonymous methods, until its next refresh derives the class again. A connection without a token, such as one made through a connect proxy, has no class: without a claim it gets the class of its user, and with a claim it is `anonymous`.

A method without `auth_class` takes `defaults.auth_class`. If that is empty or `anonymous`, every method must state its class, so a missing class never makes a method public. When the registry is enabled, a method or `defaults.auth_class` of class `pending_2fa` or `bot` needs `rpc.registry.auth_class_claim`, and `auth_class_claim` must be a name of the form `[a-zA-Z_]+`. The rule applies at config validation and on reload, and a reload which breaks it is rejected, keeping the current registry. A connection which does not satisfy the class gets `errors.auth_class`.

Metadata sent with each call. Later sources win:

1. static metadata of the target (`grpc.static_metadata`);
2. connect-time headers listed in the target's `emulated_headers`;
3. per-call headers listed in the method's `headers`;
4. values set by Centrifugo: `authorization: Bearer <token>` for non-anonymous methods when the connection has a token, `x-centrifugo-client` with the client ID, and `x-centrifugo-msg-id` for calls with a message ID;
5. `x-forwarded-for` with the client address, as resolved by `client_address`. When the address is not resolved, no `x-forwarded-for` is sent, even if a source above carried one. Clients cannot set it: the name is reserved;
6. `traceparent` and `tracestate` from the trace context of the call.

## Connection identity

A connection keeps its user and its auth class across refreshes. Two parts decide what a refresh may change: the pending connection, and the rebind.

### Pending connections are anonymous to the core

A connection whose class is `pending_2fa` has a user in its token, which the registry uses, but the core sees no user for it:

* it gets no personal channel, no user-limited channels and no entry in the user index of the node, so calls which address a user do not reach it;
* the user is still reported in the connect and refresh results, and the token is still forwarded to `pending_2fa` methods;
* it is revocable by session: revoking its session disconnects it at once, with or without the user argument. The node keeps an index of connections without a user which carry a token session, and a connection leaves it when it gets a user or disconnects. The index also holds anonymous connections with a session;
* connection limits and the per-user connect limit do not count pending connections, and logs and the connections API show no user for them.

### Rebind on refresh

`client.rebind.enabled` lets a refresh move a live connection to another identity without a reconnect. It is off by default: a refresh whose token has another user disconnects the connection, as it always did. `client.rebind.allow_downgrade` is read only when rebind is enabled.

```json
"client": {
  "rebind": {
    "enabled": true,
    "allow_downgrade": false
  }
}
```

The transitions of the auth class of a connection on refresh, when the user of the token differs from the user of the connection:

| From | To | Result |
|---|---|---|
| `anonymous` | `authenticated`, `bot` | rebind to the user |
| `anonymous` | `pending_2fa` | rebind; the connection stays anonymous to the core and keeps the token user for the registry |
| `pending_2fa` | `authenticated` | rebind to the user |
| `pending_2fa` | `anonymous` | rebind to anonymous, dropping the channels of the token and of the user |
| `authenticated`, `bot` | `anonymous` | with `allow_downgrade`: rebind to anonymous; otherwise disconnect |
| any other | | refresh error `permission denied`, the class is kept |

A connection with a user but no class stored under the current `auth_class_claim`, such as one made through a connect proxy, may not rebind: a refresh which would change its identity gets the same permission error.
A refresh whose token user is the user the connection already has, and which keeps the user Core sees, takes the refresh path as before. Any other change of the token user takes the rebind path: with rebind off it disconnects, as it did before, and with rebind on the table decides. So `anonymous` to `pending_2fa` of a user, and `pending_2fa` of one user to `pending_2fa` of another, are routed by the table even though Core sees no user change.

On a rebind to a user the connection gets its personal channel and the channels of its token, and the single connection of the user is enforced when the personal channel is configured so. Its user rate limit state moves to the new user. A refresh which rebinds does not disconnect the connection for changed token channels.

On a downgrade to anonymous the connection loses its personal channel, its user-limited channels, and the channels of token and proxy sources. Client channels stay when their namespace allows anonymous subscriptions.

Access checks run before the rebind: a blocked user disconnects the connection, a revoked session or invalidated token gets a refresh error `token expired`. A revocation applied between the check and the rebind disconnects the connection, because the connection is checked again after it moves. A user connection limit reached by the new user disconnects with the connection limit disconnect, a refresh of an expired token gets `token expired`, and a connection closed meanwhile is not changed.

`client.rebind.enabled` cannot be combined with `client.proxy.refresh.enabled`: the refresh proxy answers refreshes without the token user, so the config is rejected at startup.

### Reply fields

The connect result and the refresh result carry two fields:

* `user`: the user of the token, which is also the user of a pending connection. Empty for an anonymous token;
* `auth_class`: the class name (`anonymous`, `pending_2fa`, `authenticated` or `bot`). Empty when the registry is disabled.

### Known gaps

* A refresh with the same user whose class changes, such as `authenticated` to `bot`, takes the refresh path without the transition table. The token is still verified, and the class is taken from it.
* An account switch between two users is denied: a refresh from one `authenticated` user to another gets a permission error. The client must log out, and then connect.
* An upgrade through rebind does not consume the per-user connect rate limit. Core's per-user connection limit still applies.

## Error mapping

A failed gRPC call becomes a protocol error with the default mapping below. The `code` is the Centrifugo protocol error code.

| gRPC status | `code` | `temporary` |
|---|---|---|
| `UNAUTHENTICATED` | 101 | no |
| `PERMISSION_DENIED` | 103 | no |
| `RESOURCE_EXHAUSTED` | 111 | yes |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | 100 | yes |
| `INTERNAL`, `UNKNOWN`, `UNIMPLEMENTED`, `DATA_LOSS` | 100 | no |
| `CANCELLED` (client cancelled the call) | `errors.cancelled` | no |
| `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `NOT_FOUND`, `ALREADY_EXISTS`, `ABORTED`, `OUT_OF_RANGE` | 400 | no |

* A `CANCELLED` status which the backend sends on a live connection is not a client cancel. It maps like `INTERNAL`, to code 100.
* `grpc_status_to_code_transforms` replaces the row of a status. Each rule has `status` (a gRPC status name, exact match) and `to_error` or `to_disconnect`, the same as `status_to_code_transforms` for HTTP proxies:

  ```json
  "grpc_status_to_code_transforms": [
    {"status": "UNAUTHENTICATED", "to_error": {"code": 150, "message": "sign in again"}}
  ]
  ```
* With the default mapping, the protocol error `message` is the gRPC status message, cut to 256 bytes.
* The application error code comes from the `app_error.code_key` trailer as a decimal `int32`. Without it, the code is 401 for `UNAUTHENTICATED`, 403 for `PERMISSION_DENIED`, 420 for `RESOURCE_EXHAUSTED`, 400 for `INVALID_ARGUMENT`, and 500 otherwise.
* The application error type comes from the `app_error.type_key` trailer and must match `^[A-Z0-9_]{1,64}$`. Without a valid trailer, the status message is used when it matches that pattern.
* Protocol error `metadata` holds only the trailers listed in the method's `error_metadata`, nothing else.

Check the registry in CI with the usual config check. It validates the registry file too, when `rpc.registry.enabled` is true:

```bash
centrifugo checkconfig -c config.json
```

## Metrics

* `centrifugo_rpc_registry_calls_total{rpc_method, result}`: calls by method and result (`ok`, `error`, `cancelled`, `timeout`, `denied`, `unknown`). A call to a name which is not in the registry counts as `unknown`.
* `centrifugo_rpc_registry_errors_total{rpc_method, app_type}`: failed calls with an application error type. Cancelled calls are not counted here.
* `centrifugo_rpc_registry_reloads_total{trigger, result}`: reloads of the registry. `trigger` is `signal` for a configuration reload on SIGHUP, and `poll` for a change of the registry file found by the poll. `result` is `ok` or `error`. A touch without a change of content is not a reload. A reload which fails, on a content or a dial error, counts as `error`. A file which cannot be read is logged and not counted.
* Per target, the proxy metrics with protocol `grpc` and type `rpc`: `centrifugo_proxy_duration_seconds`, `centrifugo_proxy_duration_seconds_histogram`, `centrifugo_proxy_errors` and `centrifugo_proxy_inflight_requests`. A call cancelled by the client is not observed in them, as the proxy handler does for a closed client; `centrifugo_proxy_inflight_requests` still counts it while it runs.
* The series are created when the registry is built. Each registry method exports one zero-valued `calls_total` series per result, so N methods export 6·N series. Each target exports its four proxy series, the same way. The `unknown` method label is the exception: its series are created on the first call to an unknown name.

## Planned

Not implemented yet:

* A way for the connect proxy to return the auth class, so that connections made through it can have a class when `auth_class_claim` is set.
* The `x-centrifugo-retransmit` and `x-centrifugo-without-updates` headers.
* Cancelling an in-flight call by its `msg_id`.
* Deduplication of calls with a message ID. `idempotent` is accepted and reserved for it. It has no effect yet.
