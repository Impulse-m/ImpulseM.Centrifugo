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

A connection gets its class from the connection token at connect, and a refresh replaces it. The claim named by `rpc.registry.auth_class_claim` holds the class, with one of the values `pending_2fa`, `bot`, `authenticated` or `anonymous`. Each value must agree with the user of the connection: `anonymous` only without a user, `authenticated` and `bot` only with one, and `pending_2fa` with or without one. A claim with any other value, a claim which is not a string, or a value which does not agree with the user, makes the token invalid, so the connect or the refresh is rejected. A refresh with a full token upgrades a pending connection.

When `auth_class_claim` is empty, a connection is `anonymous` without a user and `authenticated` with one.

When `auth_class_claim` is set, issuers put the claim on every connection token which is not a full session, and leave it out of full sessions. A token without the claim is taken as a full session: `authenticated` with a user, `anonymous` without one. So a half-logged-in token must carry the claim.

A stored class is valid only under the claim it was derived under. After a reload which changes `auth_class_claim`, a connection which was connected under the old claim is `anonymous` for non-anonymous methods, until its next refresh derives the class again. A connection without a token, such as one made through a connect proxy, has no class: without a claim it gets the class of its user, and with a claim it is `anonymous`.

A method without `auth_class` takes `defaults.auth_class`. If that is empty or `anonymous`, every method must state its class, so a missing class never makes a method public. When the registry is enabled, a method or `defaults.auth_class` of class `pending_2fa` or `bot` needs `rpc.registry.auth_class_claim`, and `auth_class_claim` must be a name of the form `[a-zA-Z_]+`. The rule applies at config validation and on reload, and a reload which breaks it is rejected, keeping the current registry. A connection which does not satisfy the class gets `errors.auth_class`.

Metadata sent with each call. Later sources win:

1. static metadata of the target (`grpc.static_metadata`);
2. connect-time headers listed in the target's `emulated_headers`;
3. per-call headers listed in the method's `headers`;
4. values set by Centrifugo: `authorization: Bearer <token>` for non-anonymous methods when the connection has a token, `x-centrifugo-client` with the client ID, `x-centrifugo-msg-id` for calls with a message ID, and `x-centrifugo-retransmit: 1` and `x-centrifugo-without-updates: 1` when the client set them;
5. `x-forwarded-for` with the client address, as resolved by `client_address`. When the address is not resolved, no `x-forwarded-for` is sent, even if a source above carried one. Clients cannot set it: the name is reserved;
6. `traceparent` and `tracestate` from the trace context of the call.

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

## Exactly-once calls

Off by default. Turn it on with `rpc.registry.dedup.enabled`. It needs `rpc.registry.enabled` and `client.reply_include_server_time`, and a change of it, of `window` or of `max_skew` takes effect after a restart.

```json
"rpc": {"registry": {"dedup": {"enabled": true, "window": "120s", "max_skew": "30s", "max_reply_size": 2048, "local_max_entries": 200000, "local_max_bytes": 268435456, "local_anonymous_max_entries": 20000, "local_anonymous_max_bytes": 33554432, "scope_reserve": 4, "scope_reserve_bytes": 8192, "max_scopes_per_user": 8, "max_records_per_scope": 1024, "on_store_error": "fail_closed"}}}
```

* `window` is how long an answer is kept, and how far in the past a `msg_id` may be. `max_skew` is how far in the future it may be. `window` is at least 1s and greater than `max_skew`.
* A call with a `msg_id` is checked against the window. The `msg_id` is the Unix second shifted left by 20 bits, plus a sequence, so it fits in 53 bits. Outside the window the call answers `errors.msg_id_invalid`. The reply carries the server time, so the SDK corrects its clock and sends the call again. A `msg_id` of 0 is not deduplicated.
* Scope. A connection with a token session is scoped to its user and session. Otherwise it is scoped to its user and its install `instance`, which the client sends on connect. An instance must match `^[A-Za-z0-9_-]{22,64}$`, and any other value is dropped. A connection without a user is anonymous: it is scoped to its instance only, in the anonymous pool, and with `local_anonymous_max_entries: 0` it has no scope. A connection with neither has no scope: its calls are not deduplicated, and count as `no_scope`. Scope keys are hashed, so they do not show the instance.
* Only calls of non-idempotent methods are recorded. An idempotent method is executed again on retransmit, and is cancelled on disconnect.
* A recorded call keeps its answer, or its protocol error, for the window, on this node. A record counts 256 bytes plus its reply, or plus its error message, and at most `max_records_per_scope` records are kept for one scope. Records are spread over 64 shards: by user for an authenticated scope, and by scope for an anonymous one. Each shard holds its share of the budgets. A retransmit with the same method and data gets the answer with `cached: true`. A duplicate which arrives while the call runs waits for it, and gets the same answer as the retransmits do: a reply which is kept as uncached gets `errors.result_evicted` too. A duplicate with another method or other data gets `errors.msg_id_reused`, and neither joins the running call nor gets its answer. A cancel of a duplicate's msg_id, from any connection in the same scope, cancels the call of its owner, and the duplicate is answered with the owner's record. Cancel tombstones are records too.
* Budgets and the reserve. The node keeps at most `local_max_entries` records and `local_max_bytes` bytes, anonymous ones included. Every scope which holds records or running calls is admitted, and is guaranteed `scope_reserve` records, which no other scope can evict. Each record is kept up to `scope_reserve_bytes / scope_reserve` bytes, which is 2048 bytes at the defaults, so the records of a scope within its reserve fit `scope_reserve_bytes`. A reply over that share is kept as uncached, and a retransmit of it gets `errors.result_evicted`. `scope_reserve_bytes` must be at least 1024 bytes and `scope_reserve` times 256 bytes. Each shard admits at most its share of `local_max_entries / scope_reserve` scopes, and of `local_max_bytes / scope_reserve_bytes` scopes for the bytes, whichever is less, so the node admits 64 times that figure at most. Per pool, the authenticated scopes take `local_max_entries` and `local_max_bytes` less their anonymous parts, and the anonymous scopes take the anonymous parts. With the defaults the bytes limit binds: 448 authenticated scopes and 64 anonymous ones per shard, so 28672 and 4096 for the node. Admission is per shard: once one shard is full, the node refuses a new scope even below the node figure, and with random hashing the fullest shard fills a few percent before the average, so the node admits somewhat fewer scopes than the formula gives. Size the budgets as `scope_reserve` times the scopes which make a non-idempotent call within one window. Memory is used only by records which exist, so a larger budget costs nothing while the store is not full. Above its reserve a scope keeps records as long as there is room. When the budget is full, a new record first evicts the oldest record of its own scope while that scope is over its reserve, then the oldest record of the scope which has been over its reserve the longest. A scope is over its reserve when it holds more than `scope_reserve` records, so only such a scope gives up records to another scope: a scope within its reserve never loses a record because of another scope, whatever the bytes of its records. Beyond `scope_reserve` records, a scope's answers are best-effort: when the node is full, a scope within its reserve can evict them, so the reserve is the only guarantee a scope has. Admission does not protect a scope's surplus, since a scope within its reserve evicts surplus whichever scope holds it. An answer evicted this way is gone, so a retransmit of its call runs the call again, and the eviction is counted in `dedup_evictions_total`.
* Admission. A call of a scope which is not admitted is refused when its pool is at its scope limit, or when its user already has `max_scopes_per_user` scopes with records or calls on this node. A refused call under `on_store_error: fail_closed` answers the temporary `errors.dedup_unavailable` and is not executed. Under `fail_open` it runs without dedup. A cancel for a refused scope records nothing, and its command still answers the empty reply. The slot frees when the scope's records are acked or expire. Anonymous connections have their own pool of `local_anonymous_max_entries` and `local_anonymous_max_bytes`, so a flood of anonymous instances cannot refuse or evict an authenticated scope.
* Errors of the registry's own gates (unknown method, auth class, request size) are not recorded.
* A reply over `max_reply_size` is not kept, and neither is one over `scope_reserve_bytes / scope_reserve`, see Budgets and the reserve. A retransmit of it gets `errors.result_evicted`, and the call is not executed again.
* Cancel. The client sends a call with `cancel: <msg_id>` and no method. It cancels the call of that `msg_id` on the connection, and in its scope, and it is answered with an empty reply. The cancelled call answers `errors.cancelled`, and so does a later retransmit. A cancel which arrives before its call keeps a record, so the call never runs. Cancel commands do not wait for `client.concurrency`. A cancel whose `msg_id` is outside the window answers `errors.msg_id_invalid` and records nothing. With dedup off a cancel command is not special: it goes on as an ordinary call, and so is answered as an unknown method.
* Ack. A call lists in `ack` the `msg_id`s whose replies it received. Their records are removed.
* Mutations and disconnects. A non-idempotent call in a scope runs on a context which a disconnect does not cancel, so a backend commit completes, and its answer is kept. A disconnect which the status mapping makes of the answer is kept as a protocol error with its code and reason, so a retransmit gets it without a second backend call. A non-idempotent call without a scope, and an idempotent call, are cancelled by a disconnect.
* A store failure answers a tracked call with the temporary `errors.dedup_unavailable` under `on_store_error: fail_closed`, the default. Under `fail_open` the call runs without dedup. The same applies to a call of a new scope which the node cannot admit within its budgets, see Admission. Idempotent calls do not use the store.
* Records are node-local. A retransmit which reaches another node runs again.
* Secrecy. The instance is a bearer secret. It must be random, made by the application, of at least 128 bits (22 base64url characters), kept per installation or per tab, and never shared or logged. Whoever knows it can retransmit and cancel calls in its scope. The server never logs the instance or a scope.

| Error | Default `code` | `temporary` |
|---|---|---|
| `msg_id_invalid` | 107 | no |
| `msg_id_reused` | 107 | no |
| `result_evicted` | 110 | no |
| `dedup_unavailable` | 108 | yes |

## Metrics

* `centrifugo_rpc_registry_calls_total{rpc_method, result}`: calls by method and result (`ok`, `error`, `cancelled`, `timeout`, `denied`, `unknown`). A call to a name which is not in the registry counts as `unknown`.
* `centrifugo_rpc_registry_errors_total{rpc_method, app_type}`: failed calls with an application error type. Cancelled calls are not counted here.
* `centrifugo_rpc_registry_reloads_total{trigger, result}`: reloads of the registry. `trigger` is `signal` for a configuration reload on SIGHUP, and `poll` for a change of the registry file found by the poll. `result` is `ok` or `error`. A touch without a change of content is not a reload. A reload which fails, on a content or a dial error, counts as `error`. A file which cannot be read is logged and not counted.
* Per target, the proxy metrics with protocol `grpc` and type `rpc`: `centrifugo_proxy_duration_seconds`, `centrifugo_proxy_duration_seconds_histogram`, `centrifugo_proxy_errors` and `centrifugo_proxy_inflight_requests`. A call cancelled by the client is not observed in them, as the proxy handler does for a closed client; `centrifugo_proxy_inflight_requests` still counts it while it runs.
* `centrifugo_rpc_registry_dedup_total{outcome}`: exactly-once calls by outcome (`miss`, `hit_done`, `hit_inflight_local`, `evicted`, `cancelled`, `store_error`, `msg_id_invalid`, `msg_id_reused`, `no_scope`, `refused_pool`, `refused_user`). `cancelled` counts cancel commands. `refused_pool` is a new scope which its pool cannot admit, and `refused_user` a user at `max_scopes_per_user`; both count a call or a cancel command, and are not counted as `store_error`. Answers from a record are counted here and not in `calls_total`, which counts backend calls. No ID is in any label.
* `centrifugo_rpc_registry_dedup_evictions_total{reason}`: answers the node evicted to make room. `scope_cap` is the oldest answer of a scope over `max_records_per_scope`, `over_reserve` the oldest answer of the scope which is over its reserve while the budget is full, `surplus` the oldest answer of the scope which has been over its reserve the longest, evicted for another scope, and `uncached` a reply over its share of `scope_reserve_bytes / scope_reserve`, whose record is kept without it, and `reply_size` a reply over `max_reply_size`, whose record is kept without it when the call completes. A rise of `surplus` with `refused_*` is the signature of a flood. A rise of `uncached` means replies are larger than the share, so raise `scope_reserve_bytes` to keep them. A rise of `reply_size` means replies are larger than `max_reply_size`, so raise it, and `scope_reserve_bytes` too when they are over the share.
* `centrifugo_rpc_registry_dedup_entries`: answers the node keeps, of both pools.
* `centrifugo_rpc_registry_dedup_scopes{pool}`: scopes the node admits, by `pool` (`authenticated`, `anonymous`). Compare it with the limit of its pool, per shard: the smaller of its entries / `scope_reserve` and bytes / `scope_reserve_bytes`, times 64. The authenticated pool takes `local_max_entries` and `local_max_bytes` less their anonymous parts, and the anonymous pool takes the anonymous parts. Each shard is limited by its own share, see Budgets and the reserve.
* `centrifugo_rpc_registry_dedup_bytes{pool}`: bytes of the answers the node keeps, by `pool`.
* Suggested alerts: `rate(centrifugo_rpc_registry_dedup_total{outcome=~"refused_.*"}) > 0`, and per pool `centrifugo_rpc_registry_dedup_scopes / limit > 0.8`, where the authenticated limit is `64 * min((local_max_entries - local_anonymous_max_entries) / 64 / scope_reserve, (local_max_bytes - local_anonymous_max_bytes) / 64 / scope_reserve_bytes)` and the anonymous limit is `64 * min(local_anonymous_max_entries / 64 / scope_reserve, local_anonymous_max_bytes / 64 / scope_reserve_bytes)`. With the defaults these are 28672 and 4096.
* `centrifugo_rpc_registry_dedup_wait_seconds`: time a duplicate waited for the call it joined.
* `centrifugo_rpc_registry_retransmit_total{rpc_method}`: calls which the client sent again after a transport loss.
* The series are created when the registry is built. Each registry method exports one zero-valued `calls_total` series per result, so N methods export 6·N series. Each target exports its four proxy series, the same way. The `unknown` method label is the exception: its series are created on the first call to an unknown name.

## Planned

Not implemented yet:

* A way for the connect proxy to return the auth class, so that connections made through it can have a class when `auth_class_claim` is set.
* Restricting what a `pending_2fa` connection can do outside the registry. Such a connection still keeps its user for channels and personal subscriptions. Restricting that comes with the auth upgrade work.
* Shared exactly-once records across nodes. Until then a retransmit which reaches another node runs again.
* The `after` field, which orders a call after other `msg_id`s, and the wakeup of duplicates across nodes by notification.
