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
* `errors`: protocol errors returned for failures of the registry itself: unknown method, auth class mismatch, request too large, response too large, and a call cancelled by the client. Each has `code`, `message`, and optional `app_code` and `app_type`.
* `app_error`: names of the gRPC trailers which carry the application error code and type. Defaults are `app-code` and `app-type`.
* `grpc_status_to_code_transforms`: overrides for the default mapping of gRPC status codes to protocol errors, see [error mapping](#error-mapping).
* `max_inflight_bytes`: the node-wide budget of registry calls in flight, in bytes. Default `0`, no budget. A call costs its request bytes plus its response bound, and a call which would take the budget over the limit is refused, see [Admission](#admission). Read at start.
* `memory_watermark_bytes`: the resident memory above which new registry calls are refused. Default `0`, off. Read at start, see [Admission](#admission).
* `errors.overloaded`: the temporary error of a refused call. Default code and message are those of `ErrorTooManyRequests`, `111` and `too many requests`. Its metadata carries `x-centrifugo-retry: later`.
* `errors.response_too_large`: the error of a reply over the response bound of its call. Unset, the reply gets `errors.request_too_large`, as before. The call ran, so the error is not temporary, carries no retry signal, and is kept as the answer of a recorded call. A grpc-go target which refuses the request with its own receive limit still gets `errors.request_too_large`, and so does a reply whose reported size equals the request size, since it is the target's refusal of the request. A target with `grpc.compression` reports a compressed size which the request size cannot match, so its oversized replies also get `errors.request_too_large`. Set `response_too_large` for a target without compression to tell the two apart.
* `refuse_expired_tokens`: off by default. When on, a call of a method which is not anonymous is refused before it runs when the token its connection would forward has a JWT `exp` at or before the node's clock. The client gets `errors.token_expired`, a temporary error with `x-centrifugo-retry: later` whose default code is 109 and message `token expired`. The call is not executed and not recorded. The switch needs `client.refresh_lead` above zero, and is refused at start with `client.insecure` or `client.proxy.refresh.enabled`, since those never renew the token. It checks the node's clock only: a backend clock ahead of the node is covered by `client.refresh_lead`. Read per call, see [Token expiry](#token-expiry).
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

Target and method fields for routing and sizes, all off unless set:

* `load_balancing: ring_hash` with `affinity_header: <name>` picks the replica of a target by a consistent hash of the value of the per-call header `<name>`. The gRPC `ring_hash` policy does the hashing. A call without the header gets a random replica. `affinity_header` is refused with any other policy, and `ring_hash` needs it. The name is lower-case, is not a reserved name, does not end in `-bin`, and at least one method of the target must list it in `headers`, so the header reaches the backend. The server never parses the value: the backend which issues it validates it. `GRPC_EXPERIMENTAL_RING_HASH_SET_REQUEST_HASH_KEY=false` in the environment makes a target with `ring_hash` fail the config check, since gRPC would hash at random.
* `response_size_hint: {request_field: N, overhead_bytes: 1024}` bounds the reply of a call by the number the request carries in field `N`, a varint field, plus `overhead_bytes`, so a small read reserves a small budget. The bound is the call's receive limit, so a reply over it fails as a reply over `max_response_bytes` does. A request without the field, with a zero value, or with a value at or above `max_response_bytes` minus the overhead, gets `max_response_bytes`. Zero `overhead_bytes` means 1024. The scan reads at most 256 top-level fields of the request.

Patterns for the answers of a backend which the client acts on, both through `error_metadata`:

* A move is an error whose trailer names the place which has the data, for example `x-example-relocate: eu-2`. The client's pool reroutes the call on it. The server does nothing special: it copies the listed trailer into the error metadata.
* A redirect is a reply or an error which names a URL, for example a presigned one in `x-example-location`. The client fetches the bytes itself. The server never proxies the URL.

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

`rpc.registry.pending_2fa_max_lifetime` caps how long each connection token of class `pending_2fa` may live. It is off by default (`0`), and a duration such as `5m` turns it on. The lifetime of a token is its `exp` minus the earlier of its `iat` and the time of the check, so an `iat` in the future does not shorten it. A token without `exp` has no end, and a token without `iat`, or with an `iat` of `0` or below, has no issue time to measure from, so each of them is over any limit. An issuer whose clock runs ahead of the server issues an `iat` later than the server's time, which counts toward the lifetime, so it must issue pending tokens shorter than the limit by its clock skew. A pending token over the limit is refused at connect and at refresh with the invalid token disconnect. The limit covers the tokens the server verifies, not the refreshes which a refresh proxy answers. The limit bounds each token, not the connection: a connection stays as long as its issuer keeps refreshing it with tokens under the limit. Under `client.insecure` a connection gets no expiry, so the limit is checked at connect and at each refresh the client sends, and no later check runs: a connection with a pending token stays open until it is closed or refreshed.

A stored class is valid only under the claim it was derived under. After a reload which changes `auth_class_claim`, a connection which was connected under the old claim is `anonymous` for non-anonymous methods, until its next refresh derives the class again. A connection without a token, such as one made through a connect proxy, has no class: without a claim it gets the class of its user, and with a claim it is `anonymous`.

A method without `auth_class` takes `defaults.auth_class`. If that is empty or `anonymous`, every method must state its class, so a missing class never makes a method public. When the registry is enabled, a method or `defaults.auth_class` of class `pending_2fa` or `bot` needs `rpc.registry.auth_class_claim`, and `auth_class_claim` must be a name of the form `[a-zA-Z_]+`. The rule applies at config validation and on reload, and a reload which breaks it is rejected, keeping the current registry. A connection which does not satisfy the class gets `errors.auth_class`.

Metadata sent with each call. Later sources win:

1. static metadata of the target (`grpc.static_metadata`);
2. connect-time headers listed in the target's `emulated_headers`;
3. per-call headers listed in the method's `headers`;
4. values set by Centrifugo: `authorization: Bearer <token>` for non-anonymous methods when the connection has a token, `x-centrifugo-client` with the client ID, `x-centrifugo-msg-id` for calls with a message ID, and `x-centrifugo-retransmit: 1` and `x-centrifugo-without-updates: 1` when the client set them;
5. `x-forwarded-for` with the client address, as resolved by `client_address`. When the address is not resolved, no `x-forwarded-for` is sent, even if a source above carried one. Clients cannot set it: the name is reserved;
6. `traceparent` and `tracestate` from the trace context of the call.

## Connection identity

A connection keeps its user and its auth class across refreshes. Two parts decide what a refresh may change: the pending connection, and the rebind.

### Pending connections are anonymous to the core

A connection whose class is `pending_2fa` has a user in its token, which the registry uses, but the core sees no user for it:

* it gets no personal channel, no user-limited channels and no entry in the user index of the node, so calls which address a user do not reach it;
* the user is still reported in the connect and refresh results (see reply fields), and the token is still forwarded to `pending_2fa` methods;
* a subscription to a private channel, and a subscription refresh, check the token user the connection holds, so a pending connection subscribes with a token of its own user. A token of another user is refused;
* it is revocable by session: revoking its session disconnects it at once, with or without the user argument. The node keeps an index of the connections with no user which hold a token session, from connect or from the refresh which gave it, so a revocation of their session disconnects them at once. A connection leaves the index when it gets a user, loses its session or disconnects;
* a user block, a user-level token invalidation and the disconnect API by user do not fan out to it, as they reach connections through the user index of the node and a pending connection is not in it. Every check of the connection (connect, refresh, recheck and the sweep) uses the token user the connection holds, so a blocked user's pending connection falls at its next refresh or sweep. The lifetime of the pending token bounds that window, and the issuer sets it;
* connection limits and the per-user connect limit do not count pending connections, and logs and the connections API show no user for them;
* `client.channel_limit` applies per connection to every class, pending included: a pending connection may hold no more channels than any other connection;
* it carries no connection info: the info of its token is not sent to the core, and an upgrade sets the info of its new token. The `subs` of a pending token are applied as the issuer wrote them, as for any token: the issuer trusted them, and a user channel in them is a plain server-side subscription with no user to bind to.

### Rebind on refresh

`client.rebind.enabled` lets a refresh move a live connection to another identity without a reconnect. It is off by default: a refresh whose token has another user disconnects the connection, as it always did. `client.rebind.allow_downgrade` is read only when rebind is enabled. With rebind enabled, `allow_downgrade` cannot be combined with `client.concurrency` above 1: startup and a reload refuse such a config. `client.concurrency` is read at start, so a reload does not change the concurrency of the running connections, and a downgrade is refused while the server runs with a concurrency above one, whatever the reloaded config says. Above one command at a time, a refresh to an anonymous token disconnects every connection which holds a user, a pending one included, whatever `allow_downgrade` says. With one command at a time, a pending connection refreshes to anonymous as the transition table allows, without `allow_downgrade`, so the same refresh keeps it at concurrency 1 and disconnects it above one: the table does not show that difference. With one command at a time per connection, a client subscription cannot commit while a downgrade runs. With more, a client subscription could commit between the two passes which drop the channels of the user, and stay on the logged-out connection until its next refresh or disconnect. A server API subscription is not a client command, so the concurrency setting does not affect it: see the known gaps.

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
| `pending_2fa` | `authenticated` | rebind to the same user; a token of another user is denied |
| `pending_2fa` | `anonymous` | rebind to anonymous, dropping the channels of the token and of the user |
| `authenticated`, `bot` | `anonymous` | with `allow_downgrade`: rebind to anonymous; otherwise disconnect |
| any other | | refresh error `permission denied`, the class is kept |

A connection made through a connect proxy never rebinds, with or without a user: any change of its identity gets the permission error. Transitions and the account of a connection are proven by tokens, and the channels a proxy granted were granted under the proxy's terms. The origin of a connection does not change: a refresh with a token of the same user does not make a proxy connection a token connection. A connection made without a token and without a proxy, which is anonymous, may rebind to a user.

A refresh whose token user is the user the connection already has, and which keeps the user Core sees, takes the refresh path as before. Any other change of the token user takes the rebind path: with rebind off it disconnects, as it did before, and with rebind on the table decides. So `anonymous` to `pending_2fa` of a user, and `pending_2fa` of one user to `pending_2fa` of another, are routed by the table even though Core sees no user change.

On the refresh path a change between `authenticated` and `bot` for the same user gets the permission error, as it is none of the transitions above: the class of a connection changes only through the table. A connection made through a connect proxy holds the class of its user, so a token of that user with another class gets the same error.

The account of a connection is the user of its last token with a user. A logout keeps it, so a connection which logged out may log in again only to its account, and a refresh to a token of another user gets the permission error.

On a rebind to a user the connection gets its personal channel and the channels of its token, and the single connection of the user is enforced when the personal channel is configured so. On a rebind to `pending_2fa` or `anonymous` the connection gets the channels of its token, and no personal channel. Its user rate limit state moves to the new user. An upgrade to a user takes the per-user connect rate limit of that user, the same bucket a connect of the user takes: a refused take gets a refresh error `too many requests`, and the connection keeps its identity. The take is not refunded when Core then refuses the rebind, for a user at its connection limit or a connection closed meanwhile: the bucket of the user loses one connect, and a client which retries such an upgrade drains the bucket for the connects of that user. A refresh which rebinds does not disconnect the connection for changed token channels.

On a downgrade to anonymous the connection loses its personal channel, its user-limited channels, and every other channel which is not a client channel: the channels of tokens, of the server API and of proxies. Client channels stay when their namespace allows anonymous subscriptions.

Access checks run before the rebind: a blocked user disconnects the connection, a revoked session or invalidated token gets a refresh error `token expired`. A revocation applied between the check and the rebind disconnects the connection, because the connection is checked again after it moves. A rebind which Core refuses keeps the old identity, which is checked again: a revocation of the old session applied in the meantime disconnects the connection. A user connection limit reached by the new user gets a refresh error `too many requests`, and the connection keeps its identity. A refresh of an expired token gets `token expired`, and a connection closed meanwhile is not changed.

`client.rebind.enabled` cannot be combined with `client.proxy.refresh.enabled`: the refresh proxy answers refreshes without the token user, so the config is rejected at startup.

### Reply fields

When `client.rebind.enabled`, the registry or `auth_class_claim` is on, the connect result and the refresh result carry two fields. Without these features neither field is sent, as before:

* `user`: the user of the token, which is also the user of a pending connection. Empty for an anonymous token;
* `auth_class`: the class name (`anonymous`, `pending_2fa`, `authenticated` or `bot`). Empty when the registry is disabled.

### Cost of the record

A connection gets a record only when a feature needs one: rebind, the registry, the auth class claim, or a user or client rate limit. With the access revocation store set, which the defaults do, a connection with no user whose token carries a session gets a record at connect, anonymous ones included, and is indexed by that session from connect on. A connection with no user and no session at connect gets none: the refresh which gives it a session attaches a 32-byte index membership and a disconnect hook to it, nothing else, so a revocation of that session reaches it through the index. With rebind, the registry, the auth class claim and the rate limiters off, a connection with no session allocates nothing for the record, with or without a user.

### Token exchange over the connection

An SDK can obtain its connection token over the connection it has just opened, instead of receiving it from the app before connect. The app keeps a long-lived refresh token, and the SDK exchanges it for a short access token and renews that before it expires. The server needs three settings for this, all off by default:

```json
"client": {
  "allow_anonymous_connect_without_token": true,
  "rebind": {
    "enabled": true
  }
}
```

* `client.allow_anonymous_connect_without_token: true` accepts a connection without a token, so the SDK can connect before it holds an access token.
* `client.rebind.enabled: true` lets a refresh change the user of the connection. The connection stays anonymous until the exchange gives it a user, and the transitions of the table above decide the refresh.
* The exchange is a registry method with `auth_class: anonymous`. Its calls never carry the connection token, so the issuer authenticates the call by the refresh token in the request alone.

```yaml
methods:
  - method: session.exchange                       # client RPC method name
    grpc_method: /example.auth.v1.SessionService/Exchange
    target: auth
    auth_class: anonymous
```

The request and the reply of the exchange are bytes the issuer defines: the server forwards them as any registry call does and does not read them. The access token the issuer returns is an ordinary connection token, verified by `client.token` like any other. Its `exp` drives the SDK's renewal, and a refresh with it rebinds the connection to the user of the token, with its personal channel and the channels of the token.

The rotation of the refresh token and the grace the issuer keeps for the previous one belong to the issuer. The server does not store or check refresh tokens.

The issuer keeps accepting a consumed refresh token for a grace, and answers it with the same rotated refresh token and a fresh access token, so a client whose reply was lost, or which lost a race with another client of the same installation, recovers by presenting the token it holds again. The issuer publishes the grace in seconds in its exchange reply; the SDKs retry a refused token until it has passed and declare the session dead only then (the SDK's grace setting, else 30 s, when the issuer publishes none). An issuer without a grace makes a lost reply a dead session. Size the issuer for the renewals: they mint at connections / (0.8 x ttl) per second, and each mint is a rotation write.

The issuer refuses a refresh token with any gRPC status and the `app-type` trailer (`rpc.registry.app_error.type_key`), for example `REFRESH_TOKEN_REFUSED`. The SDKs classify the refusal by that name, from a list the app configures, never by the protocol code or the application code. No `grpc_status_to_code_transforms` entry is needed.

The `meta` claim of the access token is the connection's attribution: it is stored at connect, replaced at a refresh or rebind whose token carries the claim (a token without it keeps the attribution the connection has, as upstream's refresh does; an issuer that wants to drop it sends an empty object), forwarded to proxies with `include_connection_meta`, and read by backends from the `authorization: Bearer` token of non-anonymous calls. The exchange call is anonymous, so its attribution is in the request the issuer defined.

### Known gaps

* A server API subscription which commits while a downgrade to anonymous runs can stay on the connection until its next refresh or disconnect. The channels of the user are dropped before the identity moves and again after it moves, so only a subscription which commits after the second pass is left. A client command cannot do this with one command at a time per connection, but a server API subscription does not go through that queue.
* A token whose expiration passes in the milliseconds between the check of the refresh and the rebind of the core gets the refresh error `token expired` after the user channels have gone. The refresh checks the expiration before the channels move, so this needs the token to expire in that window. Its personal channel comes back on the next refresh with the same user. The channels the client subscribed itself stay unsubscribed until the client subscribes them again or reconnects.
* `revoke_token` without `user` does not reach a connection which has a user. Pass `user`, or the connection is refused at its next refresh.
* A server API subscription which commits between the channel limit check of a rebind and the subscribe of its channels can take the connection to `client.channel_limit`. The limit error of that subscribe then ends the connection. A client command cannot do this with one command at a time per connection. With `client.concurrency` above one, a client subscription can do the same, and the connection reconnects. The rebind precheck and Core both use the `client.channel_limit` the node started with: a reload does not change it for the running connections.
## Token expiry

`refuse_expired_tokens` and `client.refresh_lead` work together:

* The switch checks the JWT `exp` of the token the connection forwards, which is the claim the backend checks. It does not check `expire_at`. A token without `exp` is never refused, and a token with `expire_at` later than its `exp` is refused from `exp` until the client refreshes, while one with an earlier `expire_at` is forwarded until `exp`. A backend which checks `exp` must therefore issue tokens without `expire_at`, or with `expire_at` no later than `exp`.
* The switch checks at the start of a call, so a call admitted just before `exp` can still reach the backend after it.
* The lead moves the refresh before `exp` for a token without `expire_at`, or with `expire_at` no later than `exp`. Pick a lead above the backend's clock skew plus the 99th percentile of the client's token fetch, so the refresh lands before the calls are refused.
* A refresh proxy, a server API `refresh` with `expire_at`, and `client.insecure` never renew the forwarded token, so a backend that checks `exp` needs client-side refresh.
* A connection made while the registry is off holds no token, so it is covered from its next connect while the registry is on. A reload which enables the registry covers only the connections made after the reload, and an older connection forwards no token, so the switch has nothing to refuse for it.
* The `exp` of the forwarded token is kept with the token whenever the registry is on, 8 bytes per connection, whatever the switch.
* The SDK resends a tracked call refused with `later` up to 3 times, after delays drawn from [0, 250] ms, [0, 500] ms and [0, 1000] ms: about 1.75 s in total. A refresh that lands after that leaves the call failed with 109, so the lead must cover the token fetch and the round trip.
* Clocks should be NTP-synced: the switch and the lead both compare with the node's clock.

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
* `grpc_status_to_code_transforms` replaces the row of a status. Each rule has `status` (a gRPC status name, exact match) and `to_error` or `to_disconnect`, the same as `status_to_code_transforms` for HTTP proxies. `to_error` also takes `app_code` and `app_type`, which fill the application fields the backend does not send. They need `to_error.code`, and `app_type` must match `^[A-Z0-9_]{1,64}$`:

  ```json
  "grpc_status_to_code_transforms": [
    {"status": "UNAUTHENTICATED", "to_error": {"code": 150, "message": "sign in again"}},
    {"status": "UNAVAILABLE", "to_error": {"code": 100, "message": "backend unavailable", "temporary": true, "app_code": 503, "app_type": "BACKEND_UNAVAILABLE"}}
  ]
  ```
* With the default mapping, the protocol error `message` is the gRPC status message, cut to 256 bytes.
* The application error code comes from the `app_error.code_key` trailer as a decimal `int32`. Without a valid trailer, a transform's `app_code` applies when it is set. Otherwise the code is 401 for `UNAUTHENTICATED`, 403 for `PERMISSION_DENIED`, 420 for `RESOURCE_EXHAUSTED`, 400 for `INVALID_ARGUMENT`, and 500 otherwise.
* The application error type comes from the `app_error.type_key` trailer and must match `^[A-Z0-9_]{1,64}$`. Without a valid trailer, the status message is used when it matches that pattern, and then a transform's `app_type` applies. A transport fault has a lower-case message, so it takes the rule's type. The rule fills each field separately: a valid trailer always wins, and the rule fills only what the backend did not send.
* Protocol error `metadata` holds only the trailers listed in the method's `error_metadata`, and the `x-centrifugo-retry` key the server sets itself, see [Exactly-once calls](#exactly-once-calls). A listed `x-centrifugo-retry` trailer is dropped: a backend cannot set the key.

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
* A recorded call keeps its answer, or its protocol error, for the window, on this node. A record counts 256 bytes plus its reply, or plus the encoded form of its error, and at most `max_records_per_scope` records are kept for one scope. Records are spread over 64 shards: by user for an authenticated scope, and by scope for an anonymous one. Each shard holds its share of the budgets. A retransmit with the same method and data gets the answer with `cached: true`. A duplicate which arrives while the call runs waits for it, and gets the same answer as the retransmits do: a reply which is kept as uncached gets `errors.result_evicted` too. A duplicate with another method or other data gets `errors.msg_id_reused`, and neither joins the running call nor gets its answer. A cancel of a duplicate's msg_id, from any connection in the same scope, cancels the call of its owner, and the duplicate is answered with the owner's record. Cancel tombstones are records too.
* Budgets and the reserve. The node keeps at most `local_max_entries` records and `local_max_bytes` bytes, anonymous ones included. Every scope which holds records or running calls is admitted, and is guaranteed `scope_reserve` records, which no other scope can evict. Each record, its 256 bytes of overhead included, is kept up to `scope_reserve_bytes / scope_reserve` bytes, which is 2048 bytes at the defaults, so the records of a scope within its reserve fit `scope_reserve_bytes`. A reply over that share less the overhead, which is 1792 bytes at the defaults, is kept as uncached, and a retransmit of it gets `errors.result_evicted`. `scope_reserve_bytes` must be at least 1024 bytes and `scope_reserve` times 256 bytes. Each shard admits at most its share of `local_max_entries / scope_reserve` scopes, and of `local_max_bytes / scope_reserve_bytes` scopes for the bytes, whichever is less, so the node admits 64 times that figure at most. Per pool, the authenticated scopes take `local_max_entries` and `local_max_bytes` less their anonymous parts, and the anonymous scopes take the anonymous parts. With the defaults the bytes limit binds: 448 authenticated scopes and 64 anonymous ones per shard, so 28672 and 4096 for the node. Admission is per shard: once one shard is full, the node refuses a new scope even below the node figure, and with random hashing the fullest shard fills a few percent before the average, so the node admits somewhat fewer scopes than the formula gives. Size the budgets as `scope_reserve` times the scopes which make a non-idempotent call within one window. Memory is used only by records which exist, so a larger budget costs nothing while the store is not full. Above its reserve a scope keeps records as long as there is room. When the budget is full, a new record first evicts the oldest record of its own scope while that scope is over its reserve, then the oldest record of the scope which has been over its reserve the longest. A scope is over its reserve when it holds more than `scope_reserve` records, so only such a scope gives up records to another scope: a scope within its reserve never loses a record because of another scope, whatever the bytes of its records. Beyond `scope_reserve` records, a scope's answers are best-effort: when the node is full, a scope within its reserve can evict them, so the reserve is the only guarantee a scope has. Admission does not protect a scope's surplus, since a scope within its reserve evicts surplus whichever scope holds it. An answer evicted this way is gone, so a retransmit of its call runs the call again, and the eviction is counted in `dedup_evictions_total`.
* Admission. A call of a scope which is not admitted is refused when its pool is at its scope limit, or when its user already has `max_scopes_per_user` scopes with records or calls on this node. A refused call under `on_store_error: fail_closed` answers the temporary `errors.dedup_unavailable` and is not executed. Under `fail_open` it runs without dedup. Such a call is not recorded: a retransmit of it on another connection, while it runs, is refused again and can run the call a second time, while a retransmit on the same connection is answered with the temporary `errors.dedup_unavailable`. A cancel for a refused scope records nothing, and its command still answers the empty reply. The slot frees when the scope's records are acked or expire. Anonymous connections have their own pool of `local_anonymous_max_entries` and `local_anonymous_max_bytes`, so a flood of anonymous instances cannot refuse or evict an authenticated scope.
* Errors of the registry's own gates (unknown method, auth class, request size, token expired) are not recorded. A reply over the response bound is recorded, since the call ran: it is answered with `errors.response_too_large`, or with `errors.request_too_large` when that is unset, and so is a target's refusal of the request, as before.
* A reply over `max_reply_size` is not kept, and neither is one over `scope_reserve_bytes / scope_reserve`, see Budgets and the reserve. A retransmit of it gets `errors.result_evicted`, and the call is not executed again.
* Cancel. The client sends a call with `cancel: <msg_id>` and no method. It cancels the call of that `msg_id` on the connection, and in its scope, and it is answered with an empty reply. The cancelled call answers `errors.cancelled`, and so does a later retransmit. A cancel which arrives before its call keeps a record, so the call never runs. Cancel commands do not wait for `client.concurrency`. A cancel whose `msg_id` is outside the window answers `errors.msg_id_invalid` and records nothing. With dedup off a cancel command is not special: it goes on as an ordinary call, and so is answered as an unknown method.
* Ack. A call lists in `ack` the `msg_id`s whose replies it received. Their records are removed.
* Mutations and disconnects. A non-idempotent call in a scope runs on a context which a disconnect does not cancel, so a backend commit completes, and its answer is kept. A disconnect which the status mapping makes of the answer is kept as a protocol error with its code and reason, so a retransmit gets it without a second backend call. A non-idempotent call without a scope, and an idempotent call, are cancelled by a disconnect.
* A store failure answers a tracked call with the temporary `errors.dedup_unavailable` under `on_store_error: fail_closed`, the default. Under `fail_open` the call runs without dedup. The same applies to a call of a new scope which the node cannot admit within its budgets, see Admission. Idempotent calls do not use the store.
* Retry signal. An error which tells the client that its call was not executed carries the metadata key `x-centrifugo-retry`, which the server sets itself. Its value is `new_msg_id` on `errors.msg_id_invalid` for a `msg_id` outside the window of a first transmission (a retransmit gets no key, since its call may have run), `later` on `errors.overloaded` when admission refuses the call (see [Admission](#admission)), `later` on `errors.token_expired` when `refuse_expired_tokens` refuses the call, `later` on `errors.dedup_unavailable`, when the store refuses the call under `fail_closed`, and when a duplicate waits past the method timeout for the call it joined. With the shared tier (see Shared records), `later` is also sent when the shared tier fails to begin a call under `fail_closed`, to the call and to each duplicate which joined it on this node, and when a duplicate waits on another node past the method timeout. A duplicate whose watch on another node gives up, because the record of the call disappears or its polls fail before an answer, gets `dedup_unavailable` without the key: the call may have run there. Under `fail_open` a call whose `msg_id` already runs on its connection gets `errors.dedup_unavailable` without the key: the call is running, and a resend would run it again. A duplicate of a call whose owner panicked gets `errors.dedup_unavailable` without the key too, and so does a retransmit of its `msg_id`: the call may have run, so the record of the error is kept and the call is not run again. The app decides whether to start a new call. `new_msg_id` means the client re-syncs its clock from the reply time and sends the call again with a new `msg_id`. `later` means the client sends the same call again, with the same `msg_id`, after a short delay. A later resend joins the call of its owner while the owner runs, and gets the owner's record once it is kept. A record which the store has dropped by then is a miss, and the call runs again: the signal does not guard against a dropped record. After a `begin` whose reply was lost under `fail_closed`, the in-flight record of the refused call stays in Redis with no call running, so a later resend to that node waits for the record to expire and is answered `errors.dedup_unavailable` without the key, see Re-execution windows. The call has not run, and a retransmit after the expiry runs it. A cancel is never sent again. No other error carries the key: a call which ran, `errors.msg_id_reused`, `errors.result_evicted`, a cancel, and the gate errors do not. These errors are never recorded. A record written before the key existed may hold it, and its replay to a retransmit drops it. Clients which do not know the key ignore it.
* Records are node-local unless `redis.enabled` is set, see Shared records.
* Secrecy. The instance is a bearer secret. It must be random, made by the application, of at least 128 bits (22 base64url characters), kept per installation or per tab, and never shared or logged. Whoever knows it can retransmit and cancel calls in its scope. The server never logs the instance or a scope.

| Error | Default `code` | `temporary` |
|---|---|---|
| `msg_id_invalid` | 107 | no |
| `msg_id_reused` | 107 | no |
| `result_evicted` | 110 | no |
| `dedup_unavailable` | 108 | yes |

### Shared records (Redis)

Off by default. With `rpc.registry.dedup.redis.enabled`, the records are kept in Redis too, so a retransmit which reaches another node is answered from the record of its first call, and a duplicate of a call which runs on another node waits for it. It is read at start: a change takes effect after a restart. It needs `rpc.registry.dedup.enabled`.

```json
"rpc": {"registry": {"dedup": {"redis": {"enabled": true, "address": ["redis://127.0.0.1:6379"], "prefix": "centrifugo"}}}}
```

To use the Redis of the engine instead, set `"redis": {"enabled": true, "reuse_from_engine": true}`, which requires `engine.type` to be `redis`. The address, the prefix and the timeouts are then the engine's, and the `address`, `prefix` and TLS settings of the dedup config are not read.

Which tier answers. Redis is authoritative for whether a call ran and what it answered. The node-local store is the node's cache of done records, the join point of the calls which run on the node, and the memory guard. So a done record is answered from memory, and only a local miss, or a record kept without its reply, asks Redis; an answer from Redis is cached on the node. A call which runs on this node is joined locally, as before. A call which runs on another node is found in Redis, and its duplicate waits for it. With the wakeup off, the duplicate polls Redis, with a delay which grows from 50 ms to 500 ms. With the wakeup on, the node which runs the call wakes the duplicate when it writes the answer, and the duplicate reads once more 2 seconds after it started, see Wakeup by wake streams. A scope which the node refuses under Admission is refused in both tiers: no record is kept for it in Redis only.

Keys and what is stored. A record is one hash per call, under `<prefix>.rpc.{<scope>}.<msg_id>`. It holds the state of the call, its fingerprint, the node which runs it, its reply or its error, and whether its reply was kept. The scope is the hash tag, so the records of a scope share a slot in Redis Cluster, and each script touches one scope only. The scope is `<kind>:<user tag>:<hash>`. The hash covers the user, the kind and the session or instance, so the instance is not recoverable from it, and the instance is never logged. The user tag is the first 16 hex characters of an unsalted SHA-256 of the user ID. A reader of Redis who has the access to read the keys can therefore match an enumerable user ID, such as a number, to its keys. Rate-limit keys hold the user ID itself, so this does not add a new exposure, but this README does not claim that user IDs are hidden from Redis.

Data at rest. A record keeps the reply, up to the reply limit, and an unsalted SHA-256 fingerprint of the method and the data of the call, for the window and the skew. A reader of Redis can confirm a guess of a low-entropy request body against the fingerprint, and read the reply. Treat the dedup Redis as holding user data: restrict its access, use TLS, and do not share it with services which should not read the replies.

Lifetimes. An in-flight record lives for the method timeout plus 10 seconds, so it outlives its call only when the node which runs the call died. A done record and a cancel record live for the window and the skew, as the node-local records do. When the node which runs a call dies, its in-flight record expires. A retransmit which arrives after that runs the call again. A duplicate which waited for that call sees the record disappear, and is answered with the temporary `errors.dedup_unavailable`, so its retransmit runs the call. A cancel which arrives after the call was acked finds no record in Redis, and leaves a cancelled record there, so every node which does not cache the answer answers `errors.cancelled` for that `msg_id`. A node which still caches the answer of the call from before its ack answers that answer, as the node-local store does after an ack, unless the cancel reached it: then it replaces its cached answer with the cancelled one. The backend receives the `msg_id` on every call, and a backend which must not repeat a commit should check it, as the node-local store already asks.

Re-execution windows. Besides an owner crash, a retransmit on another node runs a completed call again in the following cases.

* The owner's write of the answer fails at both tries. The write is tried twice, and both tries together stay within one IO timeout of the dedup Redis, and before the in-flight record expires, counted from the begin of the call. The answer is kept on the node, and the in-flight record expires in Redis. A slow Redis can cause this too: a write which is cut at its IO timeout, late in the call, leaves no time for the second try.
* Under `fail_open`, a `begin` which reached Redis but whose reply was lost leaves an in-flight record that the node never completes in Redis, so the call runs with node-local dedup only, and a retransmit on another node runs it again after the record expires.
* A Redis failover. Redis replicates asynchronously, so an answer which the primary acknowledged can be lost when a replica takes over before it was replicated. The record is then missing, or still in flight, and a retransmit on another node runs the call again. The shared tier does not wait for replicas, so the backend of a non-idempotent call must dedupe by `msg_id`, as it must for the two cases above.

Each of these is logged, and the failed Redis operations are counted in `centrifugo_rpc_registry_dedup_redis_errors_total`. A call which runs with `fail_closed` is not in the second case, but a `begin` whose reply was lost under `fail_closed` can leave an in-flight record owned by this node, with no call running. A retransmit on this node then waits for that record, as for a call on another node, until it expires, up to the method timeout for each retransmit, and is answered with the temporary `errors.dedup_unavailable`. No call runs twice in that case; it costs a wait. The node does not send an abort for a call which was not run, because an abort which arrives after a later begin of the same key on this node would delete the record of a call which runs.

Cross-node wait and cancel. A duplicate on a node which finds a call running on another node waits for its answer, and gets the answer as a retransmit of a done call does. Its wait ends at the method timeout, as a local join does. If the polls keep failing, the node gives up the watch at the method timeout plus 10 seconds, plus up to `io_timeout` for the last read, and the duplicates waiting for the call are answered with the temporary `errors.dedup_unavailable`, under either `on_store_error`. Under `fail_open` this is a refusal window, not node-local dedup: running the call on this node would not know whether the other node answered it. A retransmit after the window goes through the store again, and runs with node-local dedup only while the store fails. A cancel notification to another node is sent as the cancel is answered, without delaying it, from a goroutine of its own. A duplicate whose call turns out to run on another node sends one in the same way when a cancel reached its flight first. The publish is bounded by the broker connection's write timeout, which is the `io_timeout` of the broker, 4 seconds by default, so a slow broker delays only the notification. While `wakeup.enabled` is off, the wait polls Redis: one watcher runs per call on the node, not per duplicate, and it polls every 50 ms, doubling up to 500 ms, until the answer arrives or the in-flight record expires. A call which runs for its whole method timeout costs about 130 `HMGET` on the node, and about two per second after the first second. Retransmits of N distinct calls which run elsewhere cost N watchers, so a client which retransmits many calls at once adds that load to the node which it reaches. A cancel of a call which runs on another node is sent to that node as a targeted node notification, and the node cancels its call, so the call answers `errors.cancelled` and its record is kept as a cancelled one, as on one node. The notification needs a cluster broker which carries node control messages, the same as the access revocation does. Nodes which share the Redis without such a broker still deduplicate across nodes, but a cancel of a call which runs on another node then does not reach it: the call runs to its method timeout, and its record is whatever it answered. While Redis fails, that cancel is not delivered under either `on_store_error`: under `fail_open` the client's cancel command answers the empty reply and the node records nothing, so the call runs to its end on the other node, and a duplicate waiting here gets that answer. A cancel of a call which has not started is not kept either, so that call runs when it arrives. A watch whose record is gone without an answer answers its duplicates with `errors.dedup_unavailable`. That covers a record which an owner abandoned, one whose in-flight record expired, and a done record which a client ack or a Redis eviction removed before the watch read it. A conforming client does not retransmit a `msg_id` it has acked. A retransmit after the ack is outside the contract, and may run the call again.

Wakeup by wake streams. With `rpc.registry.dedup.redis.wakeup.enabled`, a duplicate which waits for a call on another node reads the answer when the node which runs the call writes it, instead of polling for it. It is on by default, needs `redis.enabled`, and is read at start. With `"enabled": false`, the wait polls Redis instead, as the fallback described above; an explicit `false` is kept. A wait reads once at its check and once more at its deadline, so an answer whose wake is lost is still returned. In a mixed rollout, a node with the wakeup off writes no wakes, so a duplicate on a node with it on, for a call which runs on the node with it off, gets its answer from those reads: late, not refused. Keep the wakeup on every node of the cluster, in one rollout. The example sets it explicitly:

```json
"rpc": {"registry": {"dedup": {"redis": {"wakeup": {"enabled": true, "max_waiter_nodes": 64}}}}}
```

What travels. A waiting node registers itself in the in-flight record when it begins its duplicate. The record lists up to `max_waiter_nodes` node ids, 64 by default, and the owner reads them from the reply of its write. After the record is written, the owner writes one entry per listed node into the stream `<prefix>.rpc.wake.{<node_id>}`, on the shard of the record, in one pipelined call of two commands per node: an `XADD`, trimmed to about 4096 entries, and a `PEXPIRE` of 60 seconds. The entry carries the scope hash and the `msg_id` of the call, and nothing else. A record with no remote waiter writes nothing, and the owner's own id is never listed. The owner's reply waits that one more Redis round trip only when a remote waiter exists. The stream is in the dedup Redis, so the wakeup needs no cluster broker, unlike the cross-node cancel. A wake is a hint, not data: the waiter answers it with one read of the record, so Redis stays the only authority for the answer, and a lost, early, repeated or stale wake is harmless.

The watcher. A node runs one wake watcher per shard of the dedup Redis, and only while it has duplicates waiting for calls on other nodes, and for a minute after the last one ended. Each watcher blocks on `XREAD` of its node's stream, from the start of the stream, for up to 5 seconds, on a connection of the Redis client's blocking pool, and deletes the entries it has read. An empty read after that minute stops it, so a node without remote waits holds no blocked connection and makes no read, and an idle cluster costs the dedup Redis nothing. A wave costs one blocked connection per node and shard while it lasts: about W × S blocked clients and W × S / 5 empty reads per second for W nodes with waits and S shards. A wake written while no watcher runs is read by the next watcher, since the stream lives for a minute.

The wait. A wait reads the record when its wake comes, and once 2 seconds after it started, at the check. The check is jittered by a fifth either way, so the checks of a wave spread over 0.8 seconds. The wait never reads on a timer again. When its in-flight deadline passes with no answer read, it makes one last read, and that read ends it. So a wait reads once for each wake it gets, once at its check, and once at its deadline when no earlier read found the answer. A wait whose owner died reads twice, at its check and at its deadline.

Lost wakes. A wake is lost when the owner dies between its record write and its wake write, when the write fails, or when a failover loses it. The duplicate is then answered at its check, or at its deadline, when the call had finished by then. Otherwise it is answered at its method timeout with the temporary `later` error, and its retransmit finds the record. A wake is also lost when the owner's first write of the record reaches Redis but its reply is lost: the retry finds the record done, gets no waiter list, and writes no wake. A read of a wait which fails keeps the wait, and is not retried: the wait then ends at its check or its deadline, as above. `checked` in `dedup_remote_waits_total` counts these while no node is lost.

Overflow. A duplicate on a node beyond `max_waiter_nodes` does not wait. It is answered with the temporary `later` error at once, counted as `refused` in `dedup_remote_waits_total`, and logged at debug once per interval. `later` carries no retry hint, so the client's own backoff paces the retransmit, and each retransmit while the list is full costs one read-only `begin`. The list holds node ids, and a node which restarted within the in-flight TTL counts again with its new id. A cluster does not reach the default unless a call can be retransmitted to more nodes than that within its method timeout plus 10 seconds. Raise the cap only when `refused` is counted.

Stream bounds. A stream holds about 4096 entries, about 400 KiB, while it is not read, and it expires 60 seconds after its last write, so the streams of restarted nodes go away by themselves. A `volatile-*` policy under `maxmemory` may evict a stream, as it evicts any key with a TTL. The cost is a check for its waiters, not a wrong answer.

Measured on localhost in the integration tests, with the default check, 200 duplicates waiting for calls on another node, which the owner completed after one second: with the wakeup off, about 6 reads per wait, all of them polls, and in the 250 ms buckets they fall from the start of the wave; with the wakeup on, one read per wait, all after the wakes. When the owner died one second into a call with an in-flight TTL of 3 seconds, a wait made about 9 reads with the wakeup off, which is the ramp's first second of about 5 and then 2 a second, and two reads with the wakeup on, at its check and at its deadline, where it ended. The checks of a wave of N dead-owner waits fall in one window of about 0.8 seconds around 2 seconds, so the peak is about N / 0.8 reads a second, once. One run under the race detector measured 7.5 ms from the owner's release to the duplicate's answer.

Per completed call, the owner sends nothing when no remote duplicate waits, and writes one wake per waiting node otherwise, in the one extra round trip above. Per remote wait of a live owner, the wakeup costs one read on its wake, and one more at the check when the call runs longer than 2 seconds. The reads of the first second of a wave are zero with the wakeup on, where the ramp makes about five per wait in it.

Under `on_store_error`, a failure of the shared tier is handled as follows.

| Redis | `fail_closed` (default) | `fail_open` |
|---|---|---|
| Unreachable at start | the node starts; tracked calls answer the temporary `errors.dedup_unavailable` until Redis is reachable | the node starts; tracked calls run with node-local dedup until Redis is reachable |
| `begin` fails | the temporary `errors.dedup_unavailable`, and the call is not run | the call runs with node-local dedup |
| `complete` fails | the write is retried once, both tries within one `io_timeout` and before the in-flight record expires; when they fail the record is kept on the node, and the in-flight record in Redis expires at its TTL, see Re-execution windows. A write which finds no shard connected yet is not retried, so a call which completes just as a node starts keeps its record on the node only | the same |
| `cancel` fails | logged; nothing is recorded in Redis, and a call which runs on another node is not cancelled | logged; nothing is recorded on the node or in Redis, so a call which runs on another node is not cancelled, and a call which has not started runs when it arrives |
| ack fails or its queue is full | counted; the records expire at their TTL | the same |

A call which misses the node-local store makes one `begin` and one `complete` to Redis, and each waits up to `io_timeout` of the dedup Redis, 4 seconds by default. A call answered by a reply which the node-local store keeps makes no Redis call, and neither does an idempotent call. A record which the node keeps without its reply is read from Redis. A Redis which drops packets, rather than refusing connections, therefore delays every tracked call by `io_timeout` before `on_store_error` applies, and the call holds its concurrency slot for that long. There is no circuit breaker. Lower the `io_timeout` of the dedup Redis to bound the delay.

The acks of a node are sent to Redis in batches: 100 ms after the first ack of a batch, or at once when 1024 acks are queued, and a node with no acks queued runs no flush timer. The queue is flushed once more at shutdown, before the connections close. A queue of 65536 acks drops the rest, counted by `centrifugo_rpc_registry_dedup_redis_ack_drops_total`. While Redis drops packets, each flush waits `io_timeout` for every shard, so the live flush can hold the queue back by that much per shard. The shutdown flush has one budget of `io_timeout` for all its batches: the acks it does not send in that time are counted as drops too. The records of acks which are not sent expire at their TTL.

The node never refuses to start because Redis is down. Idempotent methods do not use either tier. With `fail_open`, a node degrades to node-local dedup, which is what it gives without Redis; it never degrades to no dedup while its node-local store is healthy.

The bounds of the node-local store do not all carry over. Redis enforces no budget of its own, so:

| Node-local guarantee | In Redis |
|---|---|
| `max_records_per_scope` | Not enforced. The records of a scope are bounded by their TTL and by acks only. |
| `scope_reserve`, `scope_reserve_bytes` and surplus eviction | Not enforced. Records live until their TTL, their ack, or the eviction of Redis under `maxmemory`. |
| `max_scopes_per_user` | Per node: a user may hold that many scopes on each node. |
| Anonymous pool | Per node: an anonymous scope writes to Redis once the node admits it. `local_anonymous_max_entries: 0` keeps anonymous calls out of both tiers. |
| Reply limit | The same replies. A reply over the smaller of `max_reply_size` and `scope_reserve_bytes / scope_reserve` less the 256 bytes of record overhead, which is 1792 bytes at the defaults, is kept as uncached in both tiers. An error counts the same way in both tiers, by the length of its encoded form, the JSON which Redis stores: an error over that limit is kept as uncached, so an error with a long message is, and so is one whose message has characters which JSON escapes. |
| TTL | The window and the skew for done records and cancel records; the method timeout plus 10 seconds for a call in flight. |

What a flood can do. A client with a valid scope which sends distinct `msg_id`s as fast as it can writes one record per call, and each record lives for the window, the skew and one second, which is 151 seconds at the defaults. The same holds for an error record, whose size is bounded by the same limit. With acks withheld, that is about the call rate times 151 times 2.3 KiB per scope in the worst case, and `max_records_per_scope` does not limit it in Redis. At 10 000 recorded calls per second cluster-wide with 1 KiB average records, that is about 1.5 GiB without acks, and much less with them. The guards are, in order: `client.rate_limit` on RPC, with `redis_user_command`, which shares the same Redis and bounds a user cluster-wide; the admission of each node, which writes nothing for a scope the node refuses; and the `maxmemory` of Redis.

Recommended deployment. Use a dedicated Redis for the shared tier, with `maxmemory` set and `maxmemory-policy noeviction`, sized for its load. Under `noeviction` Redis never removes a record, so an in-flight record stays until its call completes or its TTL ends, and a retransmit always finds the record of a running call. When `maxmemory` is reached, the writes of `begin` fail, and they follow `on_store_error`. Do not use an eviction policy, `volatile-lru`, `volatile-ttl`, `volatile-random` or an `allkeys-*` policy: every dedup key has a TTL, so a volatile policy can evict the record of a running call, and an evicted in-flight record lets a retransmit of its call run it again while the first execution still runs, so the call can execute twice at once. The same holds for a Redis which the engine shares with `reuse_from_engine`: it should run `noeviction` too, and on such a Redis the history and presence writes of the engine also fail at `maxmemory`, so size that Redis for the engine too. `reuse_from_engine` suits small deployments, where the records live in the `rpc` namespace under the engine prefix. Redis 6 or later is required. The scripts do not call `TIME`, so they replicate and run on Redis 6 and later. The wake streams are in the `<prefix>.rpc.wake.` family, one per waiting node, bounded as described above, and under `maxmemory` they follow the policy like any key with a TTL. The wake watchers hold at most one blocked connection per node and shard, which counts toward the `maxclients` of the dedup Redis.

## Admission

Admission decides whether a registry call may run, before its dedup and its backend call. It has two signals, both off by default. Each call is admitted or refused as one.

* The in-flight budget (`max_inflight_bytes`) is a node-wide sum of the costs of the calls in flight. A call's cost is its request bytes plus its response bound (the method's `max_response_bytes`, or the hinted size with `response_size_hint`). A call which would take the sum over the budget is refused, and nothing is queued: a call's memory is allocated when the server reads it, so waiting would hold what the budget is meant to bound. The budget is charged with a compare-and-swap, so a call which fits is never refused because another call is charged at the same moment. The budget is node-wide and has no share per client: the calls of every client of the node draw on it, so one client's calls in flight can refuse the others until they finish. `client.concurrency` bounds the calls in flight of one connection, and the connection and rate limits bound each client's connections; none of them bounds a client's share of the budget.
* The memory watermark (`memory_watermark_bytes`) refuses calls while the resident memory of the Go runtime is at or over it, the quantity `GOMEMLIMIT` is measured against. The sample is taken on the admission path, at most every 100 ms, so there is no goroutine or timer. Refusals stop when the memory is under 90% of the watermark, so a node at the edge does not flap call by call. Set the watermark below `GOMEMLIMIT` or the container limit. A watermark under the resident memory of the node is a lock-out, not a limit on new calls: every registry call is refused until the memory falls under 90% of the watermark. A watermark of 64 MiB on a process with 120 MiB resident refuses all registry calls until the resident memory is about 57 MiB, and the start does not warn about it.

The budget is charged from the admission of a call until its reply is returned to the client handler. The reply's bytes after that, while they wait in the writer queue of their connection, are not charged: they are bounded per connection by `client.queue_max_size`, and across the node by `memory_watermark_bytes`. A retransmit with a `msg_id` which joins a call in flight is admitted like any call, and keeps its cost while it waits for the owner, up to the method timeout.

A refused call is answered with `errors.overloaded`, temporary, with `x-centrifugo-retry: later`: the call was not executed, so the client sends it again later, with the same `msg_id` if it has one. A refused call is not recorded for exactly-once RPC. Admission runs before the exactly-once lookup, so a retransmit of a completed call is refused like any other call while the budget or the watermark refuses; it gets the recorded answer once a retry is admitted, and the cost is latency, not a second execution. Both keys are read at start. A reload which changes either one logs a warning and keeps the values in effect until a restart; the rest of the reload applies.

Config validation refuses a budget below the smallest cost of any method. A call costs its request bytes plus its response bound, and a request may be empty, so the smallest cost is the response bound: the response limit, or for a method with `response_size_hint`, the hint overhead and one byte. It refuses a negative budget or watermark, and a watermark under 1 MiB, which is under the resident memory of any process, so it would refuse every call and never recover. A method whose full-size request costs more than the budget (`max_request_bytes` plus its response limit, for a method without `response_size_hint`) is a warning at start, on a configuration reload and in `checkconfig`, and not an error: every request of that size is refused, while smaller requests are admitted. The rules which depend on bandwidth or latency are documented in [Large-payload fleets](#large-payload-fleets). A registry file reload runs the same budget, affinity and size hint rules: a file which fails them is refused, and the current registry stays active.

Refusals are counted in `centrifugo_rpc_registry_admission_refusals_total{reason}`, with reason `inflight` or `memory`, and in `calls_total` with result `overloaded`. The budget in use is `centrifugo_rpc_registry_inflight_bytes`.

## Large-payload fleets

A large-payload fleet is an ordinary Centrifugo deployment whose registry is configured for big frames. It needs no server feature beyond the keys above and the response size hint: the frame limit, the reply queue, the write timeout and the concurrency are the existing keys of the fleet. No profile in this section is a default. Each profile is derived from the same parametric rules, and any other profile substitutes its own inputs.

### Deriving the keys

The inputs of a profile are:

* `P`: the part size of an upload, in bytes.
* `R`: the largest read, in bytes.
* `W`: the window of the client's pool, the calls in flight on one connection.
* `S`: the slowest client link the fleet serves, in bytes per second.
* `λ`: the payload rate one node admits, in bytes per second. It is at most the node's link.
* `T`: the backend's 99th percentile latency for the large methods, in seconds.
* `M`: the node's `GOMEMLIMIT`, in bytes.

Two constants are fixed. `E` is 1024 bytes, the envelope of one frame, and `K` is 65536 bytes. The hint's `overhead_bytes` is 1024, which is `E`. The keys follow:

| Key | Formula | Reason |
|---|---|---|
| `max_request_bytes` of a write | `P + E` | a part and its envelope |
| `max_request_bytes` of a read | `E` | a read carries a position and a length only |
| `max_response_bytes` of a read | `R + E` | a read and its envelope |
| `max_response_bytes` of a write | `E` | a write answers with an acknowledgement |
| `websocket.message_size_limit` | `(P + E) + E` | the largest request and one envelope; a frame over the limit closes the socket with 1009 |
| `client.concurrency` | `W + 1` | the window plus one, so the read loop keeps a slot to read pongs |
| `client.queue_max_size` | `client.concurrency × (R + E) + K` | the queue holds one reply per call in flight, at most the window, and the concurrency is at least the window plus one, so the product is an upper bound; a reply over the size disconnects the client as slow |
| `websocket.write_timeout` | `(R + E) ÷ S`, rounded up to a whole second | the largest reply must leave on the slowest link in time |
| `rpc.registry.max_inflight_bytes` | `2 × λ × T`, rounded up to a power of two in MiB | twice the bytes in flight at the tail (see Sizing the admission) |
| `rpc.registry.memory_watermark_bytes` | `0.75 × M` | the watermark sits at 75% of the memory limit |
| `response_size_hint.overhead_bytes` | `O`, 1024 | a read of `h` bytes reserves `h + O`, and the full `R + E` from `h = R` on |
| `client.max_messages_in_frame` | `1` | each reply leaves in its own frame, so the write timeout covers one reply |
| `client.pong_timeout` | `⌈client.queue_max_size ÷ S⌉ + 2 s` | the server queues its ping behind everything queued on the connection, and starts the pong timeout when it queues the ping; the 2 s covers the round trip and the socket send buffer |
| `client.ping_interval` | greater than `client.pong_timeout` | the server does not start otherwise |

The SDKs close a connection with "no ping" after `ping_interval + maxServerPingDelay` without a ping. The JS SDK restarts that timer on any message, so the sum must cover one frame drain, `(R + E) ÷ S`. The Java SDK restarts it only on a ping, so its `maxServerPingDelay` must be at least `⌈client.queue_max_size ÷ S⌉ + 2 s`, the same figure as the pong timeout. The window `W` no longer appears in these keys: `W × (R + E) ÷ S` is the time for the replies of the window, not the bound on the queue. Raising these keys slows the detection of dead connections. A fleet that needs faster detection lowers `R`, the queue, or `W`.

Two checks close the derivation. The transient of the budget must fit under the watermark: `3 × max_inflight_bytes ≤ memory_watermark_bytes`. And the budget must cover one full-size write, `max_request_bytes + max_response_bytes`, or it refuses that write at any load. If the first check fails, lower the rate or the latency the budget is sized for, or raise the memory limit.

### Worked examples

Each profile lists its inputs, then its keys. The arithmetic column applies the formulas above to the inputs. Sizes are in bytes unless a unit is given, and the rounding is stated where it applies.

**(a) Small objects: 64 KiB parts, 256 KiB reads, 1 GiB node.** Inputs: `P` = 65536, `R` = 262144, `W` = 2, `S` = 125000 (1 Mbit/s), `λ` = 12500000 (100 Mbit/s), `T` = 0.5 s, `M` = 768 MiB (805306368).

| Key | Value | Arithmetic |
|---|---|---|
| `max_request_bytes`, write | 66560 | 65536 + 1024 |
| `max_request_bytes`, read | 1024 | `E` |
| `max_response_bytes`, read | 263168 | 262144 + 1024 |
| `max_response_bytes`, write | 1024 | `E` |
| `websocket.message_size_limit` | 67584 | 66560 + 1024 |
| `client.concurrency` | 3 | 2 + 1 |
| `client.queue_max_size` | 855040 | 3 × 263168 + 65536 |
| `websocket.write_timeout` | 3s | 263168 ÷ 125000 = 2.11, rounded up |
| `response_size_hint` on reads | `request_field` 2 (example field), `overhead_bytes` 1024 | the full bound from `h` = 262144 |
| `rpc.registry.max_inflight_bytes` | 16777216 (16 MiB) | 2 × 12500000 × 0.5 = 12500000 = 11.9 MiB, rounded up to 16 MiB |
| `rpc.registry.memory_watermark_bytes` | 603979776 (576 MiB) | 0.75 × 805306368; the transient 3 × 16 MiB = 48 MiB fits under it |
| `client.max_messages_in_frame` | 1 | each reply leaves in its own frame |
| `client.pong_timeout` | 9s | ⌈855040 ÷ 125000⌉ = 7 s, plus 2 s; the default 8s leaves only 1.2 s over a full queue, less than the margin |
| `client.ping_interval` | 25s | greater than the pong timeout |
| Java SDK `maxServerPingDelay` | 10s | the default, which covers 9 s |

**(b) 512 KiB parts with 1 MiB reads, 2 GiB node with `GOMEMLIMIT` 1.6 GiB.** Inputs: `P` = 524288, `R` = 1048576, `W` = 3, `S` = 32000 (256 kbit/s), `λ` = 62500000 (500 Mbit/s), `T` = 2 s, `M` = 1.6 GiB.

| Key | Value | Arithmetic |
|---|---|---|
| `max_request_bytes`, write | 525312 | 524288 + 1024 |
| `max_request_bytes`, read | 1024 | `E` |
| `max_response_bytes`, read | 1049600 | 1048576 + 1024 |
| `max_response_bytes`, write | 1024 | `E` |
| `websocket.message_size_limit` | 526336 | 525312 + 1024 |
| `client.concurrency` | 4 | 3 + 1 |
| `client.queue_max_size` | 4263936 | 4 × 1049600 + 65536 |
| `websocket.write_timeout` | 33s | 1049600 ÷ 32000 = 32.8, rounded up |
| `response_size_hint` on reads | `request_field` 2 (example field), `overhead_bytes` 1024 | the full bound from `h` = 1048576 |
| `rpc.registry.max_inflight_bytes` | 268435456 (256 MiB) | 2 × 62500000 × 2 = 250000000 = 238.4 MiB, rounded up to 256 MiB; the transient 3 × 256 MiB = 768 MiB fits under the watermark |
| `rpc.registry.memory_watermark_bytes` | 1288490189 (1.2 GiB) | 0.75 × 1.6 GiB, rounded to the byte |
| `client.max_messages_in_frame` | 1 | each reply leaves in its own frame |
| `client.pong_timeout` | 136s | ⌈4263936 ÷ 32000⌉ = 134 s, plus 2 s |
| `client.ping_interval` | 150s | greater than the pong timeout |
| Java SDK `maxServerPingDelay` | 136s | the same figure as the pong timeout |

Dead peers are detected between `pong_timeout` and `ping_interval + pong_timeout` after they stop answering: up to about 4 min 45 s on this profile.

**(c) Large-object tier: 4 MiB parts and 4 MiB reads, 8 GiB node with `GOMEMLIMIT` 6 GiB.** Inputs: `P` = 4194304, `R` = 4194304, `W` = 2, `S` = 1250000 (10 Mbit/s), `λ` = 125000000 (1 Gbit/s), `T` = 4 s, `M` = 6 GiB (6442450944).

| Key | Value | Arithmetic |
|---|---|---|
| `max_request_bytes`, write | 4195328 | 4194304 + 1024 |
| `max_request_bytes`, read | 1024 | `E` |
| `max_response_bytes`, read | 4195328 | 4194304 + 1024 |
| `max_response_bytes`, write | 1024 | `E` |
| `websocket.message_size_limit` | 4196352 | 4195328 + 1024 |
| `client.concurrency` | 3 | 2 + 1 |
| `client.queue_max_size` | 12651520 | 3 × 4195328 + 65536 |
| `websocket.write_timeout` | 4s | 4195328 ÷ 1250000 = 3.36, rounded up |
| `response_size_hint` on reads | `request_field` 2 (example field), `overhead_bytes` 1024 | the full bound from `h` = 4194304 |
| `rpc.registry.max_inflight_bytes` | 1073741824 (1 GiB) | 2 × 125000000 × 4 = 1000000000 = 953.7 MiB, rounded up to 1 GiB; the transient 3 GiB fits under the watermark |
| `rpc.registry.memory_watermark_bytes` | 4831838208 (4.5 GiB) | 0.75 × 6442450944 |
| `client.max_messages_in_frame` | 1 | each reply leaves in its own frame |
| `client.pong_timeout` | 13s | ⌈12651520 ÷ 1250000⌉ = 11 s, plus 2 s; the default 10s would not cover a full queue |
| `client.ping_interval` | 25s | greater than the pong timeout |
| Java SDK `maxServerPingDelay` | 13s | the default 10s is too short |

### Fleet rules

* The registry file lists only the large-payload methods. Each has `idempotent: true`, an explicit `timeout` (set per method, not derived here), `max_request_bytes` and `max_response_bytes`, `response_size_hint` on reads, the affinity header in `headers` (for example `x-example-affinity`), and the moves and redirects in `error_metadata`. Targets which keep state per object on a replica use `ring_hash`.
* The server logs a warning at start, on a configuration reload and in `checkconfig` when a method's `max_request_bytes` is over a served transport's request bound (`websocket.message_size_limit`, `webtransport.message_size_limit`, or `emulation.max_request_body_size` with HTTP-stream or SSE), when its `max_response_bytes` is over `client.queue_max_size`, or when a method without `response_size_hint` costs more at full size (`max_request_bytes` plus `max_response_bytes`) than `max_inflight_bytes`. It keeps loading: the method still serves every request under the transport limit, and every reply under the queue size. A registry file shared by fleets with different limits is the expected case of that warning.
* At the default `client.concurrency` of `0`, calls run one at a time on the read loop, so a window of one is what a socket carries without blocking it. A larger window needs the concurrency raised, or the read loop blocks on the semaphore and reads no pongs, and the connection can be closed as not answering pings once a call outlives the pong timeout.
* No channel namespaces, and no personal channel on the fleet. Client subscriptions are refused by default, and the fleet runs no broker. Issue tokens without `channels`, since each would add a server-side subscription. A personal channel would let `single_connection` close the user's other pooled connections.

### Moves and redirects

The SDK pool acts on three answers of a backend. Each answer is an error or a reply. An error carries its answer in trailers, which the method lists in `error_metadata`, so the registry copies them into the error. A reply carries its answer in its body, and the registry drops the trailers of a reply. The server does nothing else with them.

* **Move.** An error whose trailer names the place which has the data, for example `x-example-relocate: eu-2`. The pool's `reroute` hook maps the error to an endpoint key, and the pool sends the call there at once, with its affinity hint. The hook runs on error replies only, never on a lost connection or a timeout. Each resend counts against `retries`. The application updates its own cluster map. A move never changes a connection on the server.
* **Redirect with a plain URL.** A reply whose body names a URL, or an error whose trailer in `error_metadata` names one, for example `x-example-location`. The pool's `redirect` hook reads the URL from the reply or the error and returns it with its headers and a byte limit, and the SDK fetches that final URL itself, within the call's deadline. The SDK follows no hop: a 3xx answer fails the fetch with the redirect error. A secure endpoint (`wss:` or `https:`) is never downgraded, so its URL must be `https:`, and a URL on `http:` is accepted only for an insecure endpoint (`ws:`). A URL of any other scheme is refused before the request is sent. The fetch waits for a slot of the pool's bound on concurrent fetches, within the call's deadline. It is not retried, because the URL may be single-use.
* **Application-handled redirect.** A backend-defined object, carried as an ordinary reply or error, for example a reply which names another endpoint key and a method of that cluster. The SDK does not decode it, and the `redirect` hook returns nothing for it. The application decodes the object and follows it with its own calls, for example `pool.rpc("blob.readPart", data, {endpointKey: "eu-2"})`. The original call settles with the object, and the follow-up is a new call.

### Sizing the admission

Calls in flight obey Little's law: the bytes in flight equal the admitted payload rate times the time a call stays on the node. So `max_inflight_bytes` is about twice the bytes in flight at the 99th percentile of the backend latency, as the formula in the derivation states. Below it the budget never refuses a healthy fleet. Above it, a stalled backend, whose latency grows towards the method timeout, is what trips the budget.

The memory of the calls in flight is at most about three times the budget while they are being moved, and about one time it while they wait on the backend. So the transient must fit under the watermark. The watermark sits at 75% to 80% of `GOMEMLIMIT`, or of the container limit less the headroom of the kernel and the sockets. It catches what the budget cannot see: the writer queues of slow readers, the decoder buffers, and everything else the node does. The GC works harder as the runtime nears `GOMEMLIMIT`, so the watermark sits below it, not at it.

Alert on `centrifugo_rpc_registry_inflight_bytes / max_inflight_bytes > 0.8`, and on `rate(centrifugo_rpc_registry_admission_refusals_total[5m]) > 0` by `reason`. `inflight` means the backend is slow for the offered load. `memory` means the node is undersized, or slow readers hold the writer queues.

### Connection churn

What a client costs the fleet beyond its calls, and the key which bounds each line:

| Line | Bound | Key |
|---|---|---|
| Sockets per client | connections per endpoint times the endpoint keys in use, 2 at the defaults of the client pool | client pool options |
| Sockets per user per node | the line above times the devices of the user on the node | `client.user_connection_limit` |
| New sockets per second per node | the reconnect storm after a deploy | `client.connection_rate_limit` |
| Idle sockets | the client pool closes a socket after its idle timeout; the server closes a socket only when it stops answering pings | client pool `idleTimeout`, `client.stale_close_delay` for unauthenticated sockets |
| Token mints per open | one at most per open, and a mint reaches the issuer only once per expiry of the cached token | the token provider of the client |

An application which keeps transfers rare keeps its sockets closed between them. One which transfers continuously keeps two sockets per client open.

## Metrics

* `centrifugo_rpc_registry_calls_total{rpc_method, result}`: calls by method and result (`ok`, `error`, `cancelled`, `timeout`, `denied`, `unknown`, `overloaded`). A call refused by `refuse_expired_tokens` counts as `expired`, and that series appears for a method at its first refusal. A call to a name which is not in the registry counts as `unknown`.
* `centrifugo_rpc_registry_inflight_bytes`: bytes of registry calls in flight on the node, by the cost of admission. It reads zero when `max_inflight_bytes` is off, and the gauge is always registered.
* `centrifugo_rpc_registry_admission_refusals_total{reason}`: calls which admission refused, by `reason`: `inflight` for the budget, `memory` for the watermark. See [Admission](#admission).
* `centrifugo_rpc_registry_errors_total{rpc_method, app_type}`: failed calls with an application error type. Cancelled calls are not counted here.
* `centrifugo_rpc_registry_reloads_total{trigger, result}`: reloads of the registry. `trigger` is `signal` for a configuration reload on SIGHUP, and `poll` for a change of the registry file found by the poll. `result` is `ok` or `error`. A touch without a change of content is not a reload. A reload which fails, on a content or a dial error, counts as `error`. A file which cannot be read is logged and not counted.
* Per target, the proxy metrics with protocol `grpc` and type `rpc`: `centrifugo_proxy_duration_seconds`, `centrifugo_proxy_duration_seconds_histogram`, `centrifugo_proxy_errors` and `centrifugo_proxy_inflight_requests`. A call cancelled by the client is not observed in them, as the proxy handler does for a closed client; `centrifugo_proxy_inflight_requests` still counts it while it runs.
* `centrifugo_rpc_registry_dedup_total{outcome}`: exactly-once calls by outcome (`miss`, `hit_done`, `hit_inflight_local`, `hit_inflight_remote`, `evicted`, `cancelled`, `store_error`, `msg_id_invalid`, `msg_id_reused`, `no_scope`, `refused_pool`, `refused_user`). `hit_inflight_remote` is a duplicate which waited for a call running on another node, see Shared records. `cancelled` counts cancel commands. `refused_pool` is a new scope which its pool cannot admit, and `refused_user` a user at `max_scopes_per_user`; both count a call or a cancel command, and are not counted as `store_error`. Answers from a record are counted here and not in `calls_total`, which counts backend calls. No ID is in any label.
* `centrifugo_rpc_registry_dedup_evictions_total{reason}`: answers the node evicted to make room. `scope_cap` is the oldest answer of a scope over `max_records_per_scope`, `over_reserve` the oldest answer of the scope which is over its reserve while the budget is full, `surplus` the oldest answer of the scope which has been over its reserve the longest, evicted for another scope, and `uncached` a reply over its share of `scope_reserve_bytes / scope_reserve`, whose record is kept without it, and `reply_size` a reply over `max_reply_size`, whose record is kept without it when the call completes. A rise of `surplus` with `refused_*` is the signature of a flood. A rise of `uncached` means replies are larger than the share, so raise `scope_reserve_bytes` to keep them. A rise of `reply_size` means replies are larger than `max_reply_size`, so raise it, and `scope_reserve_bytes` too when they are over the share.
* `centrifugo_rpc_registry_dedup_entries`: answers the node keeps, of both pools. Like `dedup_scopes` and `dedup_bytes`, it is node-local: the occupancy of the shared tier is read from Redis, with `INFO memory`, or `DBSIZE` of a dedicated instance.
* `centrifugo_rpc_registry_dedup_scopes{pool}`: scopes the node admits, by `pool` (`authenticated`, `anonymous`). Compare it with the limit of its pool, per shard: the smaller of its entries / `scope_reserve` and bytes / `scope_reserve_bytes`, times 64. The authenticated pool takes `local_max_entries` and `local_max_bytes` less their anonymous parts, and the anonymous pool takes the anonymous parts. Each shard is limited by its own share, see Budgets and the reserve.
* `centrifugo_rpc_registry_dedup_bytes{pool}`: bytes of the answers the node keeps, by `pool`.
* Suggested alerts: `rate(centrifugo_rpc_registry_dedup_total{outcome=~"refused_.*"}) > 0`, and per pool `centrifugo_rpc_registry_dedup_scopes / limit > 0.8`, where the authenticated limit is `64 * min((local_max_entries - local_anonymous_max_entries) / 64 / scope_reserve, (local_max_bytes - local_anonymous_max_bytes) / 64 / scope_reserve_bytes)` and the anonymous limit is `64 * min(local_anonymous_max_entries / 64 / scope_reserve, local_anonymous_max_bytes / 64 / scope_reserve_bytes)`. With the defaults these are 28672 and 4096.
* `centrifugo_rpc_registry_dedup_wait_seconds`: time a duplicate waited for the call it joined, on this node or on another one.
* `centrifugo_rpc_registry_dedup_redis_errors_total`: failed Redis operations of the shared tier. A call made while Redis is not connected counts one, and so does each ack flush while it is not connected, so an outage counts about once per flush. A `complete` whose first write finds the in-flight record not owned by the node is counted, since the answer then stays on the node only, and the node logs it at debug. A retried write which finds the record done by its first attempt is not counted: its reply was lost, and the answer is in the shared tier. Alert on any rise: `rate(centrifugo_rpc_registry_dedup_redis_errors_total[5m]) > 0`.
* `centrifugo_rpc_registry_dedup_redis_duration_seconds`: duration of the Redis operations of the shared tier.
* `centrifugo_rpc_registry_dedup_redis_ack_drops_total`: acks the node did not send to the shared tier, because its ack queue was full or because the node shut down first. The shutdown drain sends the queued acks and the acks enqueued during it while its `io_timeout` lasts; the keys of a shutdown flush which fails are counted too, and so are the acks the timeout leaves unsent. A flush which fails while the node runs counts in `centrifugo_rpc_registry_dedup_redis_errors_total` only. The records of all these acks expire at their TTL. An ack enqueued after the node has stopped the shared tier is neither sent nor counted.
* `centrifugo_rpc_registry_dedup_remote_reads_total`: reads of the shared tier which the waits for calls on other nodes make: one per wake, one at the check, and one per poll with the wakeup off.
* `centrifugo_rpc_registry_dedup_remote_waits_total{end}`: waits for calls on other nodes, by how they ended. `woken` is a wait which a wake woke, and whose read found the answer or the key gone; `checked` is a wait which its check, or its read at the deadline, ended with no wake before it; `polled` is a wait which a ramp poll ended, with the wakeup off; `deadline` is a wait which the in-flight TTL ended; `refused` is a duplicate which the full waiter list did not let wait. With the wakeup on, `checked` while no node is lost means lost wakes, and `refused` means the waiter list is full. Suggested alerts, while no node is lost: `rate(centrifugo_rpc_registry_dedup_remote_waits_total{end="checked"}[5m]) > 0` with the wakeup on, and `rate(centrifugo_rpc_registry_dedup_remote_waits_total{end="refused"}[5m]) > 0` at any time.
* `centrifugo_rpc_registry_dedup_wakes_total{result}`: wakes which this node wrote for the waiters of its calls, one per waiting node of a completed call, by `result` (`ok` or `error`). Alert on any rise of `error`, which means waits which are checked instead of woken.
* `centrifugo_rpc_registry_dedup_wake_watchers`: wake watchers of this node which read the wake streams. It is 0 on an idle node by design, and while the node has waits for calls on other nodes it equals the number of shards of the dedup Redis. Use it on dashboards, not as an alert.
* `centrifugo_rpc_registry_dedup_notifications_total{op, result}`: node notifications of exactly-once RPC which this node sent. `op` is `cancel`, which asks the node which runs a call to cancel it; `result` is `ok` or `error`.
* `centrifugo_rpc_registry_retransmit_total{rpc_method}`: calls which the client sent again after a transport loss.
* The series are created when the registry is built. Each registry method exports one zero-valued `calls_total` series per result, so N methods export 7·N series, plus one `expired` series for each method that was refused. Each target exports its four proxy series, the same way. The `unknown` method label is the exception: its series are created on the first call to an unknown name.

## Parity

Centrifugo PRO has no RPC method registry, so the features of this registry have no PRO equivalent to match:

| Feature | Status |
|---|---|
| Large-payload fleets, affinity routing, admission by in-flight bytes and memory | fork extension, no PRO equivalent |

## Planned

Not implemented yet:

* A way for the connect proxy to return the auth class, so that connections made through it can have a class when `auth_class_claim` is set.
* Restricting what a `pending_2fa` connection can do outside the registry. Such a connection still keeps its user for channels and personal subscriptions. Restricting that comes with the auth upgrade work.
* The `after` field, which orders a call after other `msg_id`s.
