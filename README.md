# ImpulseM.Centrifugo

ImpulseM fork of [Centrifugo](https://github.com/centrifugal/centrifugo): upstream Centrifugo v6 plus features needed at messenger scale. Upstream is merged regularly (last sync: upstream master of 2026-10, v6.9.x), and a weekly check opens an issue when the fork falls behind. The features are implemented from the public [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/overview), keeping its config keys and API shapes. Planned work is tracked in the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Added on top of upstream

* [Server-side publication tags filter](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7): a per-subscriber filter set by the backend that the client can't widen. The filtering itself is upstream's Core implementation; the fork lets the backend set it from the subscription token, connection token `subs`, and subscribe and connect proxy results. Recovery applies the filter; an explicit history call on a filtered subscription is refused, since history is not filtered.
* [User blocking and token revocation](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/10):
  * `block_user`, `unblock_user`, `revoke_token` and `invalidate_user_tokens` server API methods;
  * extensions for session-based revocation and revocation generations;
  * an in-memory store on every node, live disconnects and cluster propagation.
  * Differences from Centrifugo PRO:
    * `revoke_token` without `user` triggers a full sweep of local connections, coalesced and run in the background. Pass `user` for a targeted, cheap disconnect.
    * `expire_at` in the past is rejected with bad request.
    * Extensions: `user` and `revoked_at` on `revoke_token`; `generation` and `revoked_at` on `invalidate_user_tokens`; `token_revoke.uid_claim` and `user_tokens_invalidate.generation_claim` config; disconnect code 4500 for blocked users; loop guard disconnects with 3500.
  * Not implemented:
    * subscription tokens are not covered by revocation, only connection tokens;
    * `invalidate_user_tokens.channel` is not available;
    * `storage_type` is memory only: `redis` and `database` are rejected at startup. Use the per-node revocation source (`access_revoke.source`) as the durable delivery path.
* Singleflight for `History`, `Presence` and `PresenceStats` (`singleflight.enabled`): concurrent identical calls share one broker or presence request. Helps only with a remote engine (Redis), not with the memory engine.
* `shared_position_sync` channel option (namespace or `channel.without_namespace`): the node checks stream position once per channel per `client.channel_position_check_delay` instead of once per subscriber, so position checks cost one broker request per channel per interval.
  * Requires `force_positioning`, `allow_positioning`, `force_recovery` or `allow_recovery`, and a `client.channel_position_check_delay` greater than zero.
  * Detection latency: one to two `client.channel_position_check_delay` intervals for a node-wide loss, one interval for a single client.
  * `client.channel_position_check_delay` must exceed the broker's worst-case PUB/SUB lag, otherwise a publication in flight is reported as a loss.
  * A broker that lost the tail of a stream but kept its epoch (e.g. a Redis failover with async replication) is detected when the stream top falls behind what the node already delivered, confirmed over two intervals. A client ahead of the cached stream top is valid unless the top was fetched after its last update.
  * Options are applied when a channel's first subscriber arrives on the node. A config change applies to a channel after it empties.
  * Enabling it for the first time (no channel used it at startup) requires a node restart: when no channel options use it, the node skips the per-publication lookup entirely.
* Operation rate limits (`client.rate_limit`) as described in the PRO docs: per-connection `client_command` limits with `total`, `default`, channel namespace overrides and RPC method overrides, `user_command` limits per user ID (in memory, shared by all connections of a user, including user-level `connect`), and `client_error` limits that disconnect abusive connections. `redis_user_command` keeps per-user limits in Redis per user ID, so all nodes share one budget; it has no `total`. Users are spread over Redis shards by consistent hashing of the user ID. With `reuse_from_engine` it uses the engine Redis configuration and engine prefix, so apps sharing one Redis with different engine prefixes never share limits. On Redis errors commands are allowed (fail-open), and each Redis call is bounded by `io_timeout`. The node starts when Redis is unreachable: it allows commands and reconnects in the background.
  * Where the PRO docs leave it open: user-level `connect` falls back to `default` but is not subject to `total`, like `connect` in `client_command`. Limiters are checked in order `client_command`, `user_command`: a command rejected by the user limit still consumes its per-connection tokens.
  * `track`, `untrack`, `map_publish` and `map_remove` are not supported yet and are rejected at startup ([#21](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/21)). Until then map and shared poll subscriptions count as `subscribe`, map publish and remove as `publish`, and track and untrack as `sub_refresh`.

### Access revocation event source

Each node can receive access revocation events from a replayable log and rebuild its store after a restart. Kafka is the only source type now. Without `access_revoke.source.type` nothing changes.

```json
"access_revoke": {
  "source": {
    "type": "kafka",
    "window": "30m",
    "startup_timeout": "60s",
    "lag_threshold": "30s",
    "reject_connect_on_lag": false,
    "kafka": {
      "brokers": ["kafka:9092"],
      "topic": "access_revoke",
      "method_header": "centrifugo-method"
    }
  }
}
```

A record carries the API method name in the `centrifugo-method` header (`block_user`, `unblock_user`, `revoke_token` or `invalidate_user_tokens`) and the JSON request body of that API command, for example `{"user":"42","expire_at":1790000000}` with `block_user`. One producer can feed both the API and the source.

* Time defaults come from the record timestamp. Set `revoked_at` and `issued_before` in the payload when the producer may lag behind the real revocation.
* Events older than `window` are not replayed after a restart. Longer bans must be re-emitted or set by the token issuer.
* A node is ready once it caught up with the topic, or after `startup_timeout` (fail-open). The health endpoint answers 503 with `{"ready":false}` until then. With `reject_connect_on_lag: true` new connections are rejected while lagging. If all nodes lag, that is a cluster-wide outage.

Topic guidance:

* `cleanup.policy=delete`, not compact: compaction would collapse several events of one user.
* Retention of at least 2 × `window`.
* Record key is the user ID, so events of one user stay ordered.
* 1 to 6 partitions: every node reads every partition.
* Restrict producers with topic ACLs: any producer can ban users.

### RPC method registry

The registry lets clients call existing gRPC methods of backend services through Centrifugo's RPC. A client calls a registry method name, and Centrifugo calls the gRPC method it maps to by full name, for example `/example.accounts.v1.AccountService/GetProfile`. There is no wrapper service: request and response bytes pass through unchanged, and the backend keeps its own protobuf contract. Without `rpc.registry.enabled` nothing changes.

```json
"rpc": {
  "registry": {
    "enabled": true,
    "file": "/etc/centrifugo/rpc_registry.yaml",
    "poll_interval": "10s",
    "drain_timeout": "30s",
    "errors": {
      "unknown_method": {"code": 104, "message": "method not found", "app_code": 400, "app_type": "UNKNOWN_METHOD"},
      "auth_class": {"code": 103, "message": "permission denied", "app_code": 401, "app_type": "ACCESS_DENIED"},
      "request_too_large": {"code": 106, "message": "limit exceeded", "app_code": 400, "app_type": "REQUEST_TOO_LARGE"},
      "cancelled": {"code": 107, "message": "bad request", "app_code": 400, "app_type": "CALL_CANCELLED"}
    },
    "app_error": {"code_key": "app-code", "type_key": "app-type"},
    "grpc_status_to_code_transforms": []
  }
}
```

* `file`: path to the registry file, JSON or YAML, chosen by extension. Entries are read from this file only: combining `file` with inline `defaults`, `targets` or `methods` is an error.
* `poll_interval`: how often the file is checked for changes. `0` means the file is reloaded on SIGHUP only. A changed file is applied as a whole: if it fails validation, the previous registry stays active.
* `drain_timeout`: how long connections of removed or replaced targets stay open before they are closed, so calls in flight can finish. Default `30s`.
* `errors`: protocol errors returned for failures of the registry itself: unknown method, auth class mismatch, request too large, and a call cancelled by the client. Each has `code`, `message`, and optional `app_code` and `app_type`.
* `app_error`: names of the gRPC trailers which carry the application error code and type. Defaults are `app-code` and `app-type`.
* `grpc_status_to_code_transforms`: overrides for the default mapping of gRPC status codes to protocol errors, see [error mapping](#error-mapping).

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

Auth classes say which connections can call a method:

* `anonymous`: any connection. The connection token is never forwarded, even when the connection is authenticated.
* `pending_2fa`: a connection in the partially authenticated state, which forwards its connection token.
* `authenticated`: an authenticated connection, which forwards its connection token.
* `bot`: a bot connection, which forwards its connection token.

A method without `auth_class` takes `defaults.auth_class`. If that is empty or `anonymous`, every method must state its class, so a missing class never makes a method public. A connection which does not satisfy the class gets `errors.auth_class`.

Metadata sent with each call. Later sources win:

1. static metadata of the target (`grpc.static_metadata`);
2. connect-time headers listed in the target's `emulated_headers`;
3. per-call headers listed in the method's `headers`;
4. values set by Centrifugo: `authorization: Bearer <token>` for non-anonymous methods, `x-centrifugo-client` with the client ID, `x-centrifugo-msg-id` and `x-centrifugo-retransmit` for calls with a message ID, and `x-centrifugo-without-updates` when set;
5. `x-forwarded-for` set to the address of the client connection, never the value sent by the client;
6. `traceparent` and `tracestate` from the trace context of the call.

#### Error mapping

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

* `grpc_status_to_code_transforms` replaces the row of a status. Each rule has `status` (a gRPC status name, exact match) and `to_error` or `to_disconnect`, the same as `status_to_code_transforms` for HTTP proxies:

  ```json
  "grpc_status_to_code_transforms": [
    {"status": "UNAUTHENTICATED", "to_error": {"code": 150, "message": "sign in again"}}
  ]
  ```
* The protocol error `message` is the gRPC status message, cut to 256 bytes.
* The application error code comes from the `app_error.code_key` trailer as a decimal `int32`. Without it, the code is 401 for `UNAUTHENTICATED`, 403 for `PERMISSION_DENIED`, 420 for `RESOURCE_EXHAUSTED`, 400 for `INVALID_ARGUMENT`, and 500 otherwise.
* The application error type comes from the `app_error.type_key` trailer and must match `^[A-Z0-9_]{1,64}$`. Without a valid trailer, the status message is used when it matches that pattern.
* Protocol error `metadata` holds only the trailers listed in the method's `error_metadata`, nothing else.

Check the registry in CI with the usual config check. It validates the registry file too, when `rpc.registry.enabled` is true:

```bash
centrifugo checkconfig -c config.json
```

### Not implemented (yet)

* Redis and PostgreSQL persistence for revocation.
* `user_connections` API.
* Redis replica offloading.
* Sharded PUB/SUB.
* Per-namespace engines.
* Custom controllers.

Some of these are on the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Install

Docker images are published to GitHub Container Registry for `linux/amd64` and `linux/arm64`:

```
docker pull ghcr.io/impulse-m/impulsem.centrifugo:1.0.0
docker run -p 8000:8000 -v "$(pwd)/config.json:/centrifugo/config.json" ghcr.io/impulse-m/impulsem.centrifugo:1.0.0
```

Image tags:
* `X.Y.Z`, `X.Y` and `latest` for releases;
* `master` and `sha-<commit>` for builds from the `master` branch.

Linux binaries are attached to [GitHub Releases](https://github.com/Impulse-m/ImpulseM.Centrifugo/releases).

## Versioning and releases

The fork uses its own semantic versioning starting at 1.0.0, independent of upstream version numbers. A release is made either by pushing a `vX.Y.Z` tag or by running the `release` workflow manually with the version (without `v`). The workflow runs the tests, publishes the images, creates the tag and the GitHub Release.

## Client SDKs

* JavaScript (browser, Node.js, React Native): [ImpulseM.Centrifugo.Client.JS](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS), npm package `@impulsem/centrifuge`.
* Swift (iOS): [ImpulseM.Centrifugo.Client.Swift](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Swift), Swift Package Manager.
* Java (Android): [ImpulseM.Centrifugo.Client.Java](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java), Maven repository on GitHub Pages.

## Related repositories

* [ImpulseM.Centrifugo.Core](https://github.com/Impulse-m/ImpulseM.Centrifugo.Core): fork of the [centrifuge](https://github.com/centrifugal/centrifuge) core library.
* [ImpulseM.Centrifugo.Protocol](https://github.com/Impulse-m/ImpulseM.Centrifugo.Protocol): fork of the [protocol](https://github.com/centrifugal/protocol) definitions.

## Upstream Centrifugo

Centrifugo is an open-source scalable real-time messaging server. Centrifugo can instantly deliver messages to application online users connected over supported transports (WebSocket, HTTP-streaming, Server-Sent Events (aka EventSource), GRPC, WebTransport). Centrifugo has the concept of channel subscriptions – so it's a user-facing PUB/SUB server.

Centrifugo is language-agnostic and can be used to build chat apps, live comments, multiplayer games, real-time data visualizations, collaborative tools, etc. in combination with any backend. It is well suited for modern architectures and allows decoupling the business logic from the real-time transport layer.

Several official client SDKs for browser and mobile development wrap the bidirectional protocol. In addition, Centrifugo supports a unidirectional approach for simple use cases with no SDK dependency.

### Documentation

* [Centrifugo official documentation site](https://centrifugal.dev)
* [Installation instructions](https://centrifugal.dev/docs/getting-started/installation)
* [Getting started tutorial](https://centrifugal.dev/docs/getting-started/quickstart)
* [Design overview and idiomatic usage](https://centrifugal.dev/docs/getting-started/design)
* [Build a WebSocket chat/messenger app with Centrifugo](https://centrifugal.dev/docs/tutorial/intro) tutorial
* [Centrifugal blog](https://centrifugal.dev/blog)
* [FAQ](https://centrifugal.dev/docs/faq)

### Why Centrifugo

The core idea of Centrifugo is simple – it's a PUB/SUB server on top of modern real-time transports:

<img src="https://centrifugal.dev/img/protocol_pub_sub.png?v=2" />

The hard part is to make this concept production-ready, efficient, flexible and available from different application environments. Centrifugo is a mature solution that already helped many projects with adding real-time features and scale towards many concurrent connections. Centrifugo provides a set of features not available in other open-source solutions in the area:

* Efficient real-time transports: WebSocket, HTTP-streaming, Server-Sent Events, GRPC, WebTransport
* Built-in scalability with Redis (or Redis Cluster, or Redis-compatible storage – ex. AWS Elasticache, Valkey, KeyDB, DragonflyDB, etc), or Nats.
* Simple HTTP and GRPC server API to communicate with Centrifugo from the app backend
* Asynchronous PostgreSQL and Kafka consumers to support transactional outbox and CDC patterns
* Flexible connection authentication mechanisms: JWT and proxy-like (via request from Centrifugo to the backend)
* Channel subscription multiplexing over a single connection
* Different types of subscriptions: client-side and server-side
* Various channel permission strategies, channel namespace concept
* Hot message history in channels, with automatic message recovery upon reconnect, cache recovery mode (deliver latest publication immediately upon subscription)
* Delta compression in channels based on Fossil algorithm
* Online channel presence information, with join/leave notifications
* A way to send RPC calls to the backend over the real-time connection
* Strict and effective client protocol wrapped by several official SDKs
* JSON and binary Protobuf message transfer, with optimized serialization and built-in batching
* Beautiful embedded admin web UI
* Great observability with lots of Prometheus metrics exposed and official Grafana dashboard
* And much more, visit [Centrifugo documentation site](https://centrifugal.dev)

## Credits

Centrifugo is created by [Centrifugal Labs](https://github.com/centrifugal) and licensed under the Apache License 2.0. This fork keeps the same license.
