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

* Bandwidth optimizations (#14), following the [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/bandwidth_optimizations) and keeping its config keys:
  * `keep_latest_publication` namespace or `channel.without_namespace` option: the node keeps the latest publication of each channel while it has subscribers, so delta compression also works for at-most-once subscribers without positioning or recovery. Requires `allowed_delta_types`; publishers still set `delta` or `delta_publish`. Not used for map subscriptions: map channels with subscribers still get a channel medium, which holds no base for keyed publications, so it adds per-channel state without a delta benefit. Memory cost is one latest publication per channel with local subscribers, kept whether or not the publication was sent as a delta. Clients allowed to publish (`allow_publish_for_subscriber`) choose those payload sizes, so the retained memory is channels with subscribers times the largest allowed payload. Enabling it for the first time (no channel used it at startup) requires a node restart, like `shared_position_sync`; removing it from every namespace also takes effect only at restart, until then each publication still looks up the channel options.
  * `allow_channel_compaction`: clients negotiate a numeric channel id on subscribe, which replaces the channel name in publication, join and leave pushes. Negotiated automatically by the JS and Java SDKs. Not for server-side subscriptions.
  * `client_publish_debounce_interval`: returned to clients on subscribe. The JS SDK sends the first publish at once and coalesces the rest of the interval into the latest value (per key for map subscriptions). Fire-and-forget: pending data is dropped on unsubscribe or disconnect, and errors of the delayed publish are not reported. Must be `0` (off) or a whole number of milliseconds, at least `1ms`.

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

Clients call existing gRPC methods of backend services through RPC, by method name, and the request and response bytes pass through unchanged. It is off unless `rpc.registry.enabled` is set. The config, error mapping, metrics and planned work are in [internal/rpcregistry/README.md](internal/rpcregistry/README.md).

### Client address behind trusted proxies

Behind a load balancer or a reverse proxy the server sees the proxy as the peer. `client_address` resolves the real client address from two sources, and both are believed only from the proxies listed in `trusted_proxies`:

* the forwarded header (`header`, `x-forwarded-for` by default), walked from right to left, skipping trusted proxies; an entry which is not an address makes the peer the client;
* a PROXY protocol v1 or v2 header (`proxy_protocol.enabled`) on the external HTTP port and the `uni_grpc` port. The header is read before TLS. A connection from a peer outside `trusted_proxies` which sends a PROXY header is closed by default (`untrusted_peers: reject`), or has the header discarded with `untrusted_peers: ignore`. A connection from such a peer without a header uses its socket address.

With PROXY protocol, the address the PROXY header reports is the previous hop, and it decides whether the forwarded header is read. So for a chain of an L7 proxy, then an L4 balancer which sends PROXY, then the server, list both the balancer, which is the socket peer, and the L7 proxy, whose forwarded header is read. Then the client address resolves.

```json
"client_address": {
  "enabled": true,
  "trusted_proxies": ["10.0.0.0/8", "192.0.2.7"],
  "header": "x-forwarded-for",
  "proxy_protocol": {
    "enabled": false,
    "untrusted_peers": "reject",
    "read_header_timeout": "10s"
  }
}
```

Off by default, and then the auth throttle and the request log keep their previous rules. When on, the resolved address is used by the auth throttle and the request log, and sent to RPC registry targets as `x-forwarded-for`, or omitted when it is unknown. Not yet exposed to the connect proxy or the connections API. PROXY protocol does not apply to HTTP/3 or WebTransport, which are UDP; the header walk still does. Internal HTTP and gRPC API ports do not read PROXY headers unless they share the external port.

### Channel state events and cache empty events

Two backend hooks, both off by default and both as in the Centrifugo PRO docs (`event_hooks`, `channel_events`, `channel_cache_empty`):

* Channel state events: an `occupied` event when a channel gets its first subscriber in the cluster and a `vacated` event when it loses its last one. Events are sent in batches to `channel.proxy.state`, over HTTP or gRPC, with `state_proxy_enabled` on the namespace. They need `presence: true` and the Redis presence manager. Not yet delivered by this build: the Redis wiring follows in the next release, and until then the state proxy is configured but no events are sent.
* Cache empty events: a synchronous call to `channel.proxy.cache_empty` when a subscriber in cache recovery mode finds no latest publication. The proxy answers with `populated`. Enabled with `cache_empty_proxy_enabled` and `cache_empty_proxy_name` on the namespace.

```json
"engine": {
  "type": "redis",
  "redis": {
    "channel_state": {"vacated_event_delay": "5s", "num_partitions": 128}
  }
},
"channel": {
  "proxy": {
    "state": {"endpoint": "http://backend:8000/channel_state", "timeout": "1s"},
    "cache_empty": {"endpoint": "http://backend:8000/cache_empty", "timeout": "1s"}
  },
  "namespaces": [
    {
      "name": "chat",
      "presence": true,
      "state_proxy_enabled": true,
      "history_size": 1,
      "history_ttl": "300s",
      "force_recovery": true,
      "force_recovery_mode": "cache",
      "cache_empty_proxy_enabled": true
    }
  ]
}
```

The payloads are the JSON of `NotifyChannelStateRequest` and `NotifyCacheEmptyRequest` in `internal/proxyproto/proxy.proto`:

```json
{"events": [{"time_ms": 1697206286533, "channel": "chat:index", "type": "occupied"}]}
```

Answer `{"result": {}}`; an `error` object or a non-200 status makes the batch be sent again. Cache empty request: `{"channel": "chat:index"}`; answer `{"result": {"populated": true}}` with a JSON body: a 200 with no body is an error. An unreachable cache empty proxy rejects the subscription with an internal error.

Once the Redis wiring ships, the delivery rules below apply to channel state events. This build does not send them yet.

* Delivery is at-least-once, in partition order: a batch whose answer is lost is sent again, so the backend must treat a repeated event as a no-op. Events of one channel reach the backend in the order they happened.
* Retries back off from 100 ms to 20 s. Events are kept in Redis while the backend is down; the stream holds at most 8192 events per partition, and new events are dropped when it is full.
* A batch the backend keeps rejecting (an `error` answer or a non-200 status) is retried at the 20 s cap until the backend accepts it. The events of the other channels in the same partition wait behind it, and the stream fills up as above. The backend must accept every batch it can decode: a 200 with an empty result counts as delivered, while an `error` answer, a 200 with no body or a non-200 status is retried. There is no skip of a batch. A rejected batch is logged at most once a minute, and `centrifugo_channel_state_oldest_failing_batch_timestamp_seconds` holds the time the oldest batch that still fails was first sent (0 when none fails): alert when `time() - that value` grows.
* Redis must not evict these keys: set `maxmemory-policy noeviction`. Eviction loses events.
* Use a separate Redis presence manager (`presence_manager.redis.channel_state`) when the engine is shared with other data, as PRO recommends.
* Do not change `num_partitions`, or the number of Redis shards, while channels are occupied: their state is orphaned and a `vacated` event can be missed.
* Startup rules: `state_proxy_enabled` requires `presence` and a Redis presence manager (`engine.type: redis`, or `presence_manager.type: redis`); `cache_empty_proxy_enabled` requires `force_recovery_mode: cache`. Other options are checked on startup too. A reload (SIGHUP) that enables or disables a cache empty proxy, adds, removes or renames one, is rejected, and the old config stays: restart to change it. A reload does not change the endpoint or timeout of a cache empty proxy either: restart for that.
* Differences from PRO: there is no `state_proxy_name`, a single `channel.proxy.state` endpoint serves all namespaces; the stream cap of 8192 events per partition is fixed.

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
