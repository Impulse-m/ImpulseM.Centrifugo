# ImpulseM.Centrifugo

ImpulseM fork of [Centrifugo](https://github.com/centrifugal/centrifugo): upstream Centrifugo v6 plus features needed at messenger scale. Upstream is merged regularly (last sync: upstream master of 2026-10, v6.9.x), and a weekly check opens an issue when the fork falls behind. The features are implemented from the public [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/overview), keeping its config keys and API shapes. Planned work is tracked in the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Added on top of upstream

* [Server-side publication tags filter](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7): a per-subscriber filter set by the backend that the client can't widen. The filtering itself is upstream's Core implementation; the fork lets the backend set it from the subscription token, connection token `subs`, and subscribe and connect proxy results. Recovery applies the filter; an explicit history call on a filtered subscription is refused, since history is not filtered.
* Server tags filter template (roadmap item 18): a namespace filter rendered for each connection from its token claims, user and client, so every subscriber gets a filter of its own value, for example echo suppression by session. Combined with an explicit server tags filter by `and`; see [below](#server-tags-filter-template).
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

### Server tags filter template

A namespace can set `server_tags_filter_template`: a server tags filter applied to every subscription in the namespace made on a connection, including server-side subscriptions of the connection token and of the connect proxy, the personal channel and uni-connect channels. Its `val` and `vals` strings may refer to values of the connection. Each subscription gets the template rendered with those values, combined with its explicit server tags filter by `and`. Off by default, and nothing is read from the token unless a template refers to a claim.

```json
"channel": {
  "namespaces": [
    {
      "name": "chat",
      "allow_subscribe_for_client": true,
      "server_tags_filter_template": {
        "filter": {"key": "session", "cmp": "neq", "val": "{{claims.sid}}"},
        "on_missing_value": "reject"
      }
    },
    {
      "name": "tenant",
      "server_tags_filter_template": {
        "filter": {
          "op": "and",
          "nodes": [
            {"key": "tenant", "cmp": "eq", "val": "{{claims.tenant}}"},
            {"key": "audience", "cmp": "in", "vals": ["all", "user:{{user}}"]}
          ]
        },
        "on_missing_value": "reject"
      }
    }
  ]
}
```

* `filter` is a server tags filter tree in the same JSON form as the `server_tags_filter` of the subscription token: `op`, `key`, `cmp`, `val`, `vals`, `nodes`. Variables are allowed only in `val` and in `vals` entries. An empty `filter` means no template. Unknown fields are a config error. The tree is checked at config load with the rules of Core's tags filter, so an unknown `cmp`, an `in` without `vals` and a `not` with two children are config errors. `filter` has no environment variable of its own; set namespaces as one JSON value.
* Variables: `{{claims.<path>}}` reads a gjson path of the connection token claims (`sid`, `org.tenant`, `roles.0`), as `user_id_claim` does; `{{user}}` is the user ID of the connection; `{{client}}` is the client ID. Whitespace inside the braces is trimmed, and literal text around a variable is allowed, as in `"user:{{user}}"`. Any other variable, a `{{` without `}}`, and a `{{` in `op`, `key` or `cmp` are config errors.
* Values: string claims as they are; numbers as written in the token, so 64-bit integer IDs are exact; `true` and `false`. Null, absent, object, array and empty string are missing. An anonymous connection has no user, so `{{user}}` is missing.
* `on_missing_value`: `reject` (default) refuses a subscription whose template has a missing value: error 103 on subscribe and subscription refresh, disconnect 3507 at connect for a server-side subscription. `skip` applies no template to that subscription, and the explicit filter still applies. `skip` removes the whole template, including the clauses that isolate data: a missing claim under `skip` gives the subscriber the publications of every tenant tagged on the channel. `skip` is safe only for a template whose clauses do not isolate data, such as an optional audience; use `reject` for an isolation filter, as the `tenant` example does. Setting `on_missing_value` without `filter` is a config error. The log names the variable.
* Values are bound once per connection, at connect, from the verified token. Subscription refreshes render with the same values, so a refresh without a filter keeps the current filter, and a refresh with a new explicit filter keeps the template. A token refresh made while the config has no templates is not checked against the stored claims, which costs nothing when templates are off; after a reload which switches templates off and on again, the connection renders with the claims it stored until it reconnects, and the next refresh with other claims disconnects it.
* A connection refresh whose token has other values of the template claims disconnects with insufficient state, and the client reconnects with the new values. Equal values change nothing. A connection verified before a reload which adds a template has no stored claims, so its next token refresh with a value for a template claim disconnects it once; the reconnect binds the values, so an operator may see one such disconnect per long-lived connection after the reload.
* A connection which connected without a token, through the connect proxy or anonymously, has no claims bound. Its token refreshes do not compare claims, and its claim variables stay missing for its life, so a refresh never disconnects it for them and it never loops through the proxy. A token subscription which a template refuses for a missing claim is left out of the refresh, so it is not subscribed without its template. The refresh is answered when that channel is not subscribed. A channel the connection still holds disconnects it with insufficient state instead, and its reconnect does not subscribe the channel without its template. Only a rebind to a token binds claims to it.
* A rebind to a token with other values, or to another user, unsubscribes the channels with a template with code 2500 (insufficient state), and the client resubscribes them with fresh filters. The server-side channels of the connection come back too: the token subscriptions, the personal channel, and the uni-connect and connect proxy channels which connect stored with a template, which are subscribed again with filters rendered with the new values. A connection which holds a uni-connect or connect proxy channel of a templated namespace that connect did not subscribe with a template, because the template was configured later, is disconnected with insufficient state by a rebind, before the rebind moves it. The client reconnects, the connect proxy and the token run again, and every channel comes back with the current template: no channel stays unfiltered, and none is lost silently. A connection which stored no template values, because it connected while the templates were off, counts as changed at a rebind when the config has templates: its templated client channels are unsubscribed with code 2500 and resubscribed with the template. The user is one of the values: a connection without a user which takes one renders its templates with that user, so the rebind changes them even when the claims are equal. A refused template of a rebind refuses the refresh, and the connection keeps its identity. A template which a config reload adds while the rebind runs, after the rebind was checked, refuses a subscription of the new identity once the rebind is applied, so the connection is disconnected. A rebind which Core refuses changes neither the template values nor the template channels. Other channels are not covered by this: a rebind to an anonymous identity unsubscribes the user channels before the rebind is applied, so a refused one can leave them unsubscribed.
* A connect which a template refuses does not disconnect the other connection of its user: with templates in the config, the template is checked before the single connection of the user is enforced, so the single connection check runs after the connect checks which fail with an error (an invalid channel name, the personal channel), and such a refused connect does not disconnect the other connection. Without templates the order is unchanged.
* A subscription which overlaps a rebind can keep the filter of the previous identity for the length of one commit: a publication which matches that filter can reach the channel until the unsubscribe with code 2500 arrives. Subscriptions which start after the rebind render with the new values.
* The server API subscribe is refused on a channel with a template, with error 108 (not available), since no connection values exist to render it with. Subscriptions made by a connection are not affected.
* Presence, join/leave, `force_push_join_leave` and the map presence prefixes are config errors on a namespace with a template. Their messages and presence lists are not filtered by the template, so they would show the other subscribers of the channel. A namespace with `subscription_type` `shared_poll` is a config error with a template too, since its subscriptions do not apply the server tags filter of the subscribe.
* A connect proxy connection has no token claims: `{{user}}` and `{{client}}` work, and `{{claims.*}}` is missing.
* A config reload which adds a template does not change live subscriptions. A connection verified before the reload has no claims, so its claim variables are missing under the policy until it reconnects, and its next token refresh with a value for a template claim disconnects it (see above).

Consequences inherited from the server tags filter: every subscription in a templated namespace carries a server filter, so delta compression is not negotiated there. Explicit `history` is refused on every channel of a templated namespace, subscribed or not, with error 103, because history reads publications without the subscription and would show the publications the template keeps apart. Recovery applies the filter.

Publications without the tag: `neq` and `nin` match a publication which has no such tag, so `session != X` delivers untagged publications, while `eq` and `in` drop them. Tag every publication of a namespace with the session, or use `eq` where untagged publications must not reach the subscriber.

Not supported yet: array or object claims as values, `{{session}}`, values from the connect proxy result.

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
