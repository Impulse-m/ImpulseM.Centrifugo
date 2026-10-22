# ImpulseM.Centrifugo

A fork of [Centrifugo](https://github.com/centrifugal/centrifugo): upstream Centrifugo v6 plus features for large deployments. Upstream is merged regularly (last sync: upstream master of 2026-10, v6.9.x), and a weekly check opens an issue when the fork falls behind. The features are implemented from the public [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/overview), keeping its config keys and API shapes. Planned work is tracked in the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Added on top of upstream

* [Server-side publication tags filter](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7): a per-subscriber filter set by the backend that the client can't widen. The filtering itself is upstream's Core implementation; the fork lets the backend set it from the subscription token, connection token `subs`, and subscribe and connect proxy results. Recovery applies the filter; an explicit history call on a filtered subscription is refused, since history is not filtered.
* [Server tags filter template](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/18): a namespace filter rendered for each connection from its token claims, user and client, so every subscriber gets a filter of its own value, for example echo suppression by session. Combined with an explicit server tags filter by `and`; see [below](#server-tags-filter-template).
* `require_server_tags_filter` channel option (namespace or `channel.without_namespace`): every subscriber of the namespace must carry a server tags filter, so a subscription which ends up without one is refused whichever route made it, and client history calls on the namespace are refused. A forgotten route fails closed instead of giving an unfiltered subscription.
  * Client subscribe without a filter: permission denied (103). Covers a subscription token without the claim, a subscribe proxy answer without `server_tags_filter` (for example one whose key was spelled `serverTagsFilter`, which the HTTP decoder drops silently), user-limited and `allow_subscribe_for_client` channels, and map subscriptions.
  * Connect-time server-side subscriptions the backend asserts, a connection token `channels` claim or a `subs` entry without the claim, a connect proxy `channels` list or a `null` `subs` entry: the connection is refused with 3507, which SDKs do not retry. This is the cost of failing closed on the connect path: one such channel locks the user out of the whole connection, not only that channel, so roll the option out per namespace after the token and connect proxy code carries filters. A channel a client asks for at connect is dropped and the connection is made.
  * Rebind: a token whose subscriptions lack a filter is refused as a refresh error (103) before the connection moves.
  * Server API `subscribe`, which cannot carry a filter: not available (108).
  * Client `history()` on the namespace: permission denied (103). Subscribe-time recovery is still allowed and filtered; the server API `history` is unaffected.
  * With `server_tags_filter_template`: a filter the template produces counts, so `on_missing_value: skip` with this option refuses a subscription whose template was skipped and which has no explicit filter.
  * Config errors: `subscription_type: shared_poll` and `subscribe_stream_proxy_enabled` (those subscriptions do not apply the filter), the personal channel namespace (without a template), and `client.insecure_skip_token_signature_verify` (an unverified token can carry any filter).
  * Turning it on at a config reload applies to new subscriptions; existing unfiltered subscriptions live until they resubscribe or reconnect.
  * Off by default and then free. Not a PRO option: PRO has no way to require the filter.
* [User blocking and token revocation](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/10):
  * `block_user`, `unblock_user`, `revoke_token` and `invalidate_user_tokens` server API methods;
  * extensions for session-based revocation and revocation generations;
  * an in-memory store on every node, live disconnects and cluster propagation.
  * An entry is kept until the request's `expire_at`. Without it `access_revoke.default_ttl` (20 minutes) applies, so a revocation that must hold until the token expires sets `expire_at` to that expiration. A permanent block sets a far-future `expire_at`, which needs `access_revoke.max_ttl` off.
  * `access_revoke.max_ttl` (off by default, `0`) caps how long an entry may be kept, counted from the time it is applied. The API refuses an `expire_at` beyond now plus `max_ttl` with bad request, and the Kafka and notify paths clamp it to that limit. A revocation is clamped, not dropped, and each clamp is counted in `centrifugo_access_revoke_expire_at_clamped_total{source}`. A `default_ttl` beyond `max_ttl` is clamped to `max_ttl` for a request which sends no `expire_at`, and that clamp is counted under `source="api"`. Without it the store has no entry cap and keeps a far-future entry for its whole life, so watch `centrifugo_access_revoke_store_size`.
  * API calls reach the other nodes over the broker at most once: a node that misses the message (a dropped PUB/SUB message, a restart) does not apply it. The opt-in [revocation source](#access-revocation-event-source) is the durable delivery path.
  * A live disconnect which finds the disconnect queue full (`access_revoke.disconnect_queue_size`) is dropped and retried by user or session once the queue has room, so a blocked user or an invalidated token is still disconnected. Each connection is queued at most once, and a connection whose close has begun is never queued again. Each dropped user or session keeps one retry key, so the keys cost no more than the dropped disconnects, and a retry walks only the connections of its key. Size `access_revoke.disconnect_rate` and `access_revoke.disconnect_queue_size` against the connections of a node: a mass revocation drains at the rate, and `centrifugo_access_revoke_disconnect_queue_fill` shows the backlog.
  * Differences from Centrifugo PRO:
    * `revoke_token` without `user` disconnects at once the connections with no user (anonymous and pending) which hold the session, through a per-node session index; connections with a user are disconnected at once only when `user` is passed, otherwise at their next refresh. A token without `exp` keeps such a connection until it reconnects. No revocation scans the connections of a node; only the one-shot catch-up pass after a fail-open start does.
    * `expire_at` in the past is rejected with bad request.
    * Extensions: `user` and `revoked_at` on `revoke_token`; `disconnect` on `revoke_token`, a code in 4000-4999 and a reason of at most 123 bytes that every close of the revocation uses instead of 3005; `generation` and `revoked_at` on `invalidate_user_tokens`; `token_revoke.uid_claim` and `user_tokens_invalidate.generation_claim` config; disconnect code 4500 for blocked users; loop guard disconnects with 3500.
  * Not implemented:
    * subscription tokens are not covered by revocation, only connection tokens;
    * `invalidate_user_tokens.channel` is not available;
    * `storage_type` is memory only: `redis` and `database` are rejected at startup. Use the per-node revocation source (`access_revoke.source`) as the durable delivery path.
* `unsubscribe` server API method takes an optional `unsubscribe` code and reason, for the HTTP, gRPC, batch and consumer APIs: a code in 2100-2499 leaves clients unsubscribed, a code in 2600-2999 makes them subscribe again at once, so the subscribe proxy runs again and a changed server tags filter applies. The reason is at most 123 bytes. Without the field the default code 2000 applies; an invalid code or reason is a bad request, and in a consumer it is logged and the default code is used.
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
  * Operation classification: map and shared poll subscriptions count as `subscribe`. A publish with map type counts as `map_publish`, or as `map_remove` when it removes the key, whatever the channel: Core routes it to its map handlers by type, not by the channel's subscription type, so a map type publish on a stream channel is still counted as a map operation. A regular publish to a map channel counts as `publish`. On a shared poll channel, track and untrack of keys count as `track` and `untrack` (`sub_refresh` with type 1 and 2). The classification is by request type alone, as Core routes it, so a request of type 1 or 2 without a track batch is charged to `track` or `untrack` before Core rejects it. On any other channel a `sub_refresh` refreshes the subscription token and counts as `sub_refresh` whatever its type. `history`, `presence` and `presence_stats` on map channels are plain commands and count as before.
  * Upgrade: map publishes and map removes used to count as `publish`, and track and untrack as `sub_refresh`. They now have their own buckets, which fall back to `default` when unset. A deployment with `publish` set and no `default` no longer limits map publishes. Set `map_publish`, `map_remove`, and for shared poll `track` and `untrack`, explicitly.
  * Upgrade: `redis_user_command` keys carry the user ID with `%` and `}` escaped. A user ID with either character gets new Redis keys, so its counters start empty once after the upgrade, and its budget refills at once. Other user IDs keep their keys.

* Bandwidth optimizations (#14), following the [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/bandwidth_optimizations) and keeping its config keys:
  * `keep_latest_publication` namespace or `channel.without_namespace` option: the node keeps the latest publication of each channel while it has subscribers, so delta compression also works for at-most-once subscribers without positioning or recovery. Requires `allowed_delta_types`; publishers still set `delta` or `delta_publish`. Not used for map subscriptions: map channels with subscribers still get a channel medium, which holds no base for keyed publications, so it adds per-channel state without a delta benefit. Memory cost is one latest publication per channel with local subscribers, kept whether or not the publication was sent as a delta. Clients allowed to publish (`allow_publish_for_subscriber`) choose those payload sizes, so the retained memory is channels with subscribers times the largest allowed payload. Enabling it for the first time (no channel used it at startup) requires a node restart, like `shared_position_sync`; removing it from every namespace also takes effect only at restart, until then each publication still looks up the channel options.
  * `allow_channel_compaction`: clients negotiate a numeric channel id on subscribe, which replaces the channel name in publication, join and leave pushes. Negotiated automatically by the JS and Java SDKs. Not for server-side subscriptions.
  * `client_publish_debounce_interval`: returned to clients on subscribe. The JS SDK sends the first publish at once and coalesces the rest of the interval into the latest value (per key for map subscriptions). Fire-and-forget: pending data is dropped on unsubscribe or disconnect, and errors of the delayed publish are not reported. Must be `0` (off) or a whole number of milliseconds, at least `1ms`.

* `client.refresh_lead`, for example `"60s"`: the connect and refresh replies tell a client with client-side refresh to refresh that much before its connection expires, and the reply carries the time left minus the lead, but never less than half of the time left. The connection still expires at its own time, and `client.expired_close_delay` applies after it. Whole seconds, zero means no lead. Read at start; a reload does not change it. A token which lives at least twice the lead gets the full lead. The lead should exceed the backend's clock skew plus the 99th percentile of the client's token fetch. SDKs report the ttl of the auth event as the time to the refresh. Off by default and then free. The change is in the Core commit `bc8c5cab476ea34125c2ed10b739d1f257a3df69` of the fork, which `go.mod` pins.
* `client.max_messages_in_frame`, as in the [PRO docs on message batching](https://centrifugal.dev/docs/pro/client_msg_batching): the maximum number of messages joined into one transport frame, default `16`, `-1` for no limit. `1` writes each message in its own frame, so `websocket.write_timeout` covers one reply. Only frame boundaries change: the same messages leave in the same order. The other batching keys of that page are not implemented.

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
* Records are not signed. The topic ACL is the only check on who may write them, so the topic must have one producer. Every node reads the whole topic, so the broker egress grows with the node count.

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
      "name": "topic",
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
* `on_missing_value`: `reject` (default) refuses a subscription whose template has a missing value: error 103 on subscribe and subscription refresh, disconnect 3507 at connect for a server-side subscription. A channel which the client names in the connect and whose template refuses it fails the whole connect with 3507, where upstream drops an unauthorized name. `skip` applies no template to that subscription, and the explicit filter still applies. `skip` removes the whole template, including the clauses that isolate data: a missing claim under `skip` gives the subscriber the publications of every tenant tagged on the channel. `skip` is safe only for a template whose clauses do not isolate data, such as an optional audience; use `reject` for an isolation filter, as the `tenant` example does. A connection without a token (connect proxy, or anonymous connect without a token) has every `{{claims.*}}` missing, and a client takes that path by omitting its token whenever a connect proxy or anonymous connect is enabled: under `skip` it subscribes without the template, under `reject` it is refused. Setting `on_missing_value` without `filter` is a config error. The log names the variable.
* Values are bound once per connection, at connect, from the verified token. Subscription refreshes render with the same values, so a refresh without a filter keeps the current filter, and a refresh with a new explicit filter keeps the template. A connection which stored values is checked on a token refresh even while the config has no templates. A reload which removed the templates leaves its channels filtered with the claims it stored, and the refresh carries no claims to render them again, so the connection is disconnected with insufficient state once; the reconnect stores no values, and a connection made while the templates are off is not checked.
* A connection refresh whose token has other values of the template claims disconnects with insufficient state, and the client reconnects with the new values. Equal values change nothing. A connection verified before a reload which adds a template has no stored claims, so its next token refresh with a value for a template claim disconnects it once; the reconnect binds the values, so an operator may see one such disconnect per long-lived connection after the reload.
* A connection which connected without a token, through the connect proxy or anonymously, has no claims bound. Its token refreshes do not compare claims, and its claim variables stay missing for its life, so a refresh never disconnects it for them and it never loops through the proxy. This holds for a refresh of the same identity too: a token refresh which carries claims does not bind them, only a rebind to a token does. A token subscription which a template refuses for a missing claim is left out of the refresh, so it is not subscribed without its template. The refresh is answered when that channel is not subscribed. A channel the connection still holds disconnects it with insufficient state instead, and its reconnect does not subscribe the channel without its template. Only a rebind to a token binds claims to it.
* A rebind to a token with other values, or to another user, unsubscribes the channels with a template with code 2500 (insufficient state), and the client resubscribes them with fresh filters. The channels which the connection subscribed with a template are recorded, so a reload which removed the template of a channel does not keep its filter: the rebind unsubscribes it too. A rebind with the same values unsubscribes nothing for a connection which stored its values, so a channel subscribed before a reload added the template to its namespace keeps its subscription without the template until the client unsubscribes and subscribes it again. The server-side channels which the connection still holds come back too: the token subscriptions, the personal channel, and the uni-connect and connect proxy channels which connect stored with a template and which the new identity keeps, are subscribed again with filters rendered with the new values. A uni-connect channel which an earlier rebind removed, for example by a rebind to anonymous, is not restored by a later one: the client reconnects to get it back. A rebind to anonymous does not subscribe the uni-connect and connect proxy channels again: the anonymous identity keeps only the client channels that its namespace allows, as on connect. A connection which holds a uni-connect or connect proxy channel of a templated namespace that connect did not subscribe with a template, because the template was configured later, is disconnected with insufficient state by a rebind, before the rebind moves it. The client reconnects, the connect proxy and the token run again, and every channel comes back with the current template: no channel stays unfiltered, and none is lost silently. A connection which stored no template values, because it connected while the templates were off, counts as changed at a rebind when the config has templates: its templated client channels are unsubscribed with code 2500 and resubscribed with the template. The user is one of the values: a connection without a user which takes one renders its templates with that user, so the rebind changes them even when the claims are equal. A refused template of a rebind refuses the refresh, and the connection keeps its identity. A template which a config reload adds while the rebind runs, after the rebind was checked, refuses a subscription of the new identity once the rebind is applied, so the connection is disconnected. The same reload leaves a uni-connect or connect proxy channel of a templated namespace which connect subscribed without a template: the connection is disconnected with insufficient state before its channels are unsubscribed, so the channel is not lost. A rebind which Core refuses changes neither the template values nor the template channels. Other channels are not covered by this: a rebind to an anonymous identity unsubscribes the user channels before the rebind is applied, so a refused one can leave them unsubscribed.
* A connect which a template refuses does not disconnect the other connection of its user: with templates in the config, the template is checked before the single connection of the user is enforced, so the single connection check runs after the connect checks which fail with an error (an invalid channel name, the personal channel), and such a refused connect does not disconnect the other connection. Without templates the order is unchanged.
* A subscription which overlaps a rebind can keep the filter of the previous identity for the length of one commit: a publication which matches that filter can reach the channel until the unsubscribe with code 2500 arrives. Subscriptions which start after the rebind render with the new values.
* A rebind with other values is applied by Core before the templated channels are unsubscribed. Until that unsubscribe, the channels of the connection keep the filters rendered with the previous identity, while the connection already has the new identity. A publication which matches such a filter can reach the connection under the new identity during that window, which is short but not zero. Unsubscribing before the rebind would close it, at the cost of losing the channels when the rebind fails, so the order is kept.
* A connection which connected while the templates were off and has no stored template values counts as changed on a token refresh when the config has templates. It is disconnected with insufficient state, even when the refresh token carries no value for the template claims, and its reconnect binds the values of its token.
* The server API subscribe is refused on a channel with a template, with error 108 (not available), since no connection values exist to render it with. Subscriptions made by a connection are not affected. The server API history is not refused: the API key is trusted and reads every publication of a channel, as it does without a template.
* Presence, join/leave, `force_push_join_leave` and the map presence prefixes are config errors on a namespace with a template. Their messages and presence lists are not filtered by the template, so they would show the other subscribers of the channel. A namespace with `subscription_type` `shared_poll` is a config error with a template too, since its subscriptions do not apply the server tags filter of the subscribe. So is `subscribe_stream_proxy_enabled`: the stream writes its publications to the client without the server tags filter, so a template there would not filter them.
* A connect proxy connection has no token claims: `{{user}}` and `{{client}}` work, and `{{claims.*}}` is missing, so a subscription which uses a claim is refused under `reject` and subscribed without the template under `skip`.
* A config reload which adds a template does not change live subscriptions. A connection verified before the reload has no claims, so its claim variables are missing under the policy until it reconnects, and its next token refresh with a value for a template claim disconnects it (see above). A client channel subscribed before the reload keeps its unfiltered subscription until the client resubscribes, so a token refresh with the same claims does not untemplate it. A reload which removes a template path, such as `claims.sid`, makes the stored value of that claim differ from the empty value of a new token, so the next token refresh disconnects the connection once with insufficient state; the reconnect binds the claims again.

Consequences inherited from the server tags filter: every subscription in a templated namespace carries a server filter, so delta compression is not negotiated there. Explicit `history` is refused on every channel of a templated namespace, subscribed or not, with error 103, because history reads publications without the subscription and would show the publications the template keeps apart. Recovery applies the filter.

Publications without the tag: `neq` and `nin` match a publication which has no such tag, so `session != X` delivers untagged publications, while `eq` and `in` drop them. Tag every publication of a namespace with the session, or use `eq` where untagged publications must not reach the subscriber.

Not supported yet: array or object claims as values, `{{session}}`, values from the connect proxy result.

### Per-namespace engines and custom controller

A namespace can use its own broker and presence manager, so a namespace with heavy fan-out does not share a Redis instance, its PUB/SUB connections or its history keys with latency-sensitive channels. Control traffic (node pings, disconnects, subscribe and unsubscribe commands, revocations) can move off the broker to a `controller` of type `redis` or `nats`. The settings are the `brokers`, `presence_managers` and `map_brokers` lists, the `broker_name`, `presence_manager_name` and `map.broker_name` channel options, and the `controller` section. Off by default: without enabled `brokers`, `presence_managers` or `map_brokers` entries and without these channel options, nothing is created and no routing hook is set.

```json
{
  "brokers": [
    {"enabled": true, "name": "broadcast", "type": "redis", "redis": {"address": "redis://10.0.0.2:6379", "prefix": "centrifugo-broadcast"}},
    {"enabled": true, "name": "rates", "type": "nats", "nats": {"url": "nats://localhost:4222"}}
  ],
  "presence_managers": [
    {"enabled": true, "name": "presence", "type": "redis", "redis": {"address": "redis://10.0.0.3:6379"}}
  ],
  "channel": {
    "without_namespace": {"broker_name": ""},
    "namespaces": [
      {"name": "broadcast", "broker_name": "broadcast", "presence_manager_name": "presence"},
      {"name": "rates", "broker_name": "rates"}
    ]
  },
  "controller": {"enabled": true, "type": "redis", "redis": {"address": "redis://localhost:6379"}}
}
```

Rules:

* `brokers[].type` is `memory`, `redis`, `nats` or `postgres`; `presence_managers[].type` is `memory` or `redis`; `map_brokers[].type` is `memory`, `redis` or `postgres`. Each entry has the same connection options as the default `broker`, `presence_manager` and `map_broker` sections.
* A disabled entry is ignored entirely: it is not created and not connected. Its name, the uniqueness of its name and its type are still checked, so a disabled entry with an unknown type fails validation. A namespace which references a disabled or unknown entry fails validation. An enabled entry is created at startup even when no namespace references it yet, so its connection is checked at startup.
* `broker_name` and `presence_manager_name` apply to namespaces and to `channel.without_namespace`. An empty value means the default broker or presence manager. For a stream channel the selected broker serves publish, join and leave publications, history, stream position, and subscribe and unsubscribe. For a map channel (`map`, `map_clients` and `map_users` subscription types) the selected broker is not used: the state, the map stream reads, the stream position, and the subscribe and unsubscribe of a map channel use the map broker which `map.broker_name` selects, as described below. Publish is refused for a map channel, so the selected broker has no effect on it. A namespace with `join_leave` on, a map subscription type and a `broker_name` is refused at config load, since the join and leave messages of a map channel go through the broker of the channel, which does not subscribe it; the same combination is refused on `channel.without_namespace`.
* `map.broker_name` selects an entry of `map_brokers` for the map channels (`map`, `map_clients` and `map_users` subscription types) of a namespace, and it is also accepted on `channel.without_namespace`. An empty value means the default `map_broker`. The selected map broker keeps the state, the stream and the pub/sub of those channels; the rules of the default `map_broker` apply to it.
* Two Redis brokers on the same Redis must have different prefixes. The rule covers the default `broker` (also with `type` `redisnats`) or `engine`, `brokers[]`, the default `map_broker` and `map_brokers[]`, except that the default `broker` or `engine` and the default `map_broker` are not compared with each other, since they have always been accepted together. Hosts are compared as the connection uses them: `cluster_address` takes precedence over `sentinel_address`, which takes precedence over `address`; comma-separated hosts count one by one, and the `addr` parameter of an address URL adds hosts. Two brokers conflict when they share a host (whatever its scheme, user information or database) and their channel spaces overlap. A channel space is the prefix followed by `.client.`, so the spaces overlap when the prefixes are equal or one prefix continues the other at a token boundary: `a` and `a.client` conflict, `a` and `ab` do not. A host without a port counts as the host with port 6379, so `redis://10.0.0.2` and `10.0.0.2:6379` are the same host. In a config file an empty prefix is the default `centrifugo`; `centrifuge` is the default for values built in code. An empty address is `127.0.0.1:6379`. Standalone and sentinel hosts are compared as written, so `localhost` and `127.0.0.1` are different hosts. A sentinel broker is compared by its sentinel hosts and by its `sentinel_master_name`, so two sentinel brokers with the same master conflict even when their sentinel hosts differ. A sentinel broker and a standalone broker of the same master with the same prefix are not refused, since the master is not resolved; this matters only when sharded PUB/SUB is enabled. PUB/SUB is global across databases, so a different `db` does not separate them. The server does not turn on sharded PUB/SUB, so each channel is subscribed on one broker only. The rule keeps the sharded PUB/SUB mode safe: there, brokers with one prefix subscribe the same channels and each publication would be delivered twice. A broker with `cluster_address` or a `redis+cluster://` address conflicts with every other broker of the same prefix, whatever its hosts, since cluster hosts are not resolved when the configuration is validated. The control channels of a Redis controller are compared the same way, and so are the control channels of the default broker or engine when no controller is enabled, since Core makes it the controller then. Presence managers have no such rule, since a channel has one presence manager.
* Two Postgres brokers (the default `broker`, `brokers[]`, the default `map_broker` and `map_brokers[]`) with the same `dsn` and the same `table_prefix` conflict, except that the default `broker` and the default `map_broker` are not compared with each other: each broker's outbox worker reads every row of its tables, so each publication is delivered once per broker. Two `dsn` values reach the same database when their hosts (case-insensitive), ports and database names are equal, so `postgres://h/db` and `postgresql://h:5432/db` conflict; the user and the other parameters are not compared, and a `dsn` which cannot be parsed is compared as written. The table prefix is compared after trailing underscores are trimmed, with an empty prefix as `cf`.
* Two NATS subjects conflict when a publication to one reaches the subscription of the other, and the configuration is refused. The subjects compared are the subject spaces of the default `broker` (also with `type` `redisnats`) and of `brokers[]` with `type` `nats`, and the subjects of a `controller` of type `nats`, which are `<prefix>.control` and `<prefix>.node.<id>`. The subject space of a broker is its `prefix` followed by `.client.`, or its `raw_mode.prefix` in raw mode. Two spaces conflict when one is a string prefix of the other: `prefix: "a"` and `prefix: "a.client"` conflict, a raw root `a` and `prefix: "ab"` conflict, since a raw broker publishes the channel `b.client.x` to `ab.client.x`, while `prefix: "a"` and `prefix: "ab"` do not. Brokers which are not in raw mode with equal prefixes do not conflict unless one allows wildcard subscriptions, since a channel name is routed to one broker. Otherwise wildcards do not change the rule, since a wildcard subscription stays inside its space. A broker with `raw_mode` whose root is `centrifugo.` conflicts with a controller of type `nats` with `prefix: "centrifugo"`. When no controller is enabled, Core makes the default `broker` of type `nats` or `redisnats` the controller, so its control subjects are compared too: a raw root that reaches them conflicts with the default broker itself, and a named broker whose root or space reaches them conflicts with it. A non-raw default broker is not compared with its own control subjects, since its channels are below `<prefix>.client.`. Give them distinct prefixes.
* The name `controller` is reserved for brokers: the controller's Redis broker reports its metrics under that name. Presence managers and map brokers may use it.
* Each entry owns its connections. The engine's Redis connections are not shared with entries, even when an address is the same.
* A routing hook is set for each kind (brokers, presence managers, map brokers) which has an enabled entry, so a reload can route a namespace to it. While the hook is set, every channel operation makes one channel options lookup, including the channels of namespaces which select no entry. Without an enabled entry of a kind, that kind has no hook and costs nothing.
* Routing is fixed at startup. A reload is refused, and the old configuration stays, when it changes the enabled entries (names, types, and the settings which each type keeps from startup: Redis `prefix` and `use_lists`, Nats `prefix` and `raw_mode`, Postgres `table_prefix`, Redis presence manager `prefix`, and the Redis or Postgres prefix of a map broker), the `broker_name`, `presence_manager_name` or `map.broker_name` of a namespace, or the controller (`enabled`, `type`, and its prefix: `redis.prefix`, `nats.prefix` or `postgres.table_prefix`). While any entry is enabled, it also refuses a change of `channel.namespace_boundary` or `channel.private_prefix`, since they decide which namespace a channel belongs to. Removing a namespace which selects an entry is refused, with the error `namespace engines: namespace "x" selecting broker_name "y" was removed, a restart is needed`. The selection of a namespace which a reload removes is kept for the rest of the process: its subscriptions stay on the engine they were made on, so a later reload cannot add that namespace back with another selection. A reload may add a new namespace which selects an existing entry. Connection details of entries and of the controller (addresses, URLs, timeouts) are accepted on reload but read at startup only, so a running connection keeps the values of startup until restart, as for the engine's connection details. Every other setting of an entry or of the controller, such as `nats.allow_wildcards` or the `presence_ttl` of a presence manager, is read at startup only as well: a reload accepts it and keeps the value of startup. The other errors read `namespace engines: ... changed, a restart is needed`.
* `memory` entries keep publications on one node, as the memory engine does. A `nats` entry is at-most-once, and history on it returns not-available, as for the standalone Nats broker.
* Named brokers, presence managers and map brokers are closed after `Node.Shutdown`, within their own `shutdown.timeout` budget, which starts only then and keeps the value of startup. The process-wide shutdown timer starts at the signal and exits the process when it fires first, so `shutdown.timeout` must cover `Node.Shutdown` and the named closes together. A controller is not closed at process shutdown; its connections end with the process.
* Each entry adds its connect time to startup, sequentially. A wrong address fails startup with the entry name in the error.

The `controller` section takes the same connection options as the engine:

* `redis`: `prefix` and the Redis connection options (`redis`). Control messages use the controller's own PUB/SUB, so channel traffic on the engine does not carry them. Metrics appear as the Redis broker PUB/SUB series labelled `name="controller"`.
* `nats`: `prefix` and the Nats connection options (`nats`) without `allow_wildcards` and `raw_mode`.

Resource costs, per entry, measured in connections and goroutines:

* Named Redis broker: one pooled client per shard, one PUB/SUB connection per subscribe shard, about NumCPU PUB/SUB processor goroutines and one reconnect loop. The same as a standalone Redis broker.
* Named Redis presence manager: one pooled client per shard and no goroutines.
* Named map broker: the connections of the default `map_broker` of its type, one set per entry. Memory has none.
* Redis controller: one pooled client per shard, one control PUB/SUB connection and goroutine per shard, and one probe.
* Nats broker or controller: one connection and its reader goroutine.
* Per channel operation, a routed channel costs one channel options lookup and one map lookup, with no allocations. Numbers are in the benchmark `BenchmarkNamespaceEnginesBroker` in `internal/app`.

### Not implemented (yet)

* Redis and PostgreSQL persistence for revocation.
* `user_connections` API.
* Redis replica offloading.
* Sharded PUB/SUB.

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
