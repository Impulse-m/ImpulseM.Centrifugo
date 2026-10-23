# Fork options

These are the details of the smaller options of the fork. The summaries are in the [fork features](../README.md#fork-features) of the README.

## Unsubscribe codes

The `unsubscribe` server API method takes an optional `unsubscribe` code and reason, for the HTTP, gRPC, batch and consumer APIs: a code in 2100-2499 leaves clients unsubscribed, a code in 2600-2999 makes them subscribe again at once, so the subscribe proxy runs again and a changed server tags filter applies. The reason is at most 123 bytes. Without the field the default code 2000 applies; an invalid code or reason is a bad request, and in a consumer it is logged and the default code is used.

## Singleflight

Singleflight for `History`, `Presence` and `PresenceStats` (`singleflight.enabled`): concurrent identical calls share one broker or presence request. Helps only with a remote engine (Redis), not with the memory engine.

## Shared position sync

`shared_position_sync` channel option (namespace or `channel.without_namespace`): the node checks stream position once per channel per `client.channel_position_check_delay` instead of once per subscriber, so position checks cost one broker request per channel per interval.

  * Requires `force_positioning`, `allow_positioning`, `force_recovery` or `allow_recovery`, and a `client.channel_position_check_delay` greater than zero.
  * Detection latency: one to two `client.channel_position_check_delay` intervals for a node-wide loss, one interval for a single client.
  * `client.channel_position_check_delay` must exceed the broker's worst-case PUB/SUB lag, otherwise a publication in flight is reported as a loss.
  * A broker that lost the tail of a stream but kept its epoch (e.g. a Redis failover with async replication) is detected when the stream top falls behind what the node already delivered, confirmed over two intervals. A client ahead of the cached stream top is valid unless the top was fetched after its last update.
  * Options are applied when a channel's first subscriber arrives on the node. A config change applies to a channel after it empties.
  * Enabling it for the first time (no channel used it at startup) requires a node restart: when no channel options use it, the node skips the per-publication lookup entirely.

## Refresh lead

`client.refresh_lead`, for example `"60s"`: the connect and refresh replies tell a client with client-side refresh to refresh that much before its connection expires, and the reply carries the time left minus the lead, but never less than half of the time left. The connection still expires at its own time, and `client.expired_close_delay` applies after it. Whole seconds, zero means no lead. Read at start; a reload does not change it. A token which lives at least twice the lead gets the full lead. The lead should exceed the backend's clock skew plus the 99th percentile of the client's token fetch. SDKs report the ttl of the auth event as the time to the refresh. Off by default and then free. The change is part of the Core revision that `go.mod` pins.

## Message batching

`client.max_messages_in_frame`, as in the [PRO docs on message batching](https://centrifugal.dev/docs/pro/client_msg_batching): the maximum number of messages joined into one transport frame, default `16`, `-1` for no limit. `1` writes each message in its own frame, so `websocket.write_timeout` covers one reply. Only frame boundaries change: the same messages leave in the same order. The other batching keys of that page are not implemented.

## JWKS response size limit

JWKS responses are read up to 1 MiB per key set. A larger body fails that load with an error, so the node keeps the key set it has. This is a defect fix on by default (unbounded memory from a remote endpoint), and it has no key.
