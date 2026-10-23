# Rate limits

This file holds the details of `client.rate_limit` that the root README only names: the outage rules of
`redis_user_command`, its refusal memory, Redis sizing and the memory of the in-process limits.

## Options at a glance

Operation rate limits (`client.rate_limit`) as described in the PRO docs: per-connection `client_command` limits with `total`, `default`, channel namespace overrides and RPC method overrides, `user_command` limits per user ID (in memory, shared by all connections of a user, including user-level `connect`), and `client_error` limits that disconnect abusive connections. `redis_user_command` keeps per-user limits in Redis per user ID, so all nodes share one budget; it has no `total`. Users are spread over Redis shards by consistent hashing of the user ID. With `reuse_from_engine` it uses the engine Redis configuration and engine prefix, so apps sharing one Redis with different engine prefixes never share limits. On Redis errors commands are allowed (fail-open), or refused for one second with `client.rate_limit.redis_user_command.on_error: deny`; each Redis call is bounded by `io_timeout`, 4s by default, and `command_timeout` bounds the command and connect checks. With the default `on_error: allow` the node starts when Redis is unreachable: it allows commands and reconnects in the background. With `on_error: deny` it starts too, and refuses commands (one second each) and connects until Redis connects. The outage rules, the refusal memory, Redis sizing and the token bucket truncation are described in this file.

## redis_user_command

Each Redis call of a command or connect check is bounded by `io_timeout`, 4s by default. A stalled Redis costs each
command of its users up to that long on the connection reader, so for a shared Redis set
`client.rate_limit.redis_user_command.command_timeout` to 20ms to 50ms. The command and connect checks then give up
sooner, and a timed out check is handled as any Redis error is: allowed, or refused for one second with
`on_error: deny`.

On Redis errors commands are allowed (fail-open), or refused for one second with
`client.rate_limit.redis_user_command.on_error: deny`. Connects are then refused too, including while Redis is
unreachable at startup. With `on_error: allow` (the default) the node starts when Redis is unreachable: it allows commands and reconnects in the background. With `on_error: deny` it starts too, and refuses commands (one second each) and connects until Redis connects.

A refused command is remembered on the node until its wait ends, so a client which keeps sending during the wait costs
no Redis call. The memory is 100000 refusals per node at most. A refusal remembered this way still applies during a Redis
outage until its wait ends, even though fail-open allows commands otherwise. Only a refusal which Redis already decided is
remembered, so outside such a refusal an outage follows `on_error` as before. Every command checks this memory first,
one short mutex per command, so many connections of one user contend on one lock.

Every command of a user ID which is shared by many clients still reaches its one Redis shard key while it is allowed,
so do not share a user ID among many clients.

Each user takes one Redis key per scope it is limited in, with a TTL of the longest interval for token buckets and
twice it for sliding windows; a token bucket interval is not capped, so a user's keys stay for up to that interval
after its last allowed command. Size Redis by the users seen within those intervals, times the scopes they use, and by
the command rate, since every command reads Redis once.

Redis keeps token bucket times in whole microseconds, so the emission of a `redis_user_command` token bucket (its
interval divided by rate) is truncated to whole microseconds, and one under 1us is refused at startup. A truncated
emission admits up to the emission divided by its truncated value times the configured rate: an emission of 1.9us is
stored as 1us and admits about 1.9 times the rate. The smaller the emission in microseconds, the larger this error.

## Memory

A connection with client command limits holds 8 bytes per state word of each bucket of its scopes, the base, namespace
and method scopes and `total` alike: one word for `token_bucket`, two for `fixed_window` and three for
`sliding_window`.

A user with `user_command` limits holds the same per user while its state is kept: twice the longest interval of its
buckets, or once when all of them are token buckets, and at least 60s. The states are kept in memory on each node, one
per distinct user seen in that time, and Go maps do not shrink. The same formula gives the bytes of a user with
`user_command` limits, method overrides included.

The worst case of one connection is 4096 `method_overrides` entries of 16 buckets each: 4096 x 16 x 8 bytes = 512 KiB
with token buckets, and 4096 x 16 x 3 x 8 bytes = 1.5 MiB with sliding windows. Namespace overrides are not capped by
this, so each namespace override adds its own bytes on top. The validation caps (16 entries per list, 4096
`method_overrides`) bound one list, not the total: size connection and user memory by the worst case times the
connections and users seen.

The state of rate limits is kept per connection and per user in memory, and per user in Redis for `redis_user_command`.

## Where the PRO docs leave it open

* User-level `connect` falls back to `default` but is not subject to `total`, like `connect` in `client_command`. A connect takes its connect bucket token when it is attempted, so a connect which a later check rejects, such as single-connection enforcement, keeps the token used: the bucket is not refunded. Limiters are checked in order `client_command`, `user_command`: a command rejected by the user limit still consumes its per-connection tokens.

## Operation classification

* Map and shared poll subscriptions count as `subscribe`. A publish with map type counts as `map_publish`, or as `map_remove` when it removes the key, whatever the channel: Core routes it to its map handlers by type, not by the channel's subscription type, so a map type publish on a stream channel is still counted as a map operation. A regular publish to a map channel counts as `publish`. On a shared poll channel, track and untrack of keys count as `track` and `untrack` (`sub_refresh` with type 1 and 2). The classification is by request type alone, as Core routes it, so a request of type 1 or 2 without a track batch is charged to `track` or `untrack` before Core rejects it. On any other channel a `sub_refresh` refreshes the subscription token and counts as `sub_refresh` whatever its type. `history`, `presence` and `presence_stats` on map channels are plain commands and count as before.

## Upgrade notes

* Map publishes and map removes count against the `publish` buckets, and track and untrack against the `sub_refresh` buckets, unless their own buckets are set, as before the map operations existed. When `publish` has buckets this replaces the earlier fallback to `default`, so with both set, map publishes now share the `publish` budget. A map operation with its own base buckets counts against them in every namespace: neither the parent's base nor its namespace overrides apply to it. Without its own base buckets, the parent's namespace overrides still apply to the namespaces the map operation does not override. Set `map_publish`, `map_remove`, and for shared poll `track` and `untrack`, to give them a budget of their own.
* `redis_user_command` keys carry the user ID with `%` and `}` escaped. A user ID with either character gets new Redis keys, so its counters start empty once after the upgrade, and its budget refills at once. Other user IDs keep their keys.
* The node refuses to start when a bucket list has more than 16 entries, `method_overrides` has more than 4096 entries, or a `redis_user_command` token bucket has an emission under 1 microsecond (its interval divided by rate: with a one second interval, a rate above 1,000,000 per second). Reduce the entries, or lengthen the interval or lower the rate.
