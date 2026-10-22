# Rate limits

This file holds the details of `client.rate_limit` that the root README only names: the outage rules of
`redis_user_command`, its refusal memory, Redis sizing and the memory of the in-process limits.

## redis_user_command

Each Redis call of a command or connect check is bounded by `io_timeout`, 4s by default. A stalled Redis costs each
command of its users up to that long on the connection reader, so for a shared Redis set
`client.rate_limit.redis_user_command.command_timeout` to 20ms to 50ms. The command and connect checks then give up
sooner, and a timed out check is handled as any Redis error is: allowed, or refused for one second with
`on_error: deny`.

On Redis errors commands are allowed (fail-open), or refused for one second with
`client.rate_limit.redis_user_command.on_error: deny`. Connects are then refused too, including while Redis is
unreachable at startup. The node starts when Redis is unreachable: it allows commands and reconnects in the background.

A refused command is remembered on the node until its wait ends, so a client which keeps sending during the wait costs
no Redis call. The memory is 100000 refusals per node at most. A refusal remembered this way still applies during a Redis
outage until its wait ends, even though fail-open allows commands otherwise. Every command checks this memory first,
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
