# User blocking, token revocation and the revocation event source

These are the details of the user blocking and access revocation features of the fork. The summaries are in the [fork features](../README.md#fork-features) of the README.

## User blocking and token revocation

Tracking issue: [#10](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/10). The fork adds:

  * `block_user`, `unblock_user`, `revoke_token` and `invalidate_user_tokens` server API methods;
  * extensions for session-based revocation and revocation generations;
  * an in-memory store on every node, live disconnects and cluster propagation.
  * An entry is kept until the request's `expire_at`. Without it `access_revoke.default_ttl` (20 minutes) applies, so a revocation that must hold until the token expires sets `expire_at` to that expiration. A permanent block sets a far-future `expire_at`, which needs `access_revoke.max_ttl` off.
  * `access_revoke.max_ttl` (off by default, `0`; a positive value under one second is refused at startup) caps how long an entry may be kept, counted from the time it is applied. The API refuses an `expire_at` beyond now plus `max_ttl` with bad request, and the Kafka, notify and async consumer paths clamp it to that limit instead, so a consumer's revocation is applied, not skipped. A revocation is clamped, not dropped, and each clamp is counted in `centrifugo_access_revoke_expire_at_clamped_total{source}`. A permanent ban needs `max_ttl` off. A `default_ttl` beyond `max_ttl` is clamped to `max_ttl` for a request which sends no `expire_at`, and that clamp is counted under `source="api"`. Without it the store has no entry cap and keeps a far-future entry for its whole life, so watch `centrifugo_access_revoke_store_size`.
  * API calls reach the other nodes over the broker at most once: a node that misses the message (a dropped PUB/SUB message, a restart) does not apply it. The opt-in [revocation source](#access-revocation-event-source) is the durable delivery path.
  * A live disconnect which finds the disconnect queue full (`access_revoke.disconnect_queue_size`) is dropped and retried by user or session once the queue has room, so a blocked user or an invalidated token is still disconnected. Each connection is queued at most once, and a connection whose close has begun is never queued again. Each dropped user or session keeps one retry key, so the keys cost no more than the dropped disconnects, and a retry walks only the connections of its key. A retry runs only once the queue is at most half full. Under sustained overload, a dropped disconnect waits for the queue to fall to half, and meanwhile the connection is refused at its next refresh check. When the queue is empty, the kept keys are retried in one pass on the queue goroutine without pacing by `access_revoke.disconnect_rate`; that pass is bounded by the kept keys, which the dropped disconnects bound. A key whose retry drops a disconnect again is held until the queue is at or below half again, so a user with thousands of connections is re-scanned about once per half drain of the queue, not once per dequeue, and that scan stays visible in CPU. Size `access_revoke.disconnect_rate` and `access_revoke.disconnect_queue_size` against the connections of a node: a mass revocation drains at the rate, and `centrifugo_access_revoke_disconnect_queue_fill` shows the backlog.
  * Access revocation notifications of other nodes are applied on the Core control reader, without a queue, so a full disconnect queue never stalls it. Such an event costs time linear in the connections of the affected user or session on that reader, so a block of a user with many connections holds the reader for that scan. An unblock may rebuild the heap of the store when stale entries outnumber live ones by more than two to one. That rebuild takes time linear in the live entries, holds the store lock and stalls the control messages of the node meanwhile, so a node with many live blocks may see a pause on a burst of unblocks.
  * Differences from Centrifugo PRO:
    * `revoke_token` without `user` disconnects at once the connections with no user (anonymous and pending) which hold the session, through a per-node session index; connections with a user are disconnected at once only when `user` is passed, otherwise at their next refresh. A token without `exp` keeps such a connection until it reconnects. No revocation scans the connections of a node; only the one-shot catch-up pass after a fail-open start does.
    * `expire_at` in the past is rejected with bad request.
    * Extensions: `user` and `revoked_at` on `revoke_token`; `disconnect` on `revoke_token`, a code in 4000-4999 and a reason of at most 123 bytes that every close of the revocation uses instead of 3005; `generation` and `revoked_at` on `invalidate_user_tokens`; `token_revoke.uid_claim` and `user_tokens_invalidate.generation_claim` config; disconnect code 4500 for blocked users; loop guard disconnects with 3500.
  * Not implemented:
    * subscription tokens are not covered by revocation, only connection tokens;
    * `invalidate_user_tokens.channel` is not available;
    * `storage_type` is memory only: `redis` and `database` are rejected at startup. Use the per-node revocation source (`access_revoke.source`) as the durable delivery path.

## Access revocation event source

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
