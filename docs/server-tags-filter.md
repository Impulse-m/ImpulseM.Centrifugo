# Server tags filters

These are the details of the server tags filter features of the fork. The summaries are in the [fork features](../README.md#fork-features) of the README.

## Server-side publication tags filter

Tracking issue: [#7](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7). A per-subscriber filter set by the backend that the client can't widen. The filtering itself is upstream's Core implementation; the fork lets the backend set it from the subscription token, connection token `subs`, and subscribe and connect proxy results. Recovery applies the filter; an explicit history call on a filtered subscription is refused, since history is not filtered.

The namespace-level template that renders a filter per connection from its claims is described in [Server tags filter template](#server-tags-filter-template) (tracking issue [#18](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/18)).

## Server-only publication tags

A publish or broadcast request can send `server_tags` beside `tags`. `tags` reach clients as before. `server_tags` are matched by server tags filters only, and never reach a client: not in a publication frame, a subscribe reply, a history or recovery reply, or the server API history, which returns `tags` only. The field is an ordinary map on the HTTP, gRPC and batch API, and a broadcast applies the same map to every channel of the request.

```json
{"channel": "chat:room1", "data": {"text": "hi"}, "tags": {"kind": "msg"}, "server_tags": {"tenant": "t-17", "member": "u-42"}}
```

Off by default. A publish which sends no `server_tags` produces the same bytes as before the option.

* Enable it per namespace (or `channel.without_namespace`) with `allow_server_tags: true`. A publish or broadcast which sends `server_tags` to a namespace without it is refused with 108 (not available). Only stream channels carry server tags: the option is refused at config load on any other `subscription_type`, so map publish and shared poll carry none. Client publish and proxy publish cannot set them.
* A key of `server_tags` must not be empty, must not start with `__` (reserved for Centrifugo), and must not be a key of `tags`. Otherwise the request is refused with 107 (bad request). In a broadcast, such a key refuses the whole call, since the keys are the same for every channel.
* `skip_history` is allowed. The publication is live-filtered as usual, and it is never stored, so it is never recovered.
* Caps: `publish_tags_max_count` counts the entries of `tags` and `server_tags` together, and `publish_tags_max_bytes` sums the lengths of their keys and values. A publication over a cap is refused with 1000 (`publish tags limit exceeded`), HTTP 400 and gRPC `InvalidArgument`. The caps count `tags` too, so they apply to a publication without `server_tags`. Zero, the default, means no cap. The recommended values for a namespace which enables the option are 256 entries and 16384 bytes.
* A refused publish stores nothing. In a broadcast, a refusal by the gate or by a cap applies to the response of that channel only; the other channels are published.
* Error texts are fixed strings. Neither the responses nor the logs or metric labels name a key or a value: they carry counts and channel names.
* The option is refused at config load with any `subscription_type` other than `stream`, and with `subscribe_stream_proxy_enabled`, whose subscriptions do not apply a server tags filter, and with any broker other than memory or redis, the default broker or the named broker of the namespace. Negative caps are refused too.

Storage, with the memory and redis brokers:

* The memory broker holds `server_tags` with the publication, for the history window.
* Redis stores `server_tags` in clear in the history entries (`history_size` x `history_ttl`) and sends them over pub/sub, once per node. Anyone who can read Redis can read them. This is an exposure of the storage, not a leak to clients. The cost is the bytes of the server tags per publication and per node, plus the history retention. The same keys in `tags` would reach every subscriber of the channel.

Rolling upgrade and downgrade:

* Upgrade every node first (server, Core and Protocol), then set `allow_server_tags` on a namespace, with the same config on all nodes. A node which does not know the field ignores it: its server filter sees no server tags, so an `eq` on a server key drops the publication there and a `nex` or `ne` on it lets the publication through. A publish which lands on such a node loses its `server_tags` silently. Centrifugo cannot detect old peers, so the order is the operator's job.
* Downgrade: set `allow_server_tags: false` on all nodes, wait for the history window to expire, then downgrade the nodes.

Consumer messages of method `publish` or `broadcast` decode into the same requests and go through the same checks, so their `server_tags` follow the gate, key and cap rules above.

## Require server tags filter

`require_server_tags_filter` channel option (namespace or `channel.without_namespace`): every subscriber of the namespace must carry a server tags filter, so a subscription which ends up without one is refused whichever route made it, and client history calls on the namespace are refused. A forgotten route fails closed instead of giving an unfiltered subscription.
  * Client subscribe without a filter: permission denied (103). Covers a subscription token without the claim, a subscribe proxy answer without `server_tags_filter` (for example one whose key was spelled `serverTagsFilter`, which the HTTP decoder drops silently), user-limited and `allow_subscribe_for_client` channels, and map subscriptions.
  * Connect-time server-side subscriptions the backend asserts, a connection token `channels` claim or a `subs` entry without the claim, a connect proxy `channels` list or a `null` `subs` entry: the connection is refused with 3507, which SDKs do not retry. This is the cost of failing closed on the connect path: one such channel locks the user out of the whole connection, not only that channel, so roll the option out per namespace after the token and connect proxy code carries filters. A channel a client asks for at connect is dropped and the connection is made.
  * Rebind: a token whose subscriptions lack a filter is refused as a refresh error (103) before the connection moves.
  * Server API `subscribe`, which cannot carry a filter: not available (108).
  * Client `history()` on the namespace: permission denied (103). Subscribe-time recovery is still allowed and filtered; the server API `history` is unaffected.
  * With `server_tags_filter_template`: a filter the template produces counts, so `on_missing_value: skip` with this option refuses a subscription whose template was skipped and which has no explicit filter.
  * Config errors: `subscription_type: shared_poll` and `subscribe_stream_proxy_enabled` (those subscriptions do not apply the filter), the personal channel namespace (without a template), and `client.insecure_skip_token_signature_verify` (an unverified token can carry any filter).
  * Turning it on at a config reload applies to new subscriptions; existing unfiltered subscriptions live until they resubscribe or reconnect.
  * Off by default and then free. Not in the PRO docs: they describe no option that requires a filter.

## Server tags filter template

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
