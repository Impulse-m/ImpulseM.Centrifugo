# Centrifugo fork

This repository is a fork of [Centrifugo](https://github.com/centrifugal/centrifugo), the real-time messaging server. It is based on upstream Centrifugo **v6.10.2** and adds features for production deployments. Upstream is merged regularly, and a weekly workflow keeps an issue open while the fork is behind upstream. Some added features follow the public [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/overview) and keep its config keys; where the fork differs, its section says so. Planned work is tracked in the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

**Current version: v1.16.0.** The fork uses its own semantic versioning from 1.0.0, independent of upstream version numbers.

Install the server from the fork's packages:

```
docker pull ghcr.io/impulse-m/impulsem.centrifugo:1.16.0
```

Linux archives for `amd64` and `arm64` are attached to each [GitHub Release](https://github.com/Impulse-m/ImpulseM.Centrifugo/releases). See [Install](#install) for image tags and details.

## What the fork adds

* [Push notifications](#push-notifications): direct send to APNs and FCM tokens, and the push API routes over HTTP and gRPC.
* [Server-side publication tags filter](#server-side-publication-tags-filter), [server tags filter template](#server-tags-filter-template) and [require server tags filter](#require-server-tags-filter): subscription filters set by the backend that clients cannot widen.
* [User blocking and token revocation](#user-blocking-and-token-revocation), with an optional [access revocation event source](#access-revocation-event-source).
* [Client address behind trusted proxies](#client-address-behind-trusted-proxies).
* [RPC method registry](#rpc-method-registry).
* [Per-namespace engines and custom controller](#per-namespace-engines-and-custom-controller).
* [Operation rate limits](#operation-rate-limits) and [bandwidth optimizations](#bandwidth-optimizations).
* [Unsubscribe codes](#unsubscribe-codes), [singleflight](#singleflight), [shared position sync](#shared-position-sync), [refresh lead](#refresh-lead), [message batching](#message-batching) and the [JWKS response size limit](#jwks-response-size-limit).
* Not yet implemented: [see the list](#not-implemented-yet).

## Fork features

Each feature has a short summary here. The details are in the page the link points to.

### Server-side publication tags filter

A per-subscriber filter set by the backend, which the client cannot widen. Tracking issue [#7](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7). [Details](docs/server-tags-filter.md#server-side-publication-tags-filter).

### Require server tags filter

A channel option which refuses every subscription of a namespace that has no server tags filter, so a forgotten route fails closed. [Details](docs/server-tags-filter.md#require-server-tags-filter).

### Unsubscribe codes

The `unsubscribe` server API method takes a code which either leaves clients unsubscribed or makes them subscribe again at once. [Details](docs/fork-options.md#unsubscribe-codes).

### Singleflight

Concurrent identical `History`, `Presence` and `PresenceStats` calls share one request to a remote engine. [Details](docs/fork-options.md#singleflight).

### Shared position sync

The `shared_position_sync` channel option checks stream position once per channel per interval instead of once per subscriber. [Details](docs/fork-options.md#shared-position-sync).

### Operation rate limits

Per-connection, per-user and Redis-backed per-user limits on client operations, with disconnects for abusive connections, as in the PRO docs. [Details](internal/ratelimit/README.md).

### Bandwidth optimizations

`keep_latest_publication`, `allow_channel_compaction` and `client_publish_debounce_interval`, following the PRO docs. [Details](docs/bandwidth-optimizations.md).

### Refresh lead

`client.refresh_lead` makes clients with client-side refresh renew their token before it expires. [Details](docs/fork-options.md#refresh-lead).

### Message batching

`client.max_messages_in_frame` sets how many messages share one transport frame, as in the PRO docs. [Details](docs/fork-options.md#message-batching).

### JWKS response size limit

JWKS responses are read up to 1 MiB per key set, so a larger body cannot exhaust memory. [Details](docs/fork-options.md#jwks-response-size-limit).

### Push notifications

Push notification API methods (device, topic, `SendPushNotification`, `UpdatePushStatus`, `CancelPush`) are routed over HTTP and gRPC. While push notifications are off they answer error 108 "push notifications are not enabled"; before, gRPC answered `Unimplemented` and HTTP had no route. The push messages gain the direct send fields `targets`, `collapse_id`, `timeout_ms` and `results` (field numbers 100 and above).

* Direct push send (`SendPushNotification` with raw APNs and FCM tokens, the `timeout_ms` deadline, the per-target results and the routing to sender nodes) is documented in [docs/push-direct-send.md](docs/push-direct-send.md).
* Push notification settings are read at startup. A change of a key, of `sender` or of `enabled_providers` needs a restart. A SIGHUP reload validates the push section like every other section, and a failing push section rejects the reload. A valid changed push section is ignored until the restart.
* Generated configs mask key material. `push_notifications.apns.token_key_pem` is shown as `[REDACTED]` in JSON, YAML, TOML and formatted output, and an unset key shows no mask. A key must be given by file (`token_key_file`) or by environment variable; a generated config never carries its value.

### Access revocation event source

An optional replayable Kafka log from which each node receives access revocation events, and from which it rebuilds its store after a restart. Without `access_revoke.source.type` nothing changes. [Details](docs/user-blocking-and-revocation.md#access-revocation-event-source).

### User blocking and token revocation

Server API methods `block_user`, `unblock_user`, `revoke_token` and `invalidate_user_tokens`, with an in-memory store on every node, live disconnects and cluster propagation. Tracking issue [#10](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/10). [Details](docs/user-blocking-and-revocation.md#user-blocking-and-token-revocation).

### RPC method registry

Clients call existing gRPC methods of backend services through RPC, by method name, and the request and response bytes pass through unchanged. It is off unless `rpc.registry.enabled` is set. The config, error mapping, metrics and planned work are in [internal/rpcregistry/README.md](internal/rpcregistry/README.md).

### Client address behind trusted proxies

`client_address` resolves the real client address behind a load balancer or a reverse proxy, from a forwarded header or a PROXY protocol header, and believes them only from the proxies listed in `trusted_proxies`. Off by default. [Details](docs/client-address.md).

### Server tags filter template

A namespace can render a server tags filter for every subscription of a connection from the connection's claims, with `on_missing_value` choosing whether a missing claim refuses or skips the template. Off by default. [Details](docs/server-tags-filter.md#server-tags-filter-template).

### Per-namespace engines and custom controller

A namespace can use its own broker and presence manager, and control traffic can move to a dedicated controller, so heavy namespaces do not share a broker with latency-sensitive channels. Off by default. [Details](docs/per-namespace-engines.md).

### Not implemented (yet)

* Redis and PostgreSQL persistence for revocation.
* `user_connections` API.
* Sharded PUB/SUB.
* Redis replica offloading.

Some of these are on the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Install

Docker images are published to GitHub Container Registry for `linux/amd64` and `linux/arm64`:

```
docker pull ghcr.io/impulse-m/impulsem.centrifugo:1.16.0
docker run -p 8000:8000 -v "$(pwd)/config.json:/centrifugo/config.json" ghcr.io/impulse-m/impulsem.centrifugo:1.16.0
```

Image tags:
* `X.Y.Z`, `X.Y` and `latest` for releases;
* `master` and `sha-<commit>` for builds from the `master` branch.

The image keeps the notices under `/usr/share/doc/centrifugo/`: `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` and the `licenses/` directory.

Each [GitHub Release](https://github.com/Impulse-m/ImpulseM.Centrifugo/releases) attaches Linux archives `centrifugo_<version>_linux_<arch>.tar.gz` for `amd64` and `arm64`, and a SHA-256 checksums file. The archives contain the `centrifugo` binary, the `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES.md` files, the README and the changelog, and a `licenses/` directory with the license reports of the embedded web UI (`licenses/webui/THIRD_PARTY_LICENSES.txt`) and Swagger UI (`licenses/swagger-ui/THIRD_PARTY_LICENSES.txt`) dependencies.

## Versioning and releases

The fork uses its own semantic versioning starting at 1.0.0, independent of upstream version numbers; released versions are listed in the [tags](https://github.com/Impulse-m/ImpulseM.Centrifugo/tags). A release is made in one of two ways. Pushing a `vX.Y.Z` tag publishes the GitHub Release for that tag. Running the `release` workflow manually with the version (without `v`) creates the tag and then publishes the GitHub Release for it. Both ways run the tests and publish the images first.

## Client SDKs and related repositories

Each SDK is a fork of its upstream SDK and carries the fork's protocol fields: the JavaScript SDK of [centrifuge-js](https://github.com/centrifugal/centrifuge-js), the Java SDK of [centrifuge-java](https://github.com/centrifugal/centrifuge-java) and the Swift SDK of [centrifuge-swift](https://github.com/centrifugal/centrifuge-swift). Install them from the packages below. The Core and Protocol repositories that the server is built from are listed after the SDKs.

### JavaScript

Repository: [GitHub](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS). Latest version: 1.6.0.

```
npm install @impulsem/centrifuge
```

### Java (Android)

Repository: [GitHub](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Java). Latest version: 1.7.0.

```groovy
repositories {
    mavenCentral()
    maven { url "https://impulse-m.github.io/ImpulseM.Centrifugo.Client.Java/" }
}

dependencies {
    implementation "com.github.Impulse-m:ImpulseM.Centrifugo.Client.Java:1.7.0"
}
```

### Swift (iOS)

Repository: [GitHub](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Swift). Install with Swift Package Manager from the version tags; the latest is 2.0.0 and its breaking changes are described in [MIGRATION.md](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Swift/blob/v2.0.0/MIGRATION.md). Add it with `https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.Swift` and add the `SwiftCentrifuge` product to your target. There is no CocoaPods release.

### Core (Go)

Repository: [GitHub](https://github.com/Impulse-m/ImpulseM.Centrifugo.Core). Fork of the [centrifuge](https://github.com/centrifugal/centrifuge) Go library, which the server uses through a `replace` directive in its `go.mod`. It has no releases of its own and is kept in sync with upstream master.

### Protocol

Repository: [GitHub](https://github.com/Impulse-m/ImpulseM.Centrifugo.Protocol). Fork of the [protocol](https://github.com/centrifugal/protocol) definitions. It carries the fork fields of `client.proto` used by the SDKs and Core.

The upstream projects of the SDKs, Core and Protocol are MIT-licensed; see each repository for its own license.

## Upstream Centrifugo

The text below is the upstream description, kept as upstream wrote it. This fork changes some behaviour the upstream documentation describes; the differences are listed in [Fork features](#fork-features).

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

## License and credits

Centrifugo is created by [Centrifugal Labs](https://github.com/centrifugal) and licensed under the Apache License 2.0. This fork is a modified version of it and keeps the same license; see [LICENSE](LICENSE). [NOTICE](NOTICE) states that this is a modified version and where its changes are recorded. Code and assets copied into this repository from other projects, with their licenses, are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
