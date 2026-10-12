# ImpulseM.Centrifugo

ImpulseM fork of [Centrifugo](https://github.com/centrifugal/centrifugo): upstream Centrifugo v6 plus features needed at messenger scale. The features are implemented from the public [Centrifugo PRO documentation](https://centrifugal.dev/docs/pro/overview), keeping its config keys and API shapes. Planned work is tracked in the [roadmap](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/17).

## Added on top of upstream

* [Server-side publication tags filter](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/7): a per-subscriber filter set by the backend (subscription token, connection token `subs`, subscribe and connect proxy) that the client can't widen.
* [User blocking and token revocation](https://github.com/Impulse-m/ImpulseM.Centrifugo/issues/10):
  * `block_user`, `unblock_user`, `revoke_token` and `invalidate_user_tokens` server API methods;
  * extensions for session-based revocation and revocation generations;
  * an in-memory store on every node, live disconnects and cluster propagation.

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
