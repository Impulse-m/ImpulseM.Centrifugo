# Client address behind trusted proxies

This is the detail of the `client_address` feature of the fork. The summary is in the [fork features](../README.md#fork-features) of the README.

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

Off by default, and then the auth throttle keeps its previous rule: the throttle key is the last `X-Forwarded-For` entry of a private or loopback peer and the peer itself otherwise, so the proxy in front must append to or overwrite `X-Forwarded-For` rather than pass a client value on. The request log is a known gap, not an intended rule: it takes `X-Real-IP`, then `X-Forwarded-For`, then the remote address, from any peer, so a client can choose the address it shows there. It is kept as it was until the trust rule of the request log is decided. When on, the resolved address is used by the auth throttle and the request log, and sent to RPC registry targets as `x-forwarded-for`, or omitted when it is unknown. Not yet exposed to the connect proxy or the connections API. PROXY protocol does not apply to HTTP/3 or WebTransport, which are UDP; the header walk still does. Internal HTTP and gRPC API ports do not read PROXY headers unless they share the external port.
