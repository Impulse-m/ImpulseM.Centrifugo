# RPC method registry

The registry lets clients call existing gRPC methods of backend services through Centrifugo's RPC. A client calls a registry method name, and Centrifugo calls the gRPC method it maps to by full name, for example `/example.accounts.v1.AccountService/GetProfile`. There is no wrapper service: request and response bytes pass through unchanged, and the backend keeps its own protobuf contract. Without `rpc.registry.enabled` nothing changes.

```json
"rpc": {
  "registry": {
    "enabled": true,
    "file": "/etc/centrifugo/rpc_registry.yaml",
    "drain_timeout": "30s",
    "errors": {
      "unknown_method": {"code": 104, "message": "method not found", "app_code": 400, "app_type": "UNKNOWN_METHOD"},
      "auth_class": {"code": 103, "message": "permission denied", "app_code": 401, "app_type": "ACCESS_DENIED"},
      "request_too_large": {"code": 106, "message": "limit exceeded", "app_code": 400, "app_type": "REQUEST_TOO_LARGE"},
      "cancelled": {"code": 107, "message": "call cancelled", "app_code": 400, "app_type": "CALL_CANCELLED"}
    },
    "app_error": {"code_key": "app-code", "type_key": "app-type"},
    "grpc_status_to_code_transforms": []
  }
}
```

* `file`: path to the registry file, JSON or YAML, chosen by extension. Entries are read from this file only: combining `file` with inline `defaults`, `targets` or `methods` is an error.
* `poll_interval`: accepted, but has no effect yet. The file is read at start and again on a configuration reload (SIGHUP). A changed file is applied as a whole: if it fails validation, the previous registry stays active. See [Planned](#planned).
* `drain_timeout`: how long connections of removed or replaced targets stay open before they are closed, so calls in flight can finish. Default `30s`.
* `errors`: protocol errors returned for failures of the registry itself: unknown method, auth class mismatch, request too large, and a call cancelled by the client. Each has `code`, `message`, and optional `app_code` and `app_type`.
* `app_error`: names of the gRPC trailers which carry the application error code and type. Defaults are `app-code` and `app-type`.
* `grpc_status_to_code_transforms`: overrides for the default mapping of gRPC status codes to protocol errors, see [error mapping](#error-mapping).

Registry file:

```yaml
version: 1                       # format version, must be 1
defaults:
  auth_class: authenticated      # used by methods which do not set auth_class
  timeout: 5s
targets:
  - name: accounts
    endpoint: dns:///accounts.example.internal:9000
    load_balancing: round_robin  # round_robin (default) or pick_first
    emulated_headers: [x-example-init]   # connect-time headers forwarded to this target
  - name: catalog
    endpoint: dns:///catalog.example.internal:9000
    timeout: 10s                 # target default, overrides defaults.timeout
    max_request_bytes: 1048576
methods:
  - method: account.getProfile                 # client RPC method name, unique
    grpc_method: /example.accounts.v1.AccountService/GetProfile
    target: accounts
    idempotent: true
  - method: account.signIn
    grpc_method: /example.accounts.v1.AccountService/SignIn
    target: accounts
    auth_class: anonymous
    error_metadata: [trace-ref]  # backend trailers copied into the protocol error metadata
  - method: account.confirmCode
    grpc_method: /example.accounts.v1.AccountService/ConfirmCode
    target: accounts
    auth_class: pending_2fa
  - method: catalog.search
    grpc_method: /example.catalog.v1.CatalogService/Search
    target: catalog
    auth_class: authenticated
    idempotent: true
    headers: [x-example-region]  # per-call client headers forwarded to the target
```

The same tree works as JSON. Each method and target field left unset inherits from the target, then from `defaults`, then from the built-in values: `timeout` 5s, `max_request_bytes` 64 KiB, `max_response_bytes` 1 MiB, `idempotent` false. Method names match `^[A-Za-z0-9_.:/-]{1,128}$`, gRPC method paths match `^/[\w.]+/\w+$`. Header allow-lists are lower-case. `authorization`, `traceparent`, `tracestate`, `x-forwarded-for` and names with the `x-centrifugo-` prefix are reserved and rejected in both allow-lists.

Auth classes say which connections can call a method:

* `anonymous`: any connection. The connection token is never forwarded, even when the connection is authenticated.
* `pending_2fa`: planned, see [Planned](#planned). For now it is checked like `authenticated`.
* `authenticated`: a connection with a user ID, which forwards its connection token.
* `bot`: planned, see [Planned](#planned). For now it is checked like `authenticated`.

A method without `auth_class` takes `defaults.auth_class`. If that is empty or `anonymous`, every method must state its class, so a missing class never makes a method public. A connection which does not satisfy the class gets `errors.auth_class`.

Metadata sent with each call. Later sources win:

1. static metadata of the target (`grpc.static_metadata`);
2. connect-time headers listed in the target's `emulated_headers`;
3. per-call headers listed in the method's `headers`;
4. values set by Centrifugo: `authorization: Bearer <token>` for non-anonymous methods when the connection has a token, `x-centrifugo-client` with the client ID, and `x-centrifugo-msg-id` for calls with a message ID;
5. `x-forwarded-for` is planned, see [Planned](#planned). Until then no `x-forwarded-for` is sent. Clients cannot set it: the name is reserved;
6. `traceparent` and `tracestate` from the trace context of the call.

## Error mapping

A failed gRPC call becomes a protocol error with the default mapping below. The `code` is the Centrifugo protocol error code.

| gRPC status | `code` | `temporary` |
|---|---|---|
| `UNAUTHENTICATED` | 101 | no |
| `PERMISSION_DENIED` | 103 | no |
| `RESOURCE_EXHAUSTED` | 111 | yes |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | 100 | yes |
| `INTERNAL`, `UNKNOWN`, `UNIMPLEMENTED`, `DATA_LOSS` | 100 | no |
| `CANCELLED` (client cancelled the call) | `errors.cancelled` | no |
| `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `NOT_FOUND`, `ALREADY_EXISTS`, `ABORTED`, `OUT_OF_RANGE` | 400 | no |

* A `CANCELLED` status which the backend sends on a live connection is not a client cancel. It maps like `INTERNAL`, to code 100.
* `grpc_status_to_code_transforms` replaces the row of a status. Each rule has `status` (a gRPC status name, exact match) and `to_error` or `to_disconnect`, the same as `status_to_code_transforms` for HTTP proxies:

  ```json
  "grpc_status_to_code_transforms": [
    {"status": "UNAUTHENTICATED", "to_error": {"code": 150, "message": "sign in again"}}
  ]
  ```
* With the default mapping, the protocol error `message` is the gRPC status message, cut to 256 bytes.
* The application error code comes from the `app_error.code_key` trailer as a decimal `int32`. Without it, the code is 401 for `UNAUTHENTICATED`, 403 for `PERMISSION_DENIED`, 420 for `RESOURCE_EXHAUSTED`, 400 for `INVALID_ARGUMENT`, and 500 otherwise.
* The application error type comes from the `app_error.type_key` trailer and must match `^[A-Z0-9_]{1,64}$`. Without a valid trailer, the status message is used when it matches that pattern.
* Protocol error `metadata` holds only the trailers listed in the method's `error_metadata`, nothing else.

Check the registry in CI with the usual config check. It validates the registry file too, when `rpc.registry.enabled` is true:

```bash
centrifugo checkconfig -c config.json
```

## Metrics

* `centrifugo_rpc_registry_calls_total{rpc_method, result}`: calls by method and result (`ok`, `error`, `cancelled`, `timeout`, `denied`, `unknown`). A call to a name which is not in the registry counts as `unknown`.
* `centrifugo_rpc_registry_errors_total{rpc_method, app_type}`: failed calls with an application error type. Cancelled calls are not counted here.
* Per target, the proxy metrics with protocol `grpc` and type `rpc`: `centrifugo_proxy_duration_seconds`, `centrifugo_proxy_duration_seconds_histogram`, `centrifugo_proxy_errors` and `centrifugo_proxy_inflight_requests`. A call cancelled by the client is not observed in them, as the proxy handler does for a closed client; `centrifugo_proxy_inflight_requests` still counts it while it runs.
* The series are created when the registry is built. Each registry method exports one zero-valued `calls_total` series per result, so N methods export 6·N series. Each target exports its four proxy series, the same way. The `unknown` method label is the exception: its series are created on the first call to an unknown name.

## Planned

Not implemented yet:

* `x-forwarded-for` from the PROXY protocol address of the client connection, with one trusted-proxy policy shared with the listener.
* File polling: `poll_interval` is accepted and reserved for it. It has no effect yet.
* `pending_2fa` and `bot` class claims. Both are checked like `authenticated` for now.
* The `x-centrifugo-retransmit` and `x-centrifugo-without-updates` headers.
* Cancelling an in-flight call by its `msg_id`.
* Deduplication of calls with a message ID. `idempotent` is accepted and reserved for it. It has no effect yet.
