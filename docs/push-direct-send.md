# Direct push send

* Direct push send signs the APNs provider token once per 40-minute window per signing key. The token's `iat` is the start of the current window, and a node moves to a new window 60 seconds after each boundary. Nodes that share one key therefore present the same `iat`, so Apple sees at most one token update per key per window. Apple refuses token updates more often than every 20 minutes, and the node clocks must be NTP-synced for the windows to agree. The signature is deterministic (RFC 6979), so nodes that share a key and a window produce the same token bytes. A failed FCM token fetch is not held: it fails only the targets that waited on that fetch, and the next call starts a new fetch. Concurrent fetches of one credential are bounded to one at a time, and each fetch ends within `providers.request_timeout`. A caller stops waiting for a token at its call deadline, but a fetch that is already running can end up to `providers.request_timeout` after that deadline. A target whose token fetch fails is answered `not_sent` with code `token_fetch`, and a target whose deadline ends during the token wait is `not_sent` with code `deadline`. A token fetch that fails because the node shuts down is `not_sent` with code `shutdown`. None of them makes a provider call, so a resend cannot duplicate them.
* A direct send target that never left the node is answered `not_sent`, which replaces `pending` in direct send, with `http_status` 0 and a `code` that names why: `no_slot` (no in-flight slot before the slot wait ended), `connect_failed` (a transport error before the request was written), `deadline` (the call deadline, or the provider request timeout, came before the write), `token_fetch` (the FCM token fetch failed), `shutdown` (the node shut down before the write) or `address_blocked` (the Web Push address guard refused the connection). When the node's shutdown and a deadline have both ended, the code is `shutdown`, since the two ends are seen only after the fact. A target is `unanswered` only when its request was written and no status line came back, so a resend may then duplicate it. "Written" is the `WroteRequest` hook of `net/http/httptrace` reporting no error for the attempt. A received status line always keeps `http_status`.
* `collapse_id` of the request is the default of every target, and a target's own `collapse_id` overrides it. The resolved value sets APNs `apns-collapse-id`, and for FCM `android.collapse_key` and `apns.headers.apns-collapse-id`, where the caller set none. When the resolved value is set, a caller `apns-collapse-id`, `android.collapse_key` or `apns.headers.apns-collapse-id` must equal it, and a different value is refused with 107. When the resolved value is empty, the caller values pass unchanged. FCM collapses only messages queued for an offline Android device, so an online Android device can show both messages: dedup by `ref` on the caller side.
* A failed FCM answer carries its raw `error.details` array in `provider_details` (field 9 of `PushItemResult`), unmapped, so a caller reads any FCM detail the server does not name. `provider_detail` is unchanged. An array over 4 KiB is returned as a valid JSON array of its whole elements, with the whitespace removed and cut at an element boundary when it still does not fit, and `provider_details_truncated` (field 10) is set. The bound is per target, not per call: a call with many failing FCM targets can return up to 4 KiB of details for each of them. APNs answers and successes carry no `provider_details`.
* Direct push send (`SendPushNotification` with `recipient.targets`, raw APNs, FCM and Web Push tokens) is off by default. It needs `push_notifications.enabled` and `push_notifications.sender`, and it runs on sender nodes only; other nodes answer error 108. A send is synchronous: the response carries one raw result per recipient, in request order, and it takes at most `sync_timeout` plus `providers.request_timeout` plus 5 seconds. There is no database, no queue and no retry, so the caller owns the retries. A request body over `max_request_bytes` is refused with error 107. While push notifications are off, no push metric family is exported.
* `timeout_ms` (field 101 of `SendPushNotificationRequest`) sets the deadline of the whole direct send call, in milliseconds. Zero keeps the default deadline, `sync_timeout` plus `providers.request_timeout` plus 5 seconds. A longer value is clamped to that default, not refused. The slot wait ends at the earlier of `sync_timeout` and the call deadline, and each provider call ends at the earlier of `providers.request_timeout` and the call deadline. The clock starts when the request arrives, before its body is read, so the upload of the body is inside the deadline: a body that takes longer than `timeout_ms` to arrive gets no answer, and no provider call is made. The response is written within the call deadline.
* A sender node that serves push over the gRPC API sets `grpc_api.max_receive_message_size` to the push `max_request_bytes`, because gRPC decodes a message before the push checks run.
* An APNs target names its `environment`, `production` or `development`, which picks the APNs host. It is required for every APNs target, and an FCM target must not set it: both cases are refused with 107. The PRO-style `apns_tokens` list has no per-target environment, so its tokens use `push_notifications.apns.endpoint`, the default environment of the node.
* The direct send API is the `CentrifugoApi` service of the proto (`centrifugal.centrifugo.api`), rpc `SendPushNotification`, and the HTTP route `/send_push_notification` under the API prefix. Its fork fields sit in the fork extension blocks of the messages: `PushRecipient.targets` (100), `SendPushNotificationRequest.collapse_id` (100) and `timeout_ms` (101), `SendPushNotificationResult.results` (100), `Error.metadata` (100). The new messages are `PushTarget` (provider, token, environment, ref, collapse_id, apns_payload, fcm_message) and `PushItemResult` (index, ref, status, code, provider_code, provider_detail, http_status, provider_message_id, and the fields 9 and 10 `provider_details` and `provider_details_truncated`). Each release attaches the proto as the `api.proto` asset of the GitHub release, so a caller can generate a client from it.
* Dry run (`push_notifications.dry_run`) runs every check of the live send before a provider call: the recipient count and the body cap, the provider and the environment of each target, the header allow-list and the collapse id, the APNs payload ceilings and the FCM message ceiling after the server additions, and `expire_at`. A refused request is refused with the same error in both modes, and a target gets the same status and code in both, except that a valid target answers `dry_run` where a live send answers `sent`. The one exception is the Web Push result `address_blocked`, which is known only at connect time, so a dry run cannot give it. A dry run loads no credential and makes no provider call.
* A direct send call over `max_concurrent_requests` is answered error 111 `too many requests`, which is temporary. Its `metadata` holds `x-centrifugo-retry-after` set to `"1"`, the same key that a rate limited registry error carries: the caller waits one second and retries, since there is no queue. The refusal comes before the body is read. In the transport error mode it is HTTP 429. The 111 is per call, not per target.
* Route refusals of `SendPushNotification` (errors 108 and 111, the recipient count and 107) follow `http_api.error_mode` like every API method: by default HTTP 200 with the error in the response body, and with `transport` the mapped HTTP status.

## Routing push calls to sender nodes

Direct send (`SendPushNotification` with `recipient.targets`, or the shorthand `fcm_tokens` and `apns_tokens`) runs only on nodes with `push_notifications.sender` set. Any other node answers error 108 `direct send is available on sender nodes only`. A node with push notifications off answers 108 `push notifications are not enabled`. gRPC returns the same `Error` for both, so every protocol gives the same answer. Nodes do not forward calls to each other: the deployment routes them.

Run the sender nodes as their own group, with their own Deployment or instance group, and their own Service, DNS name or load balancer pool. Point the push API calls of the backend at that address, and keep the other nodes for everything else. The sender group needs the provider credentials (`push_notifications.apns.token_key_file` or `token_key_pem`, `push_notifications.fcm.credentials_file`, and `push_notifications.webpush.vapid_private_key_file` or `vapid_private_key`) and the push settings, all read at startup.

A Kubernetes example, with the credential in a Secret mounted as a file:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: centrifugo-push-senders
spec:
  replicas: 2
  selector:
    matchLabels:
      app: centrifugo-push-senders
  template:
    metadata:
      labels:
        app: centrifugo-push-senders
    spec:
      containers:
        - name: centrifugo
          image: ghcr.io/<owner>/<image>:<tag>
          env:
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_ENABLED
              value: "true"
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_SENDER
              value: "true"
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_ENABLED_PROVIDERS
              value: "apns"
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_TOKEN_KEY_FILE
              value: /secrets/apns/AuthKey.p8
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_TOKEN_KEY_ID
              value: ABC123DEFG
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_TOKEN_TEAM_ID
              value: TEAM123456
            - name: CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_BUNDLE_ID
              value: com.example.app
          ports:
            - containerPort: 8000
          volumeMounts:
            - name: apns-key
              mountPath: /secrets/apns
              readOnly: true
      volumes:
        - name: apns-key
          secret:
            secretName: apns-auth-key
---
apiVersion: v1
kind: Service
metadata:
  name: centrifugo-push-senders
spec:
  selector:
    app: centrifugo-push-senders
  ports:
    - name: http
      port: 8000
      targetPort: 8000
```

The backend then posts to the `send_push_notification` route of the HTTP API of that Service, under the API prefix of the node, with the API key, and its HTTP client timeout is at least 30 seconds. A call's answer takes at most `timeout_ms`, counted from the arrival of the request, or the default deadline of `sync_timeout` plus `providers.request_timeout` plus 5 seconds when `timeout_ms` is zero. A call that reaches a node outside the sender group gets error 108 at once, and no provider call is made. The sender group is not a separate listener: a request is served by any sender pod behind the Service.

## Web Push

Web Push sends to a browser subscription (`recipient.webpush_tokens`, or `recipient.targets` with `provider: "webpush"`). The server signs the request with VAPID (RFC 8292) and encrypts the payload (RFC 8291, `aes128gcm`). It is off by default: add `webpush` to `push_notifications.enabled_providers`.

### VAPID key pair

Make a P-256 key pair with openssl and print its two base64url values. The public value is the 65-byte uncompressed point, the private value is the 32-byte scalar:

```sh
openssl ecparam -name prime256v1 -genkey -noout | openssl ec -outform DER 2>/dev/null > vapid.der
echo "public: $(tail -c 65 vapid.der | base64 | tr -d '\n=' | tr '+/' '-_')"
echo "private: $(head -c 39 vapid.der | tail -c 32 | base64 | tr -d '\n=' | tr '+/' '-_')"
rm vapid.der
```

* Any Web Push key generator gives the same base64url pair.
* The private value is a secret. Keep it out of the repository.

### Configuration

A private key mounted as a file:

```yaml
push_notifications:
  enabled: true
  sender: true
  enabled_providers:
    - webpush
  webpush:
    vapid_public_key: <public key, base64url>
    vapid_private_key_file: /secrets/webpush/vapid_private_key
    subject: mailto:push@example.com
```

The same key inline:

```yaml
push_notifications:
  enabled: true
  sender: true
  enabled_providers:
    - webpush
  webpush:
    vapid_public_key: <public key, base64url>
    vapid_private_key: <private key, base64url>
    subject: mailto:push@example.com
```

* When `webpush` is enabled, give exactly one of `vapid_private_key` and `vapid_private_key_file`. Environment variables follow the same names, for example `CENTRIFUGO_PUSH_NOTIFICATIONS_WEBPUSH_VAPID_PRIVATE_KEY_FILE`.
* `subject` is a `mailto:` or `https:` URL. It is the `sub` claim of the VAPID token.
* The key file is read at the start of a sender node, as the APNs key file is.
* `vapid_private_key` is masked in generated configs.

### Frontend and service worker

The frontend subscribes with the public key and sends the whole subscription to the backend:

```js
const registration = await navigator.serviceWorker.ready;
const subscription = await registration.pushManager.subscribe({
  userVisibleOnly: true,
  applicationServerKey: "<public key, base64url>",
});
await fetch("/api/push-subscriptions", {
  method: "POST",
  body: JSON.stringify(subscription),
});
```

* The backend stores the JSON of the subscription as it is. It is the token: `{"endpoint": "...", "keys": {"p256dh": "...", "auth": "..."}}`.
* The service worker receives the request payload as JSON in the `push` event. It must show a notification, because the subscription is `userVisibleOnly`.

```js
self.addEventListener("push", (event) => {
  const data = event.data.json();
  event.waitUntil(self.registration.showNotification(data.title, { body: data.body }));
});
```

### Request

Send to stored subscriptions with `recipient.webpush_tokens`:

```json
{
  "recipient": {
    "webpush_tokens": [
      "{\"endpoint\":\"https://fcm.googleapis.com/fcm/send/<id>\",\"keys\":{\"p256dh\":\"<p256dh>\",\"auth\":\"<auth>\"}}"
    ]
  },
  "notification": {
    "webpush": {
      "headers": {"TTL": "3600", "Urgency": "normal", "Topic": "chat-42"},
      "payload": {"title": "New message", "body": "Hello"}
    }
  }
}
```

Send with `recipient.targets` when each subscription needs its own `ref`, or its own `webpush_payload`, which overrides the request payload:

```json
{
  "recipient": {
    "targets": [
      {
        "provider": "webpush",
        "token": "{\"endpoint\":\"https://fcm.googleapis.com/fcm/send/<id>\",\"keys\":{\"p256dh\":\"<p256dh>\",\"auth\":\"<auth>\"}}",
        "ref": "device-42"
      }
    ]
  },
  "notification": {
    "webpush": {
      "payload": {"title": "New message", "body": "Hello"}
    }
  }
}
```

The answer has one item result per target, in request order:

```json
{
  "result": {
    "results": [
      {"index": 0, "ref": "device-42", "status": "sent", "http_status": 201, "provider_message_id": "<Location header>"},
      {"index": 1, "ref": "device-43", "status": "rejected", "http_status": 410},
      {"index": 2, "ref": "device-44", "status": "rejected", "code": "invalid_subscription"}
    ]
  }
}
```

* Delete the subscription when the item is `rejected` with `http_status` 404 or 410, or with `code` `invalid_subscription`.
* `sent` means that the push service answered with a 2xx status. It does not mean that the browser showed the notification. An endpoint under an allowed origin is trusted to answer honestly.
* `provider_message_id` is the `Location` header of the answer when it is valid UTF-8 without control characters and at most 256 bytes long, and empty otherwise.
* Any other `rejected` status, such as 429 or 5xx, is not a delete answer. `Retry-After` is not read, so the caller resends at its own pace.
* A 3xx answer is not followed. It is `rejected` with its status.
* `unanswered` and `not_sent` follow the rules of direct push send above. `address_blocked` means that nothing left the node, so a resend cannot help.
* Other item results with no provider call: `endpoint_not_allowed`, `payload_too_large` (the payload is over 3993 bytes), `invalid_topic` and `expired`.

### Headers, TTL and Topic

* `headers` takes exactly the names `TTL`, `Urgency` and `Topic`, in that spelling. Any other name, a case variant included, is refused with 107.
* `TTL` is a non-negative integer of seconds. Without a caller `TTL`, it is the seconds left until `expire_at`, with no cap, or 2419200 (four weeks) when `expire_at` is not set. The `TTL` header is always sent. Unlike the PRO documentation, `expire_at` sets the TTL here, as it does for FCM and APNs.
* `Urgency` is one of `very-low`, `low`, `normal`, `high`.
* `Topic` is 1 to 32 characters of the URL-safe base64 alphabet (RFC 8030).
* When the caller sets no `Topic`, the collapse id of the target becomes the `Topic`. A caller `Topic` that differs from a set collapse id is refused with 107.
* A collapse id that is not a valid `Topic` is the item result `invalid_topic` for that target only, so one request-level `collapse_id` that suits APNs does not fail a mixed batch. Set a per-target `collapse_id` to give a Web Push target its own value.
* The payload is one JSON object. A payload of up to 5120 bytes that is not a JSON object, or a webpush target with no payload at all, is refused with 107. A larger payload is not scanned: it is the item result `payload_too_large`, as any payload over 3993 bytes is. The request payload is checked only when a webpush target uses it, that is, when a target has no `webpush_payload` of its own.

### Origins and the address guard

* The default allowed origins are `https://*.googleapis.com`, `https://*.push.apple.com`, `https://*.notify.windows.com` and `https://*.push.services.mozilla.com`.
* `allowed_endpoint_origins` replaces the default list when it is not empty. `extra_allowed_endpoint_origins` adds patterns to the effective list.
* A glob `*` crosses dots, so `https://*.googleapis.com` matches every host of that domain.
* `*` allows any origin. Then a subscription can name any public host, and that host gets a POST from the node with the VAPID header. Avoid it.
* The origin is `https://` plus the lower-case host, and a port only when the URL names one other than 443.
* The endpoint must be an absolute `https` URL with a domain name that is ASCII, has no trailing dot and no user info. An IP literal is refused.
* A connection is refused when its address is loopback, private (RFC 1918 or IPv6 unique local), link-local, unspecified or multicast. It is also refused in `100.64.0.0/10`, `0.0.0.0/8`, `198.18.0.0/15`, `240.0.0.0/4`, `fec0::/10`, `64:ff9b::/96`, `64:ff9b:1::/48`, `2002::/16`, `2001::/32` and `::/96`.
* Redirects are not followed. Proxy environment variables have no effect, so the guard sees the address of the push service.
* A self-hosted push service must have a public address and an `https` domain name, and its origin must be named in `extra_allowed_endpoint_origins`.

### Limits

* Safari and iOS need the web app installed to the Home Screen. A subscription made in a browser tab does not work there.
* All push services share the provider's `max_inflight`. A push service that answers slowly holds slots that targets of the other services wait for, up to `providers.request_timeout` each.
* Every target is encrypted on its own, with a fresh key. The encryption of a payload of the largest size takes about 50 microseconds of one core and allocates about 18 KB, measured on a hosted CI runner with the benchmark of the package (`internal/push/provider/webpush`): a call with 1000 targets costs about 50 milliseconds of CPU.
