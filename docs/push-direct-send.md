# Direct push send

* Direct push send signs the APNs provider token once per 40-minute window per signing key. The token's `iat` is the start of the current window, and a node moves to a new window 60 seconds after each boundary. Nodes that share one key therefore present the same `iat`, so Apple sees at most one token update per key per window. Apple refuses token updates more often than every 20 minutes, and the node clocks must be NTP-synced for the windows to agree. The signature is deterministic (RFC 6979), so nodes that share a key and a window produce the same token bytes. A failed FCM token fetch is not held: it fails only the targets that waited on that fetch, and the next call starts a new fetch. Concurrent fetches of one credential are bounded to one at a time, and each fetch ends within `providers.request_timeout`. A caller stops waiting for a token at its call deadline, but a fetch that is already running can end up to `providers.request_timeout` after that deadline. A target whose token fetch fails is answered `not_sent` with code `token_fetch`, and a target whose deadline ends during the token wait is `not_sent` with code `deadline`. A token fetch that fails because the node shuts down is `not_sent` with code `shutdown`. None of them makes a provider call, so a resend cannot duplicate them.
* A direct send target that never left the node is answered `not_sent`, which replaces `pending` in direct send, with `http_status` 0 and a `code` that names why: `no_slot` (no in-flight slot before the slot wait ended), `connect_failed` (a transport error before the request was written), `deadline` (the call deadline, or the provider request timeout, came before the write), `token_fetch` (the FCM token fetch failed) or `shutdown` (the node shut down before the write). When the node's shutdown and a deadline have both ended, the code is `shutdown`, since the two ends are seen only after the fact. A target is `unanswered` only when its request was written and no status line came back, so a resend may then duplicate it. "Written" is the `WroteRequest` hook of `net/http/httptrace` reporting no error for the attempt. A received status line always keeps `http_status`.
* `collapse_id` of the request is the default of every target, and a target's own `collapse_id` overrides it. The resolved value sets APNs `apns-collapse-id`, and for FCM `android.collapse_key` and `apns.headers.apns-collapse-id`, where the caller set none. When the resolved value is set, a caller `apns-collapse-id`, `android.collapse_key` or `apns.headers.apns-collapse-id` must equal it, and a different value is refused with 107. When the resolved value is empty, the caller values pass unchanged. FCM collapses only messages queued for an offline Android device, so an online Android device can show both messages: dedup by `ref` on the caller side.
* A failed FCM answer carries its raw `error.details` array in `provider_details` (field 9 of `PushItemResult`), unmapped, so a caller reads any FCM detail the server does not name. `provider_detail` is unchanged. An array over 4 KiB is returned as a valid JSON array of its whole elements, with the whitespace removed and cut at an element boundary when it still does not fit, and `provider_details_truncated` (field 10) is set. The bound is per target, not per call: a call with many failing FCM targets can return up to 4 KiB of details for each of them. APNs answers and successes carry no `provider_details`.
* Direct push send (`SendPushNotification` with `recipient.targets`, raw APNs and FCM tokens) is off by default. It needs `push_notifications.enabled` and `push_notifications.sender`, and it runs on sender nodes only; other nodes answer error 108. A send is synchronous: the response carries one raw result per recipient, in request order, and it takes at most `sync_timeout` plus `providers.request_timeout` plus 5 seconds. There is no database, no queue and no retry, so the caller owns the retries. A request body over `max_request_bytes` is refused with error 107. While push notifications are off, no push metric family is exported.
* `timeout_ms` (field 101 of `SendPushNotificationRequest`) sets the deadline of the whole direct send call, in milliseconds. Zero keeps the default deadline, `sync_timeout` plus `providers.request_timeout` plus 5 seconds. A longer value is clamped to that default, not refused. The slot wait ends at the earlier of `sync_timeout` and the call deadline, and each provider call ends at the earlier of `providers.request_timeout` and the call deadline. The clock starts when the request arrives, before its body is read, so the upload of the body is inside the deadline: a body that takes longer than `timeout_ms` to arrive gets no answer, and no provider call is made. The response is written within the call deadline.
* A sender node that serves push over the gRPC API sets `grpc_api.max_receive_message_size` to the push `max_request_bytes`, because gRPC decodes a message before the push checks run.
* An APNs target names its `environment`, `production` or `development`, which picks the APNs host. It is required for every APNs target, and an FCM target must not set it: both cases are refused with 107. The PRO-style `apns_tokens` list has no per-target environment, so its tokens use `push_notifications.apns.endpoint`, the default environment of the node.
* The direct send API is the `CentrifugoApi` service of the proto (`centrifugal.centrifugo.api`), rpc `SendPushNotification`, and the HTTP route `/send_push_notification` under the API prefix. Its fork fields sit in the fork extension blocks of the messages: `PushRecipient.targets` (100), `SendPushNotificationRequest.collapse_id` (100) and `timeout_ms` (101), `SendPushNotificationResult.results` (100), `Error.metadata` (100). The new messages are `PushTarget` (provider, token, environment, ref, collapse_id, apns_payload, fcm_message) and `PushItemResult` (index, ref, status, code, provider_code, provider_detail, http_status, provider_message_id, and the fields 9 and 10 `provider_details` and `provider_details_truncated`). Each release attaches the proto as the `api.proto` asset of the GitHub release, so a caller can generate a client from it.
* Dry run (`push_notifications.dry_run`) runs every check of the live send before a provider call: the recipient count and the body cap, the provider and the environment of each target, the header allow-list and the collapse id, the APNs payload ceilings and the FCM message ceiling after the server additions, and `expire_at`. A refused request is refused with the same error in both modes, and a target gets the same status and code in both, except that a valid target answers `dry_run` where a live send answers `sent`. A dry run loads no credential and makes no provider call.
* A direct send call over `max_concurrent_requests` is answered error 111 `too many requests`, which is temporary. Its `metadata` holds `x-centrifugo-retry-after` set to `"1"`, the same key that a rate limited registry error carries: the caller waits one second and retries, since there is no queue. The refusal comes before the body is read. In the transport error mode it is HTTP 429. The 111 is per call, not per target.
* Route refusals of `SendPushNotification` (errors 108 and 111, the recipient count and 107) follow `http_api.error_mode` like every API method: by default HTTP 200 with the error in the response body, and with `transport` the mapped HTTP status.

## Routing push calls to sender nodes

Direct send (`SendPushNotification` with `recipient.targets`, or the shorthand `fcm_tokens` and `apns_tokens`) runs only on nodes with `push_notifications.sender` set. Any other node answers error 108 `direct send is available on sender nodes only`. A node with push notifications off answers 108 `push notifications are not enabled`. gRPC returns the same `Error` for both, so every protocol gives the same answer. Nodes do not forward calls to each other: the deployment routes them.

Run the sender nodes as their own group, with their own Deployment or instance group, and their own Service, DNS name or load balancer pool. Point the push API calls of the backend at that address, and keep the other nodes for everything else. The sender group needs the provider credentials (`push_notifications.apns.token_key_file` or `token_key_pem`, and `push_notifications.fcm.credentials_file`) and the push settings, all read at startup.

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
