// Package fcm sends direct push notifications to FCM with the HTTP v1 API. The request is built by byte concatenation,
// so the message bytes of the caller reach FCM unchanged apart from the server additions. The Firebase Admin SDK is
// not used: its messaging.Message is a typed struct that would re-encode the caller JSON.
package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// endpointBase is the base of the FCM HTTP v1 send endpoint.
const endpointBase = "https://fcm.googleapis.com/v1/projects/"

// Option sets a field of a Client. The option is a test seam: production code uses none of them.
type Option func(*Client)

// withFCMEndpoint replaces the send endpoint of the client.
func withFCMEndpoint(endpoint string) Option {
	return func(client *Client) {
		client.endpoint = endpoint
	}
}

// Client sends the targets of one FCM service account.
type Client struct {
	endpoint   string
	gate       *tokenGate
	httpClient *http.Client
}

// NewClient returns the client of account. processCtx is the process-lifetime context: it ends the token fetches at
// shutdown. requestTimeout bounds each token fetch through the HTTP client of the token source, so it must be
// positive: a zero timeout would leave a fetch with no bound but the process context.
func NewClient(processCtx context.Context, account *provider.FCMServiceAccount, requestTimeout time.Duration, options ...Option) (*Client, error) {
	if requestTimeout <= 0 {
		return nil, errors.New("fcm request timeout must be positive")
	}

	// One transport serves the token fetch and the send, so a credential holds one set of connections. The token client
	// keeps its own timeout, and the send is bounded by the context of its call.
	transport := provider.NewTransport()
	tokenHTTP := &http.Client{
		Timeout:   requestTimeout,
		Transport: transport,
	}
	tokenCtx := context.WithValue(processCtx, oauth2.HTTPClient, tokenHTTP)

	client := &Client{
		endpoint: endpointBase + account.ProjectID + "/messages:send",
		gate:     newTokenGate(account.JWT.TokenSource(tokenCtx)),
		httpClient: &http.Client{
			Transport: transport,
		},
	}
	for _, option := range options {
		option(client)
	}
	return client, nil
}

// fcmError is the error body of a failed send request: the status, and the details that name the cause.
type fcmError struct {
	Error struct {
		Status  string `json:"status"`
		Details []struct {
			ErrorCode       string `json:"errorCode"`
			FieldViolations []struct {
				Field string `json:"field"`
			} `json:"fieldViolations"`
		} `json:"details"`
	} `json:"error"`
}

// Send sends the job to FCM and returns its raw result. It implements provider.Sender.
func (c *Client) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	// An expired target is refused before any call, as the dispatcher refuses it too.
	if provider.Expired(job, time.Now()) {
		return provider.NewCodeResult(job, provider.CodeExpired)
	}

	// The size is checked before the message is scanned or copied. A request message is shared by its targets, so a
	// large one would otherwise be copied once per target.
	if Oversized(job) {
		return provider.NewCodeResult(job, provider.CodePayloadTooLarge)
	}
	message := provider.FCMMessageOf(job)
	if len(message) == 0 {
		return provider.NewResult(job, provider.StatusRejected)
	}
	// The request validator answers 107 before any provider call. This check is defence in depth only: a refusal
	// here is a rejected item without a code, and it is never the refusal path of the request.
	err := CheckMessage(message, job.CollapseID)
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	// The token wait is not a provider call: when ctx ends first, nothing is sent and the item is pending. A fetch
	// that fails while ctx is alive is a rejected item without a code and without a provider call.
	token, err := c.gate.Token(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return provider.NewResult(job, provider.StatusPending)
		}
		return provider.NewResult(job, provider.StatusRejected)
	}
	// The token can be ready in the same moment as ctx ends, and the select then picks it. A call past its deadline
	// is still not made.
	if ctx.Err() != nil {
		return provider.NewResult(job, provider.StatusPending)
	}

	// The message token is the device token of the target. The OAuth access token is only the bearer of the request.
	injected, err := injectMessage(message, job.Target.GetToken(), job.CollapseID, job.Notification.GetExpireAt(), time.Now())
	if errors.Is(err, errExpired) {
		return provider.NewCodeResult(job, provider.CodeExpired)
	}
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}
	if len(injected) > maxMessageBytes {
		return provider.NewCodeResult(job, provider.CodePayloadTooLarge)
	}

	return c.post(ctx, job, token.AccessToken, requestBody(injected))
}

// post sends one request with body. Its errors are dropped: a net/http error text can carry the URL.
func (c *Client) post(ctx context.Context, job *provider.Job, accessToken string, body []byte) *apiproto.PushItemResult {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}
	request.Header.Set("authorization", "Bearer "+accessToken)
	request.Header.Set("content-type", "application/json; charset=utf-8")

	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		return provider.NewResult(job, provider.StatusUnanswered)
	}
	defer func() {
		_ = httpResponse.Body.Close()
	}()

	// The status line is kept even when the body cannot be read.
	result := provider.NewResult(job, provider.StatusRejected)
	result.HttpStatus = uint32(httpResponse.StatusCode)
	succeeded := httpResponse.StatusCode >= 200 && httpResponse.StatusCode < 300
	if succeeded {
		result.Status = provider.StatusSent
	}

	responseBody, readErr := provider.ReadAnswer(httpResponse)
	if readErr != nil {
		return result
	}

	if succeeded {
		var success struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(responseBody, &success) == nil {
			result.ProviderMessageId = provider.Truncate(success.Name)
		}
		return result
	}

	var failure fcmError
	if json.Unmarshal(responseBody, &failure) != nil {
		return result
	}
	result.ProviderCode = provider.Truncate(failure.Error.Status)
	result.ProviderDetail = failureDetail(failure)
	return result
}

// failureDetail returns the first FCM error code of the details, else the first field violation, truncated.
func failureDetail(failure fcmError) string {
	for _, detail := range failure.Error.Details {
		if detail.ErrorCode != "" {
			return provider.Truncate(detail.ErrorCode)
		}
	}
	for _, detail := range failure.Error.Details {
		for _, violation := range detail.FieldViolations {
			if violation.Field != "" {
				return provider.Truncate(violation.Field)
			}
		}
	}
	return ""
}
