// Package fcm sends direct push notifications to FCM with the HTTP v1 API. The request is built by byte concatenation,
// so the message bytes of the caller reach FCM unchanged apart from the server additions. The Firebase Admin SDK is
// not used: its messaging.Message is a typed struct that would re-encode the caller JSON.
package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// endpointBase is the base of the FCM HTTP v1 send endpoint.
const endpointBase = "https://fcm.googleapis.com/v1/projects/"

// maxResponseBodyBytes bounds the body of a provider response. The body is never returned.
const maxResponseBodyBytes = 64 << 10

// maxDetailBytes bounds the provider detail of a result.
const maxDetailBytes = 256

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
// shutdown. requestTimeout bounds each token fetch through the HTTP client of the token source.
func NewClient(processCtx context.Context, account *provider.FCMServiceAccount, requestTimeout time.Duration, options ...Option) *Client {
	tokenHTTP := &http.Client{
		Timeout:   requestTimeout,
		Transport: provider.NewTransport(),
	}
	tokenCtx := context.WithValue(processCtx, oauth2.HTTPClient, tokenHTTP)

	client := &Client{
		endpoint: endpointBase + account.ProjectID + "/messages:send",
		gate:     newTokenGate(account.JWT.TokenSource(tokenCtx)),
		httpClient: &http.Client{
			Transport: provider.NewTransport(),
		},
	}
	for _, option := range options {
		option(client)
	}
	return client
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
	message := provider.FCMMessageOf(job)
	if len(message) == 0 {
		return provider.NewResult(job, provider.StatusRejected)
	}
	err := CheckMessage(message, job.CollapseID)
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	// The token wait is not a provider call: when ctx ends first, nothing is sent and the item is pending.
	token, err := c.gate.Token(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return provider.NewResult(job, provider.StatusPending)
		}
		return provider.NewResult(job, provider.StatusRejected)
	}

	injected, err := injectMessage(message, token.AccessToken, job.CollapseID, job.Notification.GetExpireAt(), time.Now())
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

	responseBody, readErr := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponseBodyBytes))
	if readErr != nil {
		return result
	}

	if succeeded {
		var success struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(responseBody, &success) == nil {
			result.ProviderMessageId = success.Name
		}
		return result
	}

	var failure fcmError
	if json.Unmarshal(responseBody, &failure) != nil {
		return result
	}
	result.ProviderCode = failure.Error.Status
	result.ProviderDetail = failureDetail(failure)
	return result
}

// failureDetail returns the first FCM error code of the details, else the first field violation, truncated.
func failureDetail(failure fcmError) string {
	for _, detail := range failure.Error.Details {
		if detail.ErrorCode != "" {
			return truncate(detail.ErrorCode)
		}
	}
	for _, detail := range failure.Error.Details {
		for _, violation := range detail.FieldViolations {
			if violation.Field != "" {
				return truncate(violation.Field)
			}
		}
	}
	return ""
}

// truncate returns value cut to at most maxDetailBytes.
func truncate(value string) string {
	if len(value) > maxDetailBytes {
		return value[:maxDetailBytes]
	}
	return value
}
