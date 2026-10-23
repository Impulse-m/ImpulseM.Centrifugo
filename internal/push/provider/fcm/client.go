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

// statusInvalidArgument is the error status of an answer about a bad field of the request.
const statusInvalidArgument = "INVALID_ARGUMENT"

// Option sets a field of a Client. The option is a test seam: production code uses none of them.
type Option func(*Client)

// withFCMEndpoint replaces the send endpoint of the client.
func withFCMEndpoint(endpoint string) Option {
	return func(client *Client) {
		client.endpoint = endpoint
	}
}

// withFCMTransport replaces the transport of the send requests, so a test can fail a write the way a connection does.
func withFCMTransport(roundTripper http.RoundTripper) Option {
	return func(client *Client) {
		client.httpClient.Transport = roundTripper
	}
}

// Client sends the targets of one FCM service account.
type Client struct {
	endpoint   string
	gate       *tokenGate
	httpClient *http.Client
	// shutdown is the process-lifetime context. Its end classifies a token fetch that failed as shutdown.
	shutdown context.Context
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
		gate:     newShutdownTokenGate(account.JWT.TokenSource(tokenCtx), processCtx),
		httpClient: &http.Client{
			Transport: transport,
		},
		shutdown: processCtx,
	}
	for _, option := range options {
		option(client)
	}
	return client, nil
}

// fcmError is the error body of a failed send request: the status, and the details that name the cause. The details
// stay raw, so that the array can be passed on unchanged; they are decoded only to name the cause.
type fcmError struct {
	Error struct {
		Status  string          `json:"status"`
		Details json.RawMessage `json:"details"`
	} `json:"error"`
}

// fcmDetail is one element of the details of an error: an error code, or field violations of the request.
type fcmDetail struct {
	ErrorCode       string `json:"errorCode"`
	FieldViolations []struct {
		Field string `json:"field"`
	} `json:"fieldViolations"`
}

// maxProviderDetailsBytes bounds the raw details array of a result.
const maxProviderDetailsBytes = 4 << 10

// Send sends the job to FCM and returns its raw result. It implements provider.Sender.
func (c *Client) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	// An expired target is refused before any call, as the dispatcher refuses it too.
	if provider.Expired(job, time.Now()) {
		return provider.NewCodeResult(job, provider.CodeExpired)
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

	// The body is built once: the set has built it already before the dry run answer, and a job sent without the set
	// is built here. The size is checked before the copy, so an over-limit message is never copied.
	if job.Body == nil && Prepare(job) {
		return provider.NewCodeResult(job, provider.CodePayloadTooLarge)
	}
	if job.Body == nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	// The token wait is not a provider call. When it ends without a token, nothing was sent, so the item is not_sent and a
	// resend cannot duplicate a message. A wait that ended with the call context has no code: the set names the cause.
	// A fetch that failed while the call context was alive is token_fetch, unless the node is shutting down: a fetch
	// that the shutdown cancelled is shutdown, because the fetch failed for the shutdown and not for the provider.
	token, err := c.gate.Token(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return provider.NewNotSentResult(job, "")
		}
		if c.shutdown != nil && c.shutdown.Err() != nil {
			return provider.NewNotSentResult(job, provider.CodeShutdown)
		}
		return provider.NewNotSentResult(job, provider.CodeTokenFetch)
	}
	// The token can be ready in the same moment as ctx ends, and the select then picks it. A call past its deadline
	// is still not made.
	if ctx.Err() != nil {
		return provider.NewNotSentResult(job, "")
	}

	// The OAuth access token is only the bearer of the request. The message token is the device token of the target,
	// and it is already in the body.
	return c.post(ctx, job, token.AccessToken, requestBody(job.Body))
}

// post sends one request with body. Its errors are dropped: a net/http error text can carry the URL.
func (c *Client) post(ctx context.Context, job *provider.Job, accessToken string, body []byte) *apiproto.PushItemResult {
	trace := &provider.WriteTrace{}
	request, err := http.NewRequestWithContext(trace.Trace(ctx), http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}
	request.Header.Set("authorization", "Bearer "+accessToken)
	request.Header.Set("content-type", "application/json; charset=utf-8")

	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		return provider.NoAnswerResult(ctx, job, trace.Written())
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
	// The details array is split into its elements once. The typed details and the raw result both read the elements.
	elements := rawElements(failure.Error.Details)
	details := decodeDetails(elements)
	result.ProviderDetail = failureDetail(failure.Error.Status, details)
	result.ProviderDetails, result.ProviderDetailsTruncated = providerDetails(failure.Error.Details, elements)
	return result
}

// rawElements returns the elements of a raw details array. A value that is not an array has none, and nil is returned.
func rawElements(raw json.RawMessage) []json.RawMessage {
	var elements []json.RawMessage
	if json.Unmarshal(raw, &elements) != nil {
		return nil
	}
	return elements
}

// decodeDetails returns the typed details of the elements of a details array. One element that is not an object makes
// the whole array have no typed details.
func decodeDetails(elements []json.RawMessage) []fcmDetail {
	details := make([]fcmDetail, 0, len(elements))
	for _, element := range elements {
		var detail fcmDetail
		if json.Unmarshal(element, &detail) != nil {
			return nil
		}
		details = append(details, detail)
	}
	return details
}

// failureDetail returns the first FCM error code of the details, else the first field violation, truncated. For an
// INVALID_ARGUMENT answer the field violation comes first: it names the bad field, such as message.token, and an error
// code of the same answer would hide which field is bad.
func failureDetail(status string, details []fcmDetail) string {
	if status == statusInvalidArgument {
		if field := firstFieldViolation(details); field != "" {
			return provider.Truncate(field)
		}
	}
	for _, detail := range details {
		if detail.ErrorCode != "" {
			return provider.Truncate(detail.ErrorCode)
		}
	}
	return provider.Truncate(firstFieldViolation(details))
}

// firstFieldViolation returns the first non-empty field of the field violations of the details, or empty.
func firstFieldViolation(details []fcmDetail) string {
	for _, detail := range details {
		for _, violation := range detail.FieldViolations {
			if violation.Field != "" {
				return violation.Field
			}
		}
	}
	return ""
}

// providerDetails returns the raw details array of a failed answer for the result, and whether it is not the array the
// provider sent. An array within the limit is returned as it was sent, and it is not flagged. An array over the limit is
// returned as a valid JSON array of its elements: the elements that fit are kept whole and in order, with the whitespace
// between them removed, and the result is flagged. The flag is set for every array over the limit, because a cut array
// and a compacted array are both not the bytes of the provider. A value that is not an array, and no details, give no
// bytes.
func providerDetails(raw json.RawMessage, elements []json.RawMessage) ([]byte, bool) {
	if elements == nil {
		return nil, false
	}
	if len(raw) <= maxProviderDetailsBytes {
		return raw, false
	}

	var out bytes.Buffer
	out.WriteByte('[')
	for index, element := range elements {
		separator := 0
		if index > 0 {
			separator = 1
		}
		// The closing bracket is counted with the element, so the cut array never exceeds the limit.
		if out.Len()+separator+len(element)+1 > maxProviderDetailsBytes {
			break
		}
		if separator == 1 {
			out.WriteByte(',')
		}
		out.Write(element)
	}
	out.WriteByte(']')
	return out.Bytes(), true
}
