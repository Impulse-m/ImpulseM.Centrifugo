// Package apns sends direct push notifications to APNs. The client is ours, on the standard net/http client, and not
// sideshow/apns2: its body decode error drops the HTTP status, its request sends a fixed header set without
// apns-thread-id, and its token package imports a fourth JWT module. The request and response path below answers
// each of those gaps.
package apns

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// Environments of an APNs target and their hosts.
const (
	environmentProduction  = "production"
	environmentDevelopment = "development"
	hostProduction         = "https://api.push.apple.com"
	hostDevelopment        = "https://api.sandbox.push.apple.com"
)

// maxResponseBodyBytes bounds the body of a provider response. The body is never returned.
const maxResponseBodyBytes = 64 << 10

// Provider reasons which mean that the provider token is refused. They can repeat once with a token of a newer window.
const (
	reasonExpiredProviderToken = "ExpiredProviderToken"
	reasonInvalidProviderToken = "InvalidProviderToken"
)

// Option sets a field of a Client. The options are test seams: production code uses none of them.
type Option func(*Client)

// withHost replaces the host of the client.
func withHost(host string) Option {
	return func(client *Client) {
		client.host = host
	}
}

// withTLSConfig replaces the TLS config of the transport of the client.
func withTLSConfig(config *tls.Config) Option {
	return func(client *Client) {
		client.httpClient.Transport.(*http.Transport).TLSClientConfig = config
	}
}

// Client sends the targets of one APNs environment. The clients of a credential share one Credential.
type Client struct {
	host       string
	bundleID   string
	credential *Credential
	httpClient *http.Client
}

// NewClient returns the client of environment for credential, with bundleID as the default topic.
func NewClient(credential *Credential, environment string, bundleID string, options ...Option) (*Client, error) {
	var host string
	switch environment {
	case environmentProduction:
		host = hostProduction
	case environmentDevelopment:
		host = hostDevelopment
	default:
		return nil, fmt.Errorf("apns environment must be %s or %s", environmentProduction, environmentDevelopment)
	}

	client := &Client{
		host:       host,
		bundleID:   bundleID,
		credential: credential,
		httpClient: &http.Client{
			Transport: provider.NewTransport(),
		},
	}
	for _, option := range options {
		option(client)
	}
	return client, nil
}

// response is the provider answer of one request: the status line, the body reason and detail, and the apns-id.
type response struct {
	status    int
	reason    string
	detail    string
	messageID string
}

// Send sends the job to APNs and returns its raw result. It implements provider.Sender.
func (c *Client) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	caller := job.Notification.GetApns().GetHeaders()
	err := CheckHeaders(caller, job.CollapseID)
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	headers := requestHeaders(caller, job.CollapseID, job.Notification.GetExpireAt(), c.bundleID)
	payload := provider.ApnsPayloadOf(job)
	if len(payload) > maxPayloadBytes(headers[headerPushType]) {
		return provider.NewCodeResult(job, provider.CodePayloadTooLarge)
	}

	token, issuedAt, err := c.credential.bearer.Current()
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	answer, answered := c.post(ctx, job, payload, headers, token)
	if answered && answer.status == http.StatusForbidden && providerTokenRefused(answer.reason) && issuedAt < c.credential.bearer.Window() {
		// The bearer is of an earlier window, so one repeat with the bearer of the current window is allowed.
		token, _, err = c.credential.bearer.Current()
		if err == nil {
			answer, answered = c.post(ctx, job, payload, headers, token)
		}
	}

	if !answered {
		return provider.NewResult(job, provider.StatusUnanswered)
	}
	if answer.status >= 200 && answer.status < 300 {
		return answeredResult(job, provider.StatusSent, answer)
	}
	return answeredResult(job, provider.StatusRejected, answer)
}

// post sends one request for job with token. It reports false when no status line was received. Its errors are
// dropped: the text of a net/http error carries the URL, and the URL carries the device token.
func (c *Client) post(ctx context.Context, job *provider.Job, payload []byte, headers map[string]string, token string) (response, bool) {
	requestURL := c.host + "/3/device/" + url.PathEscape(job.Target.GetToken())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(payload))
	if err != nil {
		return response{}, false
	}

	request.Header.Set("authorization", "bearer "+token)
	request.Header.Set("content-type", "application/json; charset=utf-8")
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		return response{}, false
	}
	defer func() {
		_ = httpResponse.Body.Close()
	}()

	answer := response{
		status:    httpResponse.StatusCode,
		messageID: httpResponse.Header.Get(headerID),
	}

	// A body read error keeps the status: the reason is set only from a body that was read whole.
	body, readErr := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponseBodyBytes))
	if readErr != nil {
		return answer, true
	}

	var fields struct {
		Reason    string          `json:"reason"`
		Timestamp json.RawMessage `json:"timestamp"`
	}
	if json.Unmarshal(body, &fields) != nil {
		return answer, true
	}
	answer.reason = fields.Reason
	if answer.status == http.StatusGone {
		answer.detail = string(fields.Timestamp)
	}
	return answer, true
}

// providerTokenRefused reports whether a 403 reason means that the provider token is refused.
func providerTokenRefused(reason string) bool {
	return reason == reasonExpiredProviderToken || reason == reasonInvalidProviderToken
}

// truncate returns value cut to at most the detail limit of a result.
func truncate(value string) string {
	const maxDetailBytes = 256
	if len(value) > maxDetailBytes {
		return value[:maxDetailBytes]
	}
	return value
}

// answeredResult returns the result of job for a provider answer with status. The status line is always kept.
func answeredResult(job *provider.Job, status string, answer response) *apiproto.PushItemResult {
	result := provider.NewResult(job, status)
	result.HttpStatus = uint32(answer.status)
	result.ProviderCode = answer.reason
	result.ProviderDetail = truncate(answer.detail)
	result.ProviderMessageId = answer.messageID
	return result
}
