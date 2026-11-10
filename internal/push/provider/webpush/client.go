package webpush

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/origin"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// Option sets a field of a Client. The options are test seams: production code uses none of them.
type Option func(*Client)

// withTLSConfig replaces the TLS config of the transport of the client.
func withTLSConfig(config *tls.Config) Option {
	return func(client *Client) {
		client.httpClient.Transport.(*http.Transport).TLSClientConfig = config
	}
}

// withRoundTripper replaces the transport of the client, so a test can fail a write the way a connection does.
func withRoundTripper(roundTripper http.RoundTripper) Option {
	return func(client *Client) {
		client.httpClient.Transport = roundTripper
	}
}

// withClock replaces the clock of the client: the expiry check, the TTL and the VAPID token age.
func withClock(now func() time.Time) Option {
	return func(client *Client) {
		client.now = now
	}
}

// withRandom replaces the source of the ephemeral key and the salt of each message.
func withRandom(random randomSource) Option {
	return func(client *Client) {
		client.random = random
	}
}

// withoutAddressGuard replaces the dialer of the client with a plain one, so a test server on a loopback address is reached.
// Production code never uses it: the guard is always on.
func withoutAddressGuard() Option {
	return func(client *Client) {
		client.httpClient.Transport.(*http.Transport).DialContext = (&net.Dialer{Timeout: dialTimeout}).DialContext
	}
}

// Client sends the targets of Web Push subscriptions. It holds the VAPID key and the table of tokens, the allowed origins
// and the transport. It keeps no subscription and no payload.
type Client struct {
	publicKey  string
	origins    *origin.PatternChecker
	vapid      *vapidTable
	httpClient *http.Client
	now        func() time.Time
	random     randomSource
}

// response is the provider answer of one request: the status line, the reason and detail of a body, and the Location.
type response struct {
	status    int
	reason    string
	detail    string
	messageID string
}

// attempt is the outcome of one request. answered is set when a status line was received. written is set when the request
// was written, and refused when the address guard refused a connection of the request.
type attempt struct {
	answer   response
	answered bool
	written  bool
	refused  bool
}

// errRequestBuild is the error of a request that could not be built locally. No request was made.
var errRequestBuild = errors.New("webpush request could not be built")

// NewClient returns the sender of the Web Push targets. The key is the VAPID private key and publicKey its base64url form,
// which the frontend uses as its application server key. subject is the sub claim of the token, and origins decides which
// push service origins are allowed.
func NewClient(
	key *ecdsa.PrivateKey,
	publicKey string,
	subject string,
	origins *origin.PatternChecker,
	options ...Option,
) (*Client, error) {
	if origins == nil {
		return nil, errors.New("webpush origin patterns are required")
	}

	client := &Client{
		publicKey: publicKey,
		origins:   origins,
		now:       time.Now,
		random:    systemRandom,
		httpClient: &http.Client{
			Transport:     newTransport(),
			CheckRedirect: noRedirect,
		},
	}
	for _, option := range options {
		option(client)
	}

	vapid, err := newVAPIDTable(key, subject, client.now)
	if err != nil {
		return nil, err
	}
	client.vapid = vapid
	return client, nil
}

// newTransport returns the transport of the client: the shared provider transport, with the guarded dialer and its own TLS
// handshake timeout. It has no proxy, so the guard sees the address of the push service.
func newTransport() *http.Transport {
	transport := provider.NewTransport()
	transport.DialContext = dialContext
	transport.TLSHandshakeTimeout = tlsHandshakeTimeout
	return transport
}

// noRedirect stops the client at the first answer: a 3xx is the answer of the push service, and it is not followed.
func noRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// Send sends the job to its endpoint and returns its raw result. It implements provider.Sender. The item checks run first,
// and the payload is encrypted after the caller holds the in-flight slot, so a target that waits holds no encrypted body.
func (c *Client) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	now := c.now()
	// An expired target is refused before any call, as the dispatcher refuses it too.
	if provider.Expired(job, now) {
		return provider.NewCodeResult(job, provider.CodeExpired)
	}

	target, code := prepare(job, c.origins)
	if code != "" {
		return provider.NewCodeResult(job, code)
	}

	token, err := c.vapid.token(target.origin)
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}
	authorization := "vapid t=" + token + ", k=" + c.publicKey

	ephemeral, salt, err := c.random()
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}
	body, err := encrypt(provider.WebPushPayloadOf(job), target.receiver, target.auth, ephemeral, salt)
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	result, err := c.post(ctx, target, body, authorization, requestHeaders(job, now))
	if err != nil {
		return provider.NewResult(job, provider.StatusRejected)
	}

	if !result.answered {
		// A connection the guard refused never left the node, so a resend cannot help: it is not_sent with its own code.
		if result.refused && !result.written {
			return provider.NewNotSentResult(job, provider.CodeAddressBlocked)
		}
		return provider.NoAnswerResult(ctx, job, result.written)
	}
	if result.answer.status >= 200 && result.answer.status < 300 {
		return answeredResult(job, provider.StatusSent, result.answer)
	}
	return answeredResult(job, provider.StatusRejected, result.answer)
}

// post sends the request of one attempt to the endpoint of target, with the encrypted body and the headers, and reads the
// answer. The errors of the transport are dropped: a net/http error text carries the URL, and the URL carries the
// subscription. A request that cannot be built is the only error returned, and no request was made for it.
func (c *Client) post(
	ctx context.Context,
	target *delivery,
	body []byte,
	authorization string,
	headers map[string]string,
) (attempt, error) {
	trace := &provider.WriteTrace{}
	guard := &addressGuard{}
	request, err := http.NewRequestWithContext(
		guard.Context(trace.Trace(ctx)),
		http.MethodPost,
		target.requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return attempt{}, errRequestBuild
	}

	request.Header.Set("content-encoding", "aes128gcm")
	request.Header.Set("content-type", "application/octet-stream")
	request.Header.Set("authorization", authorization)
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	httpResponse, err := c.httpClient.Do(request)
	if err != nil {
		return attempt{written: trace.Written(), refused: guard.Refused()}, nil
	}
	defer func() {
		_ = httpResponse.Body.Close()
	}()

	answer := response{status: httpResponse.StatusCode}
	if answer.status >= 200 && answer.status < 300 {
		// The Location of a 2xx is the message id. It is kept only when it is printable, so the body is not read.
		answer.messageID = provider.PrintableHeader(httpResponse.Header.Get("Location"))
		return attempt{answer: answer, answered: true, written: true}, nil
	}

	// A body read error keeps the status: the reason and the detail are set only from a body that was read whole.
	answerBody, readErr := provider.ReadAnswer(httpResponse)
	if readErr == nil {
		answer.reason, answer.detail = providerFields(answerBody)
	}
	return attempt{answer: answer, answered: true, written: true}, nil
}

// providerFields returns the reason and the detail of a provider error body. The reason is the string reason of the body
// when present, else the string error. The detail is the string message. A body that is not a JSON object gives none.
func providerFields(body []byte) (string, string) {
	var fields struct {
		Reason  *string `json:"reason"`
		Error   *string `json:"error"`
		Message *string `json:"message"`
	}
	if json.Unmarshal(body, &fields) != nil {
		return "", ""
	}

	var reason string
	switch {
	case fields.Reason != nil:
		reason = *fields.Reason
	case fields.Error != nil:
		reason = *fields.Error
	}
	var detail string
	if fields.Message != nil {
		detail = *fields.Message
	}
	return reason, detail
}

// answeredResult returns the result of job for a provider answer with status. The status line is always kept. Every
// provider string is cut to the field limit, so one answer cannot put its full size into a result.
func answeredResult(job *provider.Job, status string, answer response) *apiproto.PushItemResult {
	result := provider.NewResult(job, status)
	result.HttpStatus = uint32(answer.status)
	result.ProviderCode = provider.Truncate(answer.reason)
	result.ProviderDetail = provider.Truncate(answer.detail)
	result.ProviderMessageId = provider.Truncate(answer.messageID)
	return result
}
