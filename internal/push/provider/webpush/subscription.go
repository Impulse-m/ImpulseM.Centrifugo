// Package webpush sends direct push notifications to Web Push subscriptions: the protocol of RFC 8030, with a VAPID token of
// RFC 8292 and a payload encrypted with RFC 8291 and the aes128gcm coding of RFC 8188. The package checks the request and
// each subscription, encrypts the payload of one target and sends it. It keeps no subscription and no payload, and the
// only state it keeps is the table of VAPID tokens.
package webpush

import (
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/origin"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// DefaultEndpointOrigins is the built-in list of allowed push service origins, as the documentation of Centrifugo PRO
// lists it. A glob star crosses dots, so each pattern names every host of one push service domain.
var DefaultEndpointOrigins []string = []string{
	"https://*.googleapis.com",
	"https://*.push.apple.com",
	"https://*.notify.windows.com",
	"https://*.push.services.mozilla.com",
}

// defaultPort is the port of an https URL that the origin of an endpoint leaves out.
const defaultPort uint64 = 443

// keyLengths are the decoded lengths of the subscription keys: the p256dh is an uncompressed P-256 point, and the auth
// secret is 16 bytes.
const (
	p256dhLength = 65
	authLength   = 16
)

// errSubscriptionShape is the error of a token that is not the PushSubscription JSON of a Web Push target. The message
// names the shape and never a value.
var errSubscriptionShape error = errors.New("is not one JSON object with a string endpoint and an object keys with strings p256dh and auth")

// subscriptionKeys are the keys of a PushSubscription, as the browser gives them.
type subscriptionKeys struct {
	P256dh *string `json:"p256dh"`
	Auth   *string `json:"auth"`
}

// subscriptionToken is the PushSubscription JSON of one webpush target. Each field is a pointer, so a field that is absent
// or not of its JSON type is told apart from an empty one.
type subscriptionToken struct {
	Endpoint *string           `json:"endpoint"`
	Keys     *subscriptionKeys `json:"keys"`
}

// delivery is a webpush target that passed the item checks: the URL it is posted to, the origin that the VAPID token is
// bound to, and the subscription keys that the payload is encrypted for.
type delivery struct {
	requestURL string
	origin     string
	receiver   *ecdh.PublicKey
	auth       []byte
}

// EndpointOrigins returns the effective origin patterns: allowed, or the built-in list when allowed is empty, followed by
// extra. The result is a new slice, so the caller may keep it.
func EndpointOrigins(allowed []string, extra []string) []string {
	base := allowed
	if len(base) == 0 {
		base = DefaultEndpointOrigins
	}
	patterns := make([]string, 0, len(base)+len(extra))
	patterns = append(patterns, base...)
	return append(patterns, extra...)
}

// CheckRequest returns the error that refuses the whole request for the webpush target of job, or nil. It checks the shape
// of the target: the token, the environment, the payload and the headers. The error names the field and the reason, and
// never a value. The subscription values and the origin checks are item results, so they are not checked here.
func CheckRequest(job *provider.Job) error {
	if _, err := parseSubscription(job.Target.GetToken()); err != nil {
		return fmt.Errorf("webpush target %d token %w", job.Index, err)
	}
	if job.Target.GetEnvironment() != "" {
		return fmt.Errorf("webpush target %d environment must be empty", job.Index)
	}

	payload := provider.WebPushPayloadOf(job)
	if len(payload) == 0 {
		return fmt.Errorf("webpush target %d needs a payload: notification.webpush.payload or webpush_payload", job.Index)
	}
	// A payload over the scan threshold is not scanned: it is an item answered payload_too_large. The threshold is the
	// APNs one, the largest ceiling of the server.
	if len(payload) <= provider.MaxAPNsVoIPPayloadBytes && !provider.IsJSONObject(payload) {
		return fmt.Errorf("webpush target %d payload is not a JSON object", job.Index)
	}

	return checkHeaders(job.Notification.GetWebpush().GetHeaders(), job.CollapseID, job.Index)
}

// Refusal returns the item code that refuses job with no provider call, or the empty string for a job to send. The checks
// are the ones of the specification: the subscription, the endpoint origin, the Topic and the payload ceiling. The result
// is the same in a live send and in dry run. It loads no key and opens no connection.
func Refusal(job *provider.Job, origins *origin.PatternChecker) string {
	_, code := prepare(job, origins)
	return code
}

// prepare runs the item checks of a webpush target in the order of the specification. It returns the delivery of a valid
// target, or the item code that refuses it. It opens no connection and encrypts nothing.
func prepare(job *provider.Job, origins *origin.PatternChecker) (*delivery, string) {
	subscription, err := parseSubscription(job.Target.GetToken())
	if err != nil {
		return nil, provider.CodeInvalidSubscription
	}

	requestURL, endpointOrigin, ok := endpointOf(*subscription.Endpoint)
	if !ok {
		return nil, provider.CodeInvalidSubscription
	}
	receiver, auth, ok := decodeKeys(subscription.Keys)
	if !ok {
		return nil, provider.CodeInvalidSubscription
	}

	if !origins.MatchOrigin(endpointOrigin) {
		return nil, provider.CodeEndpointNotAllowed
	}

	topic := topicOf(job)
	if topic != "" && !validTopic(topic) {
		return nil, provider.CodeInvalidTopic
	}

	if len(provider.WebPushPayloadOf(job)) > provider.MaxWebPushPayloadBytes {
		return nil, provider.CodePayloadTooLarge
	}

	return &delivery{
		requestURL: requestURL,
		origin:     endpointOrigin,
		receiver:   receiver,
		auth:       auth,
	}, ""
}

// parseSubscription reads the PushSubscription JSON of a token. It checks the shape only: one JSON object with a string
// endpoint and an object keys with strings p256dh and auth. The values are checked by prepare.
func parseSubscription(token string) (*subscriptionToken, error) {
	if !provider.IsJSONObject([]byte(token)) {
		return nil, errSubscriptionShape
	}

	var subscription subscriptionToken
	if err := json.Unmarshal([]byte(token), &subscription); err != nil {
		return nil, errSubscriptionShape
	}
	if subscription.Endpoint == nil || subscription.Keys == nil || subscription.Keys.P256dh == nil || subscription.Keys.Auth == nil {
		return nil, errSubscriptionShape
	}
	return &subscription, nil
}

// endpointOf returns the request URL of an endpoint and the origin it is bound to. The endpoint must be an absolute https
// URL without user info, and its host must be an ASCII domain name, not an IP literal and without a trailing dot. The
// origin is https:// and the lower-case host, plus the port only when the URL names one other than 443. The request URL
// is built from the origin, so the string that is matched is the string that is dialed. ok is false for a refused endpoint.
func endpointOf(endpoint string) (requestURL string, endpointOrigin string, ok bool) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil {
		return "", "", false
	}

	host := parsed.Hostname()
	if !isDomainName(host) {
		return "", "", false
	}

	endpointOrigin = "https://" + strings.ToLower(host)
	if port := parsed.Port(); port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil || number == 0 {
			return "", "", false
		}
		if number != defaultPort {
			endpointOrigin += ":" + strconv.FormatUint(number, 10)
		}
	}

	return endpointOrigin + parsed.RequestURI(), endpointOrigin, true
}

// isDomainName reports whether host is an ASCII domain name: letters, digits, hyphens and dots, without a trailing dot,
// and not an IP literal.
func isDomainName(host string) bool {
	if host == "" || strings.HasSuffix(host, ".") {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	for index := 0; index < len(host); index++ {
		if !isDomainByte(host[index]) {
			return false
		}
	}
	return true
}

// isDomainByte reports whether character may appear in an ASCII domain name.
func isDomainByte(character byte) bool {
	switch {
	case character >= 'a' && character <= 'z':
		return true
	case character >= 'A' && character <= 'Z':
		return true
	case character >= '0' && character <= '9':
		return true
	case character == '-' || character == '.':
		return true
	default:
		return false
	}
}

// decodeKeys decodes the keys of a subscription. The p256dh must be an uncompressed point of P-256, and the auth secret must
// be 16 bytes. Both are base64url, with or without "=" padding. ok is false when a key is refused.
func decodeKeys(keys *subscriptionKeys) (*ecdh.PublicKey, []byte, bool) {
	point, ok := decodeBase64URL(*keys.P256dh)
	if !ok || len(point) != p256dhLength {
		return nil, nil, false
	}
	receiver, err := ecdh.P256().NewPublicKey(point)
	if err != nil {
		return nil, nil, false
	}

	auth, ok := decodeBase64URL(*keys.Auth)
	if !ok || len(auth) != authLength {
		return nil, nil, false
	}
	return receiver, auth, true
}

// decodeBase64URL decodes base64url with or without "=" padding. ok is false for a value that is not base64url.
func decodeBase64URL(value string) ([]byte, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
	if err != nil {
		return nil, false
	}
	return raw, true
}
