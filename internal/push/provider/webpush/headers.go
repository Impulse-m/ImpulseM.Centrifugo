package webpush

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// Header names of the Web Push request. They are the only caller headers of a webpush target, and they are spelled exactly
// so: a case variant is refused.
const (
	headerTTL     = "TTL"
	headerUrgency = "Urgency"
	headerTopic   = "Topic"
)

// defaultTTL is the TTL, in seconds, of a target with neither a caller TTL nor an expire_at: four weeks, the PRO default.
const defaultTTL int64 = 2419200

// maxTopicBytes is the longest Topic of RFC 8030.
const maxTopicBytes = 32

// topicAlphabet is the URL-safe base64 alphabet, the only characters of a Topic (RFC 8030).
const topicAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// urgencyValues are the values of the Urgency header of RFC 8030.
var urgencyValues = map[string]bool{
	"very-low": true,
	"low":      true,
	"normal":   true,
	"high":     true,
}

// checkHeaders validates the caller headers of the webpush target at index. The names must be exactly TTL, Urgency and
// Topic. TTL is a non-negative decimal integer of seconds, Urgency one of its values and Topic a valid Topic. A caller Topic
// must equal a set collapse id, and passes unchanged when the collapse id is empty.
func checkHeaders(caller map[string]string, collapseID string, index int) error {
	for name, value := range caller {
		switch name {
		case headerTTL:
			if _, err := strconv.ParseUint(value, 10, 63); err != nil {
				return fmt.Errorf("webpush target %d header TTL is not a non-negative decimal integer", index)
			}
		case headerUrgency:
			if !urgencyValues[value] {
				return fmt.Errorf("webpush target %d header Urgency must be very-low, low, normal or high", index)
			}
		case headerTopic:
			if !validTopic(value) {
				return fmt.Errorf("webpush target %d header Topic must be 1 to 32 characters of A-Z a-z 0-9 - _", index)
			}
			if collapseID != "" && value != collapseID {
				return fmt.Errorf("webpush target %d header Topic differs from the collapse id of the target", index)
			}
		default:
			return fmt.Errorf("webpush target %d header %s is not allowed", index, name)
		}
	}
	return nil
}

// validTopic reports whether topic is 1 to 32 characters of the URL-safe base64 alphabet.
func validTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > maxTopicBytes {
		return false
	}
	for index := 0; index < len(topic); index++ {
		if !strings.ContainsRune(topicAlphabet, rune(topic[index])) {
			return false
		}
	}
	return true
}

// topicOf returns the Topic of job: the caller Topic when set, else the resolved collapse id, else the empty string.
func topicOf(job *provider.Job) string {
	if topic := job.Notification.GetWebpush().GetHeaders()[headerTopic]; topic != "" {
		return topic
	}
	return job.CollapseID
}

// requestHeaders returns the Web Push headers of the request of job: TTL, and Urgency and Topic when set. The caller values
// must already pass checkHeaders. now is the time of the send, so an expire_at is measured from the same instant as the
// expiry check of the send.
func requestHeaders(job *provider.Job, now time.Time) map[string]string {
	caller := job.Notification.GetWebpush().GetHeaders()

	headers := make(map[string]string, 3)
	headers[headerTTL] = ttlOf(caller, job.Notification.GetExpireAt(), now)
	if urgency := caller[headerUrgency]; urgency != "" {
		headers[headerUrgency] = urgency
	}
	if topic := topicOf(job); topic != "" {
		headers[headerTopic] = topic
	}
	return headers
}

// ttlOf returns the TTL header value: the caller TTL when set, else the seconds left to expireAt when it is set, else the
// default. expireAt is in Unix seconds, zero when unset.
func ttlOf(caller map[string]string, expireAt int64, now time.Time) string {
	if ttl, ok := caller[headerTTL]; ok {
		return ttl
	}
	if expireAt > 0 {
		return strconv.FormatInt(expireAt-now.Unix(), 10)
	}
	return strconv.FormatInt(defaultTTL, 10)
}
