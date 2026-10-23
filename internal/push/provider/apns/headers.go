package apns

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/net/http/httpguts"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// Header names of the APNs request.
const (
	headerPushType    = "apns-push-type"
	headerTopic       = "apns-topic"
	headerID          = "apns-id"
	headerExpiration  = "apns-expiration"
	headerPriority    = "apns-priority"
	headerCollapseID  = "apns-collapse-id"
	headerThreadID    = "apns-thread-id"
	pushTypeAlert     = "alert"
	pushTypeVoIP      = "voip"
	priorityImmediate = "10"
	priorityPowerSave = "5"
)

// Length bounds of the caller header values. Every allowed value is at most maxHeaderValueBytes, the apns-topic too, and
// the thread id is at most maxThreadIDBytes. The bounds keep one target from copying a large header into a request.
const (
	maxHeaderValueBytes = 255
	maxThreadIDBytes    = 64
)

// allowedHeaders are the caller headers an APNs request may carry. Any other name is refused: the server sets the
// authorization and the content type itself.
var allowedHeaders = map[string]bool{
	headerTopic:      true,
	headerPushType:   true,
	headerID:         true,
	headerExpiration: true,
	headerPriority:   true,
	headerCollapseID: true,
	headerThreadID:   true,
}

// CheckHeaders validates the caller headers of an APNs target. collapseID is the resolved collapse id of the target,
// empty when there is none. A caller apns-collapse-id must equal a set collapse id, and passes unchanged when the
// collapse id is empty. Every value must be a valid header field value, so net/http never refuses a request after
// the validation: such an answer would be a transport error, which reads as unanswered.
func CheckHeaders(headers map[string]string, collapseID string) error {
	for name, value := range headers {
		if !allowedHeaders[name] {
			return fmt.Errorf("apns header %s is not allowed", name)
		}
		if value == "" || !httpguts.ValidHeaderFieldValue(value) {
			return fmt.Errorf("apns header %s has an empty or invalid value", name)
		}
		if len(value) > maxHeaderValueBytes {
			return fmt.Errorf("apns header %s is longer than %d bytes", name, maxHeaderValueBytes)
		}
		if name == headerThreadID && len(value) > maxThreadIDBytes {
			return fmt.Errorf("apns header %s is longer than %d bytes", name, maxThreadIDBytes)
		}

		switch name {
		case headerID:
			if !isCanonicalUUID(value) {
				return fmt.Errorf("apns header %s is not a canonical lowercase UUID", headerID)
			}
		case headerPriority:
			if value != priorityImmediate && value != priorityPowerSave {
				return fmt.Errorf("apns header %s must be 5 or 10", headerPriority)
			}
		case headerExpiration:
			if _, err := strconv.ParseUint(value, 10, 63); err != nil {
				return fmt.Errorf("apns header %s is not a non-negative decimal integer", headerExpiration)
			}
		case headerCollapseID:
			if collapseID != "" && value != collapseID {
				return fmt.Errorf("apns header %s differs from the collapse id of the target", headerCollapseID)
			}
		}
	}
	return nil
}

// isCanonicalUUID reports whether value is a UUID in the lowercase canonical form, the only form accepted for apns-id.
func isCanonicalUUID(value string) bool {
	if len(value) != 36 || value != strings.ToLower(value) {
		return false
	}
	_, err := uuid.Parse(value)
	return err == nil
}

// maxPayloadBytes returns the payload ceiling of a push type: the VoIP ceiling for VoIP, the regular one otherwise.
func maxPayloadBytes(pushType string) int {
	if pushType == pushTypeVoIP {
		return provider.MaxAPNsVoIPPayloadBytes
	}
	return provider.MaxAPNsPayloadBytes
}

// Oversized reports whether the APNs payload of job is over the ceiling of its push type. The live send and the dry run
// both use it, so a dry run answers payload_too_large for the same payloads as a live send.
func Oversized(job *provider.Job) bool {
	pushType := job.Notification.GetApns().GetHeaders()[headerPushType]
	return len(provider.ApnsPayloadOf(job)) > maxPayloadBytes(pushType)
}

// requestHeaders returns the headers of one APNs request: the server headers, and the caller headers that pass
// through. The caller headers must already pass CheckHeaders. expireAt is the expire_at of the notification in Unix
// seconds, zero when unset.
func requestHeaders(caller map[string]string, collapseID string, expireAt int64, bundleID string) map[string]string {
	headers := make(map[string]string, len(caller)+4)

	pushType := caller[headerPushType]
	if pushType == "" {
		pushType = pushTypeAlert
	}
	headers[headerPushType] = pushType

	topic := caller[headerTopic]
	if topic == "" {
		topic = bundleID
	}
	headers[headerTopic] = topic

	if collapseID != "" {
		headers[headerCollapseID] = collapseID
	} else if value, ok := caller[headerCollapseID]; ok {
		headers[headerCollapseID] = value
	}

	for _, name := range []string{headerID, headerPriority, headerThreadID, headerExpiration} {
		if value, ok := caller[name]; ok {
			headers[name] = value
		}
	}

	if _, ok := caller[headerExpiration]; !ok && expireAt > 0 {
		headers[headerExpiration] = strconv.FormatInt(expireAt, 10)
	}

	return headers
}
