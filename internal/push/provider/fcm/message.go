package fcm

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// maxMessageBytes bounds the whole message JSON after the server additions.
const maxMessageBytes = 4096

// Paths of the server additions of an FCM message.
const (
	pathToken          = "token"
	pathCollapseKey    = "android.collapse_key"
	pathTTL            = "android.ttl"
	pathAPNsCollapseID = "apns.headers.apns-collapse-id"
	pathAPNsExpiration = "apns.headers.apns-expiration"
)

// errExpired reports that the expiry of the notification is at or before the time of the injection.
var errExpired = errors.New("expire_at is not after now")

// CheckMessage validates the caller message of an FCM target. The message must be one valid JSON object: the object
// check of gjson looks at the first byte only, so the whole message is validated too. The token is set by the server,
// so a caller token is refused. When collapseID is set, a caller collapse key on either field must equal it. When it
// is empty, the caller values pass unchanged.
func CheckMessage(message []byte, collapseID string) error {
	if !gjson.ValidBytes(message) || !gjson.ParseBytes(message).IsObject() {
		return errors.New("message is not a JSON object")
	}
	if gjson.GetBytes(message, pathToken).Exists() {
		return errors.New("message.token is set by the server")
	}

	if collapseID == "" {
		return nil
	}
	for _, path := range []string{pathCollapseKey, pathAPNsCollapseID} {
		value := gjson.GetBytes(message, path)
		if value.Exists() && value.String() != collapseID {
			return fmt.Errorf("%s differs from the collapse id of the target", path)
		}
	}
	return nil
}

// The widest form of each server addition of injectMessage, in bytes: its keys, quotes, wrapper objects and comma. A
// decimal expiry is at most twenty digits. The forms are upper bounds, so a message that fits the bound fits the limit.
const (
	maxDecimalDigits   = 20
	tokenAddition      = len(`"token":"",`)
	collapseAddition   = len(`"android":{"collapse_key":""},`) + len(`"apns":{"headers":{"apns-collapse-id":""}},`)
	ttlAddition        = len(`"android":{"ttl":"s"},`) + maxDecimalDigits
	expirationAddition = len(`"apns":{"headers":{"apns-expiration":""}},`) + maxDecimalDigits
)

// jsonEscapedBound returns an upper bound of the length of value written as a JSON string. A byte that may be escaped
// is counted as six bytes, the length of a \u escape, so the bound holds for any escaping of the encoder.
func jsonEscapedBound(value string) int {
	size := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			size += 6
			continue
		}
		size++
	}
	return size
}

// injectionBound returns an upper bound of the growth of a message under injectMessage for the token, the collapse id
// and the expiry. The token is written once and the collapse id twice, once on each field. Each addition is counted
// only when injectMessage makes it, so the bound is exact for a target with a token alone.
func injectionBound(token string, collapseID string, expireAt int64) int {
	bound := tokenAddition + jsonEscapedBound(token)
	if collapseID != "" {
		bound += collapseAddition + 2*jsonEscapedBound(collapseID)
	}
	if expireAt > 0 {
		bound += ttlAddition + expirationAddition
	}
	return bound
}

// fitsInjected reports whether message stays within the message limit after injectMessage for the token, the collapse
// id and the expiry. It is checked before the copy: a message that cannot fit is refused without any copy, so one
// shared request message is never copied for every target past the limit.
func fitsInjected(message []byte, token string, collapseID string, expireAt int64) bool {
	return len(message)+injectionBound(token, collapseID, expireAt) <= maxMessageBytes
}

// injectMessage returns the message with the server additions: the token, and the collapse id and the expiry on the
// Android and APNs fields of the message. Each addition is made only where the caller set nothing, so the caller
// bytes are kept otherwise. A resolved collapse id is set on both collapse fields; expireAt is in Unix seconds, zero
// when unset. An expireAt at or before now returns errExpired, so the time to live is never negative.
func injectMessage(message []byte, token string, collapseID string, expireAt int64, now time.Time) ([]byte, error) {
	remaining := expireAt - now.Unix()
	if expireAt > 0 && remaining <= 0 {
		return nil, errExpired
	}

	result, err := sjson.SetBytes(message, pathToken, token)
	if err != nil {
		return nil, err
	}

	if collapseID != "" {
		result, err = setIfAbsent(result, pathCollapseKey, collapseID)
		if err != nil {
			return nil, err
		}
		result, err = setIfAbsent(result, pathAPNsCollapseID, collapseID)
		if err != nil {
			return nil, err
		}
	}

	if expireAt > 0 {
		result, err = setIfAbsent(result, pathTTL, strconv.FormatInt(remaining, 10)+"s")
		if err != nil {
			return nil, err
		}
		// The expiry is a JSON string, as the APNs header value is.
		result, err = setIfAbsent(result, pathAPNsExpiration, strconv.FormatInt(expireAt, 10))
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// setIfAbsent sets path to value when the message has no value at path.
func setIfAbsent(message []byte, path string, value string) ([]byte, error) {
	if gjson.GetBytes(message, path).Exists() {
		return message, nil
	}
	return sjson.SetBytes(message, path, value)
}

// requestBody returns the body of one send request: the message object with the injected message.
func requestBody(message []byte) []byte {
	body := make([]byte, 0, len(message)+len(`{"message":}`))
	body = append(body, `{"message":`...)
	body = append(body, message...)
	return append(body, '}')
}
