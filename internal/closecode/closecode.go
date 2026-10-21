package closecode

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MaxReasonSize is the longest reason of a close. The reason of a WebSocket close frame is limited to 123 bytes.
const MaxReasonSize int = 123

// Range is an inclusive range of close codes.
type Range struct {
	Min uint32
	Max uint32
}

// Absent reports whether no close is set: code 0 with an empty reason.
func Absent(code uint32, reason string) bool {
	return code == 0 && reason == ""
}

// Validate returns an error when the code lies in none of the ranges or the reason is longer than MaxReasonSize.
// The caller checks Absent first: an absent close is not validated.
func Validate(code uint32, reason string, ranges ...Range) error {
	inRange := false
	for _, codeRange := range ranges {
		if code >= codeRange.Min && code <= codeRange.Max {
			inRange = true
			break
		}
	}
	if !inRange {
		return fmt.Errorf("close code %d is not in the allowed ranges", code)
	}
	if len(reason) > MaxReasonSize {
		return fmt.Errorf("close reason is longer than %d bytes", MaxReasonSize)
	}
	return nil
}

// SanitizeBody returns body without the key of a JSON object when the value of the key is an invalid close, so the
// record is still applied with the default close. The value is checked by validate, and the error of an invalid value
// is returned with the sanitized body. A body which is not a JSON object, or a body without an invalid value, is
// returned as it is with a nil error: the request decoder rejects a body which is not a JSON object.
func SanitizeBody(body []byte, key string, validate func(value json.RawMessage) error) ([]byte, error) {
	var fields map[string]json.RawMessage
	err := json.Unmarshal(body, &fields)
	if err != nil {
		return body, nil
	}
	var invalid error
	// encoding/json matches keys without case, so every key which the decoder would take as the key is checked.
	for name, value := range fields {
		if !strings.EqualFold(name, key) {
			continue
		}
		invalid = validate(value)
		if invalid != nil {
			break
		}
	}
	if invalid == nil {
		return body, nil
	}
	for name := range fields {
		if strings.EqualFold(name, key) {
			delete(fields, name)
		}
	}
	sanitized, err := json.Marshal(fields)
	if err != nil {
		return body, nil
	}
	return sanitized, invalid
}
