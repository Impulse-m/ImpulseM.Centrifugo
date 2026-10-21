package closecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxReasonSize is the longest reason of a close. The reason of a WebSocket close frame is limited to 123 bytes.
const maxReasonSize int = 123

// Range is an inclusive range of close codes.
type Range struct {
	Min uint32
	Max uint32
}

// Absent reports whether no close is set: code 0 with an empty reason.
func Absent(code uint32, reason string) bool {
	return code == 0 && reason == ""
}

// Validate returns an error when the code lies in none of the ranges or the reason is longer than maxReasonSize.
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
	if len(reason) > maxReasonSize {
		return fmt.Errorf("close reason is longer than %d bytes", maxReasonSize)
	}
	return nil
}

// ValidateValue decodes the raw value of a close field into a T and returns the error of validate for it.
func ValidateValue[T any](value json.RawMessage, validate func(*T) error) error {
	var decoded T
	err := json.Unmarshal(value, &decoded)
	if err != nil {
		return err
	}
	return validate(&decoded)
}

// SanitizeBody returns body without the key of a JSON object when the value of the key is an invalid close, so the
// record is still applied with the default close. The value is checked by validate, and the error of an invalid value
// is returned with the sanitized body. A body which is not a JSON object, or a body without an invalid value, is
// returned as it is with a nil error: the request decoder rejects a body which is not a JSON object. A body without
// the key costs no allocation, so the records which do not use a close are not parsed.
func SanitizeBody(body []byte, key string, validate func(value json.RawMessage) error) ([]byte, error) {
	if !mayHaveKey(body, key) {
		return body, nil
	}
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
	sanitized, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		// The body is kept as it is, and the caller gets both errors: the invalid close and why it could not be removed.
		return body, errors.Join(invalid, marshalErr)
	}
	return sanitized, invalid
}

// mayHaveKey reports whether the body may hold key as an object key. The scan allocates nothing. For a body of ASCII
// bytes without escapes it is exact: a key is then spelled out between two quotes, and encoding/json matches it
// without case. Any other body reports true, so it is decoded in full.
func mayHaveKey(body []byte, key string) bool {
	for i := 0; i < len(key); i++ {
		if key[i] >= utf8.RuneSelf {
			return true
		}
	}
	for _, b := range body {
		if b >= utf8.RuneSelf || b == '\\' {
			return true
		}
	}
	for start := 0; start+len(key)+2 <= len(body); start++ {
		if body[start] != '"' || body[start+len(key)+1] != '"' {
			continue
		}
		if equalFoldASCII(body[start+1:start+1+len(key)], key) {
			return true
		}
	}
	return false
}

// equalFoldASCII reports whether the bytes equal the ASCII string without case.
func equalFoldASCII(bytes []byte, text string) bool {
	for i := 0; i < len(text); i++ {
		if lowerASCII(bytes[i]) != lowerASCII(text[i]) {
			return false
		}
	}
	return true
}

// lowerASCII returns the lower case of an ASCII upper case letter, and any other byte as it is.
func lowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
}
