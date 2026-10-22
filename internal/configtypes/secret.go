package configtypes

import (
	"encoding/json"
	"fmt"
)

// secretMask is the text every formatted and marshaled form of a Secret shows instead of its value.
const secretMask string = "[REDACTED]"

// Secret is a string that holds key material. Every formatted and marshaled form of a Secret shows
// the fixed mask, so the value does not reach logs, dumps or documentation. The only code that reads
// the value converts it with string(secret).
type Secret string

// String returns the mask.
func (s Secret) String() string {
	return secretMask
}

// GoString returns the mask, so %#v does not show the value.
func (s Secret) GoString() string {
	return secretMask
}

// Format writes the mask for every verb, so verbs other than %v, %+v and %#v do not show the value.
func (s Secret) Format(state fmt.State, verb rune) {
	_, _ = state.Write([]byte(secretMask))
}

// MarshalJSON returns the JSON string of the mask.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(secretMask)
}

// MarshalText returns the mask.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(secretMask), nil
}

// MarshalYAML returns the mask for YAML encoders.
func (s Secret) MarshalYAML() (interface{}, error) {
	return secretMask, nil
}
