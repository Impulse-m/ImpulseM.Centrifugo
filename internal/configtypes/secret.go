package configtypes

import (
	"encoding/json"
	"fmt"
)

// secretMask is what every output of a Secret shows in place of its value.
const secretMask = "[REDACTED]"

// Secret holds key material, such as a PEM private key. Its value is read only by the code that uses the key: every
// output of the value, through Format (every fmt verb, including the bad-verb form of fmt, which would otherwise print
// the raw value), String, GoString, MarshalJSON, MarshalText and MarshalYAML, is the fixed mask. The type is a string,
// as PEMData is, so the code that loads the key converts it.
type Secret string

// Format writes the mask for every fmt verb and flag, so no verb can print the value.
func (s Secret) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(secretMask))
}

// String returns the mask.
func (s Secret) String() string {
	return secretMask
}

// GoString returns the mask, so %#v prints the mask.
func (s Secret) GoString() string {
	return secretMask
}

// MarshalJSON returns the mask as a JSON string.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(secretMask)
}

// MarshalText returns the mask as text.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(secretMask), nil
}

// MarshalYAML returns the mask as a YAML value.
func (s Secret) MarshalYAML() (interface{}, error) {
	return secretMask, nil
}
