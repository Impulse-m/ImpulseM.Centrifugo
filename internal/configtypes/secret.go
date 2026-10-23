package configtypes

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"
)

// secretMask is the text every formatted and encoded form of a Secret shows instead of its value.
const secretMask string = "[REDACTED]"

// Secret holds key material. It is a struct, not a string, because go-toml v2 skips MarshalText on
// string kinds: a string-kind secret would be written to TOML in plain text. The value is set by
// decoding (text, JSON, YAML, the environment and the config file hook) and read only through Value.
// Every formatted and encoded form of a Secret shows the mask.
type Secret struct {
	// value is hidden from the configuration documentation, which walks struct fields.
	value string `expose:"-"`
}

// NewSecret returns a Secret holding value.
func NewSecret(value string) Secret {
	return Secret{value: value}
}

// isSecretType reports whether t is Secret. It is a struct so that it can mask itself in every encoder, but it holds one
// scalar value, so the config tooling shows it as a string.
func isSecretType(t reflect.Type) bool {
	return t == reflect.TypeOf(Secret{})
}

// Value returns the key material. It is the only reader of the value.
func (s Secret) Value() string {
	return s.value
}

// shown returns what every formatted and encoded form of the secret writes: the mask when a value is set, and an
// empty string when none is, so a generated config shows no key for an unset field.
func (s Secret) shown() string {
	if s.value == "" {
		return ""
	}
	return secretMask
}

// String returns the mask, or an empty string when no value is set.
func (s Secret) String() string {
	return s.shown()
}

// GoString returns the mask, so %#v does not show the value.
func (s Secret) GoString() string {
	return s.shown()
}

// Format writes the mask for every verb, so verbs other than %v, %+v and %#v do not show the value.
func (s Secret) Format(state fmt.State, verb rune) {
	_, _ = state.Write([]byte(s.shown()))
}

// MarshalText returns the mask, or an empty text when no value is set.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(s.shown()), nil
}

// MarshalJSON returns the JSON string of the mask, or an empty JSON string when no value is set.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.shown())
}

// MarshalYAML returns the mask for YAML encoders, or an empty string when no value is set.
func (s Secret) MarshalYAML() (interface{}, error) {
	return s.shown(), nil
}

// UnmarshalText sets the value from text, as the environment decoding does.
func (s *Secret) UnmarshalText(text []byte) error {
	s.value = string(text)
	return nil
}

// UnmarshalJSON sets the value from a JSON string.
func (s *Secret) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	s.value = value
	return nil
}

// UnmarshalYAML sets the value from a YAML scalar.
func (s *Secret) UnmarshalYAML(node *yaml.Node) error {
	var value string
	if err := node.Decode(&value); err != nil {
		return err
	}
	s.value = value
	return nil
}

// StringToSecretHookFunc for mapstructure to decode Secret from strings.
func StringToSecretHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(Secret{}) {
			return data, nil
		}

		return NewSecret(reflect.ValueOf(data).String()), nil
	}
}
