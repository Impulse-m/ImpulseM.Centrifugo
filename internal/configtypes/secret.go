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

// Value returns the key material. It is the only reader of the value.
func (s Secret) Value() string {
	return s.value
}

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

// MarshalText returns the mask.
func (s Secret) MarshalText() ([]byte, error) {
	return []byte(secretMask), nil
}

// MarshalJSON returns the JSON string of the mask.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(secretMask)
}

// MarshalYAML returns the mask for YAML encoders.
func (s Secret) MarshalYAML() (interface{}, error) {
	return secretMask, nil
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
