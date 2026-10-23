package configtypes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const testSecretValue string = "-----BEGIN PRIVATE KEY-----SECRETKEYBYTES-----END PRIVATE KEY-----"

type secretHolder struct {
	Key Secret `json:"key" yaml:"key"`
}

// Every formatted and encoded form of a Secret shows the mask, and none of them shows the value.
func TestSecretHidesValue(t *testing.T) {
	secret := NewSecret(testSecretValue)

	jsonBytes, err := json.Marshal(secret)
	require.NoError(t, err)

	textBytes, err := secret.MarshalText()
	require.NoError(t, err)

	yamlValue, err := secret.MarshalYAML()
	require.NoError(t, err)

	yamlBytes, err := yaml.Marshal(secretHolder{Key: secret})
	require.NoError(t, err)

	holderJSON, err := json.Marshal(secretHolder{Key: secret})
	require.NoError(t, err)

	tomlBytes, err := toml.Marshal(secretHolder{Key: secret})
	require.NoError(t, err)

	forms := []string{
		secret.String(),
		secret.GoString(),
		string(jsonBytes),
		string(textBytes),
		fmt.Sprint(yamlValue),
		string(yamlBytes),
		string(holderJSON),
		string(tomlBytes),
		fmt.Sprintf("%v", secret),
		fmt.Sprintf("%+v", secret),
		fmt.Sprintf("%#v", secret),
		fmt.Sprintf("%s", secret),
		fmt.Sprintf("%q", secret),
		fmt.Sprintf("%d", secret),
		fmt.Sprintf("%x", secret),
		fmt.Sprintf("%+v", secretHolder{Key: secret}),
		fmt.Sprintf("%#v", secretHolder{Key: secret}),
	}
	for _, form := range forms {
		require.NotContains(t, form, "SECRETKEYBYTES", "form leaks the value: %s", form)
	}

	require.Equal(t, "[REDACTED]", secret.String())
	require.Equal(t, "[REDACTED]", secret.GoString())
	require.Equal(t, `"[REDACTED]"`, string(jsonBytes))
	require.Equal(t, "[REDACTED]", string(textBytes))
	require.Equal(t, "[REDACTED]", yamlValue)
	require.Equal(t, "[REDACTED]", fmt.Sprintf("%v", secret))
	require.Equal(t, "[REDACTED]", fmt.Sprintf("%s", secret))
	require.Contains(t, string(tomlBytes), "[REDACTED]")
}

// The value stays available to the code that loads the key, through Value.
func TestSecretValueReadable(t *testing.T) {
	secret := NewSecret(testSecretValue)
	require.Equal(t, testSecretValue, secret.Value())
	require.Equal(t, "", NewSecret("").Value())
}

// A dump of a structure holding a key shows the mask in every field and never the key bytes.
func TestSecretNotInStructDumpsOfKeyText(t *testing.T) {
	holder := secretHolder{Key: NewSecret(testSecretValue)}
	dumped := fmt.Sprintf("%+v|%#v|%v", holder, holder, []any{holder})
	require.False(t, strings.Contains(dumped, "SECRETKEYBYTES"), dumped)
}

// Each decoding path sets the value: text (environment variables), JSON and YAML.
func TestSecretDecodingSetsValue(t *testing.T) {
	var fromText Secret
	require.NoError(t, fromText.UnmarshalText([]byte(testSecretValue)))
	require.Equal(t, testSecretValue, fromText.Value())

	var fromJSON Secret
	require.NoError(t, json.Unmarshal([]byte(`"`+testSecretValue+`"`), &fromJSON))
	require.Equal(t, testSecretValue, fromJSON.Value())

	var fromYAML secretHolder
	require.NoError(t, yaml.Unmarshal([]byte("key: "+testSecretValue), &fromYAML))
	require.Equal(t, testSecretValue, fromYAML.Key.Value())
}

// The mapstructure hook turns a string into a Secret, and leaves other types alone.
func TestStringToSecretHookFunc(t *testing.T) {
	hook := StringToSecretHookFunc()
	hookFunc, ok := hook.(func(reflect.Type, reflect.Type, interface{}) (interface{}, error))
	require.True(t, ok)

	decoded, err := hookFunc(reflect.TypeOf(""), reflect.TypeOf(Secret{}), testSecretValue)
	require.NoError(t, err)
	require.Equal(t, NewSecret(testSecretValue), decoded)

	untouched, err := hookFunc(reflect.TypeOf(""), reflect.TypeOf(""), testSecretValue)
	require.NoError(t, err)
	require.Equal(t, testSecretValue, untouched)
}
