package configtypes

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const testSecretValue string = "-----BEGIN PRIVATE KEY-----SECRETKEYBYTES-----END PRIVATE KEY-----"

type secretHolder struct {
	Key Secret `json:"key" yaml:"key"`
}

// Every formatted and marshaled form of a Secret shows the mask, and none of them shows the value.
func TestSecretHidesValue(t *testing.T) {
	secret := Secret(testSecretValue)

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

	forms := []string{
		secret.String(),
		secret.GoString(),
		string(jsonBytes),
		string(textBytes),
		fmt.Sprint(yamlValue),
		string(yamlBytes),
		string(holderJSON),
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
}

// The value stays available to the code that loads the key, through a plain string conversion.
func TestSecretValueReadableByConversion(t *testing.T) {
	secret := Secret(testSecretValue)
	require.Equal(t, testSecretValue, string(secret))
}

// A dump of a structure holding a key shows the mask in every field and never the key bytes.
func TestSecretNotInStructDumpsOfKeyText(t *testing.T) {
	holder := secretHolder{Key: Secret(testSecretValue)}
	dumped := fmt.Sprintf("%+v|%#v|%v", holder, holder, []any{holder})
	require.False(t, strings.Contains(dumped, "SECRETKEYBYTES"), dumped)
}
