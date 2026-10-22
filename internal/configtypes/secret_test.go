package configtypes

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// secretKeyBytes is a stand-in for key material. No output of a Secret may contain it.
const secretKeyBytes = "-----BEGIN PRIVATE KEY-----\nSECRET-KEY-BYTES\n-----END PRIVATE KEY-----"

// secretHolder carries a Secret in an exported field, as the config structs do.
type secretHolder struct {
	TokenKeyPEM Secret `json:"token_key_pem" yaml:"token_key_pem"`
}

func TestSecretOutputsAreMasked(t *testing.T) {
	secret := Secret(secretKeyBytes)

	require.Equal(t, secretMask, secret.String())
	require.Equal(t, secretMask, secret.GoString())

	text, err := secret.MarshalText()
	require.NoError(t, err)
	require.Equal(t, secretMask, string(text))

	jsonBytes, err := secret.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `"`+secretMask+`"`, string(jsonBytes))

	yamlValue, err := secret.MarshalYAML()
	require.NoError(t, err)
	require.Equal(t, secretMask, yamlValue)
}

func TestSecretFormatVerbsAreMasked(t *testing.T) {
	secret := Secret(secretKeyBytes)

	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		output := fmt.Sprintf(format, secret)
		require.NotContains(t, output, "SECRET-KEY-BYTES", format)
		require.NotContains(t, output, "BEGIN PRIVATE KEY", format)
	}
}

func TestSecretInStructIsMasked(t *testing.T) {
	holder := secretHolder{TokenKeyPEM: Secret(secretKeyBytes)}

	jsonBytes, err := json.Marshal(holder)
	require.NoError(t, err)
	require.NotContains(t, string(jsonBytes), "SECRET-KEY-BYTES")
	require.Contains(t, string(jsonBytes), secretMask)

	yamlBytes, err := yaml.Marshal(holder)
	require.NoError(t, err)
	require.NotContains(t, string(yamlBytes), "SECRET-KEY-BYTES")
	require.Contains(t, string(yamlBytes), secretMask)

	for _, format := range []string{"%v", "%+v", "%#v"} {
		output := fmt.Sprintf(format, holder)
		require.NotContains(t, output, "SECRET-KEY-BYTES", format)
		require.True(t, strings.Contains(output, secretMask), format)
	}
}

func TestSecretValueIsKeptForTheLoader(t *testing.T) {
	// The loader converts the Secret to PEMData; the stored value is not changed by the masking methods.
	secret := Secret(secretKeyBytes)

	require.Equal(t, secretKeyBytes, string(PEMData(string(secret))))
}
