package provider

import (
	"crypto/elliptic"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

func TestLoadAPNSKeyPEMPathRefusesLargeFile(t *testing.T) {
	// The inline value is a path to a file over the bound: the stat of the PEM load refuses it before the read.
	path := writeTestFile(t, []byte(strings.Repeat("A", maxKeyFileBytes+1)))

	_, err := LoadAPNSKey("", configtypes.NewSecret(path))

	require.ErrorContains(t, err, apnsKeyPEMKey)
	require.ErrorContains(t, err, "larger than")
	require.NotContains(t, err.Error(), "AAAA")
}

func TestLoadAPNSKeyPEMRefusesLargeInlineValue(t *testing.T) {
	_, err := LoadAPNSKey("", configtypes.NewSecret(strings.Repeat("A", maxKeyFileBytes+1)))

	require.ErrorContains(t, err, apnsKeyPEMKey)
	require.NotContains(t, err.Error(), "AAAA")
}

func TestLoadAPNSKeyPEMStillLoadsInline(t *testing.T) {
	keyPEM := newTestPKCS8PEM(t, elliptic.P256())

	key, err := LoadAPNSKey("", configtypes.NewSecret(string(keyPEM)))

	require.NoError(t, err)
	require.Equal(t, elliptic.P256(), key.Curve)
}
