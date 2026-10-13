package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigEnvVarsEd25519PublicKeys(t *testing.T) {
	_ = os.Setenv("CENTRIFUGO_CLIENT_TOKEN_ED25519_PUBLIC_KEYS", `[{"kid":"k1","public_key":"key1"},{"kid":"k2","public_key":"key2"}]`)
	defer func() {
		_ = os.Unsetenv("CENTRIFUGO_CLIENT_TOKEN_ED25519_PUBLIC_KEYS")
	}()
	conf, meta := getConfig(t, "testdata/config.json")
	require.Len(t, meta.UnknownEnvs, 0)
	require.Len(t, conf.Client.Token.Ed25519PublicKeys, 2)
	require.Equal(t, "k1", conf.Client.Token.Ed25519PublicKeys[0].KID)
	require.Equal(t, "key1", conf.Client.Token.Ed25519PublicKeys[0].PublicKey)
	require.Equal(t, "k2", conf.Client.Token.Ed25519PublicKeys[1].KID)
	require.Equal(t, "key2", conf.Client.Token.Ed25519PublicKeys[1].PublicKey)
}
