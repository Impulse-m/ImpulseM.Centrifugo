package provider

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// newTestPKCS8PEM returns a PKCS#8 PEM private key on curve, generated in the test.
func newTestPKCS8PEM(t *testing.T, curve elliptic.Curve) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func writeTestFile(t *testing.T, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "key.p8")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	return path
}

func TestLoadAPNSKeyFromFile(t *testing.T) {
	path := writeTestFile(t, newTestPKCS8PEM(t, elliptic.P256()))

	key, err := LoadAPNSKey(path, "")

	require.NoError(t, err)
	require.Equal(t, elliptic.P256(), key.Curve)
}

func TestLoadAPNSKeyFromPEM(t *testing.T) {
	keyPEM := newTestPKCS8PEM(t, elliptic.P256())

	key, err := LoadAPNSKey("", configtypes.PEMData(string(keyPEM)))

	require.NoError(t, err)
	require.Equal(t, elliptic.P256(), key.Curve)
}

func TestCredentialErrorsNameKeyNotContent(t *testing.T) {
	const secretLine = "SECRET-KEY-BYTES-DO-NOT-LOG"

	t.Run("file not PEM", func(t *testing.T) {
		path := writeTestFile(t, []byte(secretLine))

		_, err := LoadAPNSKey(path, "")

		require.ErrorContains(t, err, apnsKeyFileKey)
		require.ErrorContains(t, err, path)
		require.NotContains(t, err.Error(), secretLine)
	})

	t.Run("inline PEM not a key", func(t *testing.T) {
		keyPEM := "-----BEGIN PRIVATE KEY-----\n" + secretLine + "\n-----END PRIVATE KEY-----\n"

		_, err := LoadAPNSKey("", configtypes.PEMData(keyPEM))

		require.Error(t, err)
		require.NotContains(t, err.Error(), secretLine)
	})

	t.Run("wrong curve", func(t *testing.T) {
		path := writeTestFile(t, newTestPKCS8PEM(t, elliptic.P384()))

		_, err := LoadAPNSKey(path, "")

		require.ErrorContains(t, err, apnsKeyFileKey)
		require.ErrorContains(t, err, "P-256")
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := LoadAPNSKey(filepath.Join(t.TempDir(), "absent.p8"), "")

		require.ErrorContains(t, err, apnsKeyFileKey)
	})
}

func TestLoadFCMServiceAccount(t *testing.T) {
	keyPEM := newTestPKCS8PEM(t, elliptic.P256())
	account, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key":    string(keyPEM),
		"client_email":   "sender@test-project.iam.gserviceaccount.com",
		"token_uri":      "https://oauth2.invalid/token",
		"private_key_id": "kid",
	})
	require.NoError(t, err)

	loaded, err := LoadFCMServiceAccount(account)

	require.NoError(t, err)
	require.Equal(t, "test-project", loaded.ProjectID)
	require.NotNil(t, loaded.JWT)
}

func TestLoadFCMServiceAccountRefusesMissingProjectID(t *testing.T) {
	_, err := LoadFCMServiceAccount([]byte(`{"type":"service_account"}`))

	require.ErrorContains(t, err, fcmCredsKey)
}
