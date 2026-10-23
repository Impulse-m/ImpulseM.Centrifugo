package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const pushTestKeyText string = "-----BEGIN PRIVATE KEY-----SECRETKEYBYTES-----END PRIVATE KEY-----"

// writePushConfigFile writes a configuration file for a push test and returns its path.
func writePushConfigFile(t *testing.T, name string, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// clearPushEnv unsets every push_notifications environment variable for the test. t.Setenv restores
// the original values when the test ends.
func clearPushEnv(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, pushEnvPrefix) {
			continue
		}
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
}

// A configuration file without push notification keys loads with the subsystem off and no push key
// reported as unknown.
func TestPushConfigAbsentLoadsAsBefore(t *testing.T) {
	path := writePushConfigFile(t, "config.json", "{}")
	conf, meta, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.False(t, conf.PushNotifications.Enabled)
	require.NoError(t, conf.Validate())
	for _, key := range meta.UnknownKeys {
		require.NotContains(t, key, "push_notifications")
	}
}

// With push notifications disabled, a push key that this build does not have is only reported as
// an unknown key, and the configuration loads.
func TestPushConfigDisabledUnknownKeyIsWarning(t *testing.T) {
	path := writePushConfigFile(t, "config.json", `{"push_notifications": {"enabled": false, "queue": {"type": "redis"}}}`)
	conf, meta, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.False(t, conf.PushNotifications.Enabled)
	require.Contains(t, meta.UnknownKeys, "push_notifications.queue")
}

// With push notifications enabled, a push environment variable that this build does not have refuses
// the startup, as the same key in the file does.
func TestPushConfigEnabledUnknownEnvRefused(t *testing.T) {
	clearPushEnv(t)
	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_ENABLED", "true")
	_, _, err := GetConfig(nil, "")
	require.NoError(t, err)

	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE", "/nonexistent/cert.p12")
	_, _, err = GetConfig(nil, "")
	require.ErrorContains(t, err, "CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE is not available in this build")
}

// With push notifications enabled, the variables that Kubernetes injects for a service whose name
// starts with the push prefix are not unknown variables, so they do not refuse the startup.
func TestPushConfigKubernetesServiceEnvAccepted(t *testing.T) {
	clearPushEnv(t)
	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_ENABLED", "true")
	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_SERVICE_HOST", "10.0.0.7")
	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_SERVICE_PORT", "tcp://10.0.0.7:8000")
	_, meta, err := GetConfig(nil, "")
	require.NoError(t, err)
	require.NotContains(t, meta.UnknownEnvs, "CENTRIFUGO_PUSH_NOTIFICATIONS_SERVICE_HOST")
}

// With push notifications enabled, a push key that this build does not have refuses the startup.
func TestPushConfigEnabledUnknownKeyRefused(t *testing.T) {
	path := writePushConfigFile(t, "config.json", `{"push_notifications": {"enabled": true, "enabled_providers": ["fcm"], "fcm": {"credentials_file": "/nonexistent/sa.json"}, "queue": {"type": "redis"}}}`)
	_, _, err := GetConfig(nil, path)
	require.ErrorContains(t, err, "push_notifications.queue is not available in this build")
}

// A certificate key of APNs is an unknown key of this build, so it is refused while push is enabled.
func TestPushConfigCertificateKeyRefused(t *testing.T) {
	path := writePushConfigFile(t, "config.json", `{"push_notifications": {"enabled": true, "enabled_providers": ["apns"], "apns": {"cert_p12_file": "/nonexistent/cert.p12"}}}`)
	_, _, err := GetConfig(nil, path)
	require.ErrorContains(t, err, "push_notifications.apns.cert_p12_file is not available in this build")
}

// Loading a configuration reads no key or credential file: the files named by the configuration
// need not exist when sender is false. The sender true case belongs to the startup tests of D3 and
// D4 (TestSetupRejectsBadKeyAtStartup, spec section 6.2).
func TestPushConfigLoadReadsNoFiles(t *testing.T) {
	path := writePushConfigFile(t, "config.json", `{"push_notifications": {"enabled": true, "sender": false, "enabled_providers": ["apns", "fcm"], "apns": {"bundle_id": "com.example.app", "token_key_id": "KEYID12345", "token_team_id": "TEAMID1234", "token_key_file": "/nonexistent/AuthKey.p8"}, "fcm": {"credentials_file": "/nonexistent/sa.json"}}}`)
	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.False(t, conf.PushNotifications.Sender)
	require.NoError(t, conf.Validate())
}

// A key held inline is set from the configuration file and from the environment variable, and the
// loaded value reaches no dump: the JSON, YAML and TOML dumps, the String form and the formatted
// forms of the configuration carry the mask and never the key bytes. The TOML dump is the one of
// the defaultconfig command, which marshals the loaded configuration; with -b the base file is the
// configuration file read here, so the base file path is covered by the file case.
func TestPushTokenKeyPEMSetAndMaskedInDumps(t *testing.T) {
	sources := map[string]func(t *testing.T) (Config, error){
		"config file": func(t *testing.T) (Config, error) {
			clearPushEnv(t)
			path := writePushConfigFile(t, "base.json", `{"push_notifications": {"enabled": false, "apns": {"token_key_pem": "`+pushTestKeyText+`"}}}`)
			conf, _, err := GetConfig(nil, path)
			return conf, err
		},
		"environment variable": func(t *testing.T) (Config, error) {
			clearPushEnv(t)
			t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_TOKEN_KEY_PEM", pushTestKeyText)
			conf, _, err := GetConfig(nil, "")
			return conf, err
		},
	}
	for name, load := range sources {
		t.Run(name, func(t *testing.T) {
			conf, err := load(t)
			require.NoError(t, err)
			require.Equal(t, pushTestKeyText, conf.PushNotifications.APNs.TokenKeyPEM.Value())
			require.Equal(t, "[REDACTED]", conf.PushNotifications.APNs.TokenKeyPEM.String())

			jsonBytes, err := json.MarshalIndent(conf, "", "  ")
			require.NoError(t, err)
			require.NotContains(t, string(jsonBytes), "SECRETKEYBYTES")
			require.Contains(t, string(jsonBytes), "[REDACTED]")

			yamlBytes, err := yaml.Marshal(conf)
			require.NoError(t, err)
			require.NotContains(t, string(yamlBytes), "SECRETKEYBYTES")
			require.Contains(t, string(yamlBytes), "[REDACTED]")

			tomlBytes, err := toml.Marshal(conf)
			require.NoError(t, err)
			require.NotContains(t, string(tomlBytes), "SECRETKEYBYTES")
			require.Contains(t, string(tomlBytes), "[REDACTED]")

			formatted := fmt.Sprintf("%v|%+v|%#v", conf.PushNotifications, conf.PushNotifications, conf.PushNotifications)
			require.NotContains(t, formatted, "SECRETKEYBYTES")
		})
	}
}

// The default configuration, the one the defaultconfig command prints as TOML, carries no key
// material: its token key is empty.
func TestPushDefaultConfigTOMLHasNoKeyMaterial(t *testing.T) {
	clearPushEnv(t)
	conf, _, err := GetConfig(nil, "")
	require.NoError(t, err)
	require.Empty(t, conf.PushNotifications.APNs.TokenKeyPEM.Value())

	tomlBytes, err := toml.Marshal(conf)
	require.NoError(t, err)
	require.NotContains(t, string(tomlBytes), "PRIVATE KEY")
	require.NotContains(t, string(tomlBytes), "SECRETKEYBYTES")
}
