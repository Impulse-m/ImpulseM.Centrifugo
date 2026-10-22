package config

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const pushTestKeyText string = "-----BEGIN PRIVATE KEY-----SECRETKEYBYTES-----END PRIVATE KEY-----"

// A configuration file without push notification keys loads with the subsystem off and no push key
// reported as unknown.
func TestPushConfigAbsentLoadsAsBefore(t *testing.T) {
	path := writeRegistryFile(t, "config.json", "{}")
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
	path := writeRegistryFile(t, "config.json", `{"push_notifications": {"enabled": false, "queue": {"type": "redis"}}}`)
	conf, meta, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.False(t, conf.PushNotifications.Enabled)
	require.Contains(t, meta.UnknownKeys, "push_notifications.queue")
}

// With push notifications enabled, a push key that this build does not have refuses the startup.
func TestPushConfigEnabledUnknownKeyRefused(t *testing.T) {
	path := writeRegistryFile(t, "config.json", `{"push_notifications": {"enabled": true, "enabled_providers": ["fcm"], "fcm": {"credentials_file": "/nonexistent/sa.json"}, "queue": {"type": "redis"}}}`)
	_, _, err := GetConfig(nil, path)
	require.ErrorContains(t, err, "push_notifications.queue is not available in this build")
}

// A certificate key of APNs is an unknown key of this build, so it is refused while push is enabled.
func TestPushConfigCertificateKeyRefused(t *testing.T) {
	path := writeRegistryFile(t, "config.json", `{"push_notifications": {"enabled": true, "enabled_providers": ["apns"], "apns": {"cert_p12_file": "/nonexistent/cert.p12"}}}`)
	_, _, err := GetConfig(nil, path)
	require.ErrorContains(t, err, "push_notifications.apns.cert_p12_file is not available in this build")
}

// Loading a configuration reads no key or credential file: the files named by the configuration
// need not exist when sender is false, and the load does not open them when sender is true.
func TestPushConfigLoadReadsNoFiles(t *testing.T) {
	path := writeRegistryFile(t, "config.json", `{"push_notifications": {"enabled": true, "sender": false, "enabled_providers": ["apns", "fcm"], "apns": {"bundle_id": "com.example.app", "token_key_id": "KEYID12345", "token_team_id": "TEAMID1234", "token_key_file": "/nonexistent/AuthKey.p8"}, "fcm": {"credentials_file": "/nonexistent/sa.json"}}}`)
	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.False(t, conf.PushNotifications.Sender)
	require.NoError(t, conf.Validate())
}

// A key held inline is masked in every output path of the configuration: the JSON, TOML and YAML
// dumps of the default configuration command, and formatted output.
func TestPushTokenKeyPEMMaskedInDumps(t *testing.T) {
	path := writeRegistryFile(t, "config.json", `{"push_notifications": {"enabled": false, "apns": {"token_key_pem": "`+pushTestKeyText+`"}}}`)
	conf, _, err := GetConfig(nil, path)
	require.NoError(t, err)
	require.Equal(t, configtypes.Secret(pushTestKeyText), conf.PushNotifications.APNs.TokenKeyPEM)

	jsonBytes, err := json.MarshalIndent(conf, "", "  ")
	require.NoError(t, err)
	require.NotContains(t, string(jsonBytes), "SECRETKEYBYTES")
	require.Contains(t, string(jsonBytes), "[REDACTED]")

	tomlBytes, err := toml.Marshal(conf)
	require.NoError(t, err)
	require.NotContains(t, string(tomlBytes), "SECRETKEYBYTES")

	yamlBytes, err := yaml.Marshal(conf)
	require.NoError(t, err)
	require.NotContains(t, string(yamlBytes), "SECRETKEYBYTES")

	formatted := fmt.Sprintf("%v|%+v|%#v", conf.PushNotifications, conf.PushNotifications, conf.PushNotifications)
	require.NotContains(t, formatted, "SECRETKEYBYTES")
}
