package config

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// pushTestConfig returns the default configuration with push notifications enabled for APNs and FCM,
// with every required setting present. The files it names do not exist: validation must not read them.
// The Web Push settings hold a generated VAPID key pair inline, so they are valid while webpush is not
// enabled.
func pushTestConfig() Config {
	conf := DefaultConfig()
	conf.PushNotifications.Enabled = true
	conf.PushNotifications.EnabledProviders = []string{"apns", "fcm"}
	conf.PushNotifications.APNs.BundleID = "com.example.app"
	conf.PushNotifications.APNs.TokenKeyID = "KEYID12345"
	conf.PushNotifications.APNs.TokenTeamID = "TEAMID1234"
	conf.PushNotifications.APNs.TokenKeyFile = "/nonexistent/AuthKey.p8"
	conf.PushNotifications.FCM.CredentialsFile = "/nonexistent/service-account.json"
	publicKey, privateKey := newWebPushTestKeys()
	conf.PushNotifications.WebPush.VAPIDPublicKey = publicKey
	conf.PushNotifications.WebPush.VAPIDPrivateKey = configtypes.NewSecret(privateKey)
	conf.PushNotifications.WebPush.Subject = "mailto:push@example.com"
	return conf
}

// newWebPushTestKeys generates a VAPID key pair and returns the public key and the private scalar in
// the unpadded base64url form of the configuration. It panics when the generation fails.
func newWebPushTestKeys() (string, string) {
	private, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes()),
		base64.RawURLEncoding.EncodeToString(private.Bytes())
}

func TestValidatePushNotificationsValid(t *testing.T) {
	conf := pushTestConfig()
	require.NoError(t, conf.Validate())
}

// With push notifications disabled, nothing beyond decoding is checked: the values that would be
// refused while enabled are accepted.
func TestValidatePushNotificationsDisabledChecksNothing(t *testing.T) {
	conf := DefaultConfig()
	conf.PushNotifications.Enabled = false
	conf.PushNotifications.EnabledProviders = []string{"hms", "webpush"}
	conf.PushNotifications.APNs.AuthType = "cert"
	conf.PushNotifications.APNs.Endpoint = "staging"
	conf.PushNotifications.SyncTimeout = configtypes.Duration(0)
	conf.PushNotifications.MaxRecipientItems = 5000
	require.NoError(t, validatePushNotifications(conf))
	require.NoError(t, conf.Validate())
}

// The default configuration with push notifications disabled validates as before.
func TestValidatePushNotificationsDefaultConfigUnchanged(t *testing.T) {
	conf := DefaultConfig()
	require.False(t, conf.PushNotifications.Enabled)
	require.False(t, conf.PushNotifications.Sender)
	require.NoError(t, conf.Validate())
}

// The defaults are the struct tags of the push notification configuration.
func TestPushNotificationsDefaults(t *testing.T) {
	conf := DefaultConfig()
	push := conf.PushNotifications
	require.Equal(t, 10, int(push.SyncTimeout.ToDuration().Seconds()))
	require.Equal(t, 1000, push.MaxRecipientItems)
	require.Equal(t, 1048576, push.MaxRequestBytes)
	require.Equal(t, 64, push.MaxConcurrentRequests)
	require.Equal(t, 10, int(push.Providers.RequestTimeout.ToDuration().Seconds()))
	require.Equal(t, "development", push.APNs.Endpoint)
	require.Equal(t, "token", push.APNs.AuthType)
	require.Equal(t, 100, push.APNs.TokensBatchSize)
	require.Equal(t, 500, push.APNs.MaxInflight)
	require.Equal(t, 500, push.FCM.TokensBatchSize)
	require.Equal(t, 200, push.FCM.MaxInflight)
	require.Equal(t, 100, push.WebPush.TokensBatchSize)
	require.Equal(t, 500, push.WebPush.MaxInflight)
}

func TestValidatePushNotificationsRules(t *testing.T) {
	otherPublicKey, _ := newWebPushTestKeys()
	tests := []struct {
		name    string
		mutate  func(conf *Config)
		wantErr string
	}{
		{
			name: "enabled with no provider",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = nil
			},
			wantErr: "must name at least one provider",
		},
		{
			name: "hms provider is refused",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"hms"}
			},
			wantErr: `entry "hms" is not available in this build`,
		},
		{
			name: "webpush provider is accepted",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"fcm", "webpush"}
			},
		},
		{
			name: "webpush with the private key as a file only",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPrivateKey = configtypes.NewSecret("")
				conf.PushNotifications.WebPush.VAPIDPrivateKeyFile = "/nonexistent/vapid.key"
			},
		},
		{
			name: "webpush https subject",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.Subject = "https://example.com"
			},
		},
		{
			name: "webpush without a public key",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPublicKey = ""
			},
			wantErr: "push_notifications.webpush.vapid_public_key is required when webpush is enabled",
		},
		{
			name: "webpush without a subject",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.Subject = ""
			},
			wantErr: "push_notifications.webpush.subject is required when webpush is enabled",
		},
		{
			name: "webpush without a private key",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPrivateKey = configtypes.NewSecret("")
			},
			wantErr: "push_notifications.webpush.vapid_private_key or push_notifications.webpush.vapid_private_key_file is required when webpush is enabled",
		},
		{
			name: "webpush subject without a scheme",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.Subject = "push@example.com"
			},
			wantErr: "push_notifications.webpush.subject must start with mailto: or https://",
		},
		{
			name: "webpush subject over plain http",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.Subject = "http://example.com"
			},
			wantErr: "push_notifications.webpush.subject must start with mailto: or https://",
		},
		{
			name: "webpush public key is not a point",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPublicKey = "AAAA"
			},
			wantErr: "push_notifications.webpush.vapid_public_key: public key is not a P-256 point",
		},
		{
			name: "webpush private key is not base64url",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPrivateKey = configtypes.NewSecret("!!!")
			},
			wantErr: "push_notifications.webpush.vapid_private_key: private key is not base64url",
		},
		{
			name: "webpush public key of another pair",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPublicKey = otherPublicKey
			},
			wantErr: "push_notifications.webpush.vapid_private_key: private key does not match the public key",
		},
		{
			name: "webpush key file and inline key are both set",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"webpush"}
				conf.PushNotifications.WebPush.VAPIDPrivateKeyFile = "/nonexistent/vapid.key"
			},
			wantErr: "push_notifications.webpush.vapid_private_key and push_notifications.webpush.vapid_private_key_file are mutually exclusive",
		},
		{
			name: "webpush tokens batch size above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.TokensBatchSize = 1001
			},
			wantErr: "push_notifications.webpush.tokens_batch_size must be at most 1000, got 1001",
		},
		{
			name: "webpush max inflight of zero",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.MaxInflight = 0
			},
			wantErr: "push_notifications.webpush.max_inflight must be greater than zero",
		},
		{
			name: "webpush max inflight above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.MaxInflight = 10001
			},
			wantErr: "push_notifications.webpush.max_inflight must be at most 10000, got 10001",
		},
		{
			name: "webpush allowed origin pattern does not compile",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.AllowedEndpointOrigins = []string{"https://[abc"}
			},
			wantErr: "push_notifications.webpush.allowed_endpoint_origins: malformed origin pattern",
		},
		{
			name: "webpush extra origin pattern does not compile",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.ExtraAllowedEndpointOrigins = []string{"https://[abc"}
			},
			wantErr: "push_notifications.webpush.extra_allowed_endpoint_origins: malformed origin pattern",
		},
		{
			name: "webpush origin patterns compile",
			mutate: func(conf *Config) {
				conf.PushNotifications.WebPush.AllowedEndpointOrigins = []string{"https://*.googleapis.com", "*"}
				conf.PushNotifications.WebPush.ExtraAllowedEndpointOrigins = []string{"https://push.example.org"}
			},
		},
		{
			name: "unknown provider",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"apn"}
			},
			wantErr: `unknown push_notifications.enabled_providers entry: "apn"`,
		},
		{
			name: "duplicate provider",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"fcm", "fcm"}
			},
			wantErr: `duplicate push_notifications.enabled_providers entry: "fcm"`,
		},
		{
			name: "unknown apns endpoint",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.Endpoint = "staging"
			},
			wantErr: `unknown push_notifications.apns.endpoint: "staging"`,
		},
		{
			name: "certificate auth is refused",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.AuthType = "cert"
			},
			wantErr: `push_notifications.apns.auth_type "cert" is not available in this build`,
		},
		{
			name: "key file and key pem are both set",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.TokenKeyPEM = configtypes.NewSecret("-----BEGIN PRIVATE KEY-----")
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "apns without a key",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.TokenKeyFile = ""
			},
			wantErr: "token_key_file or push_notifications.apns.token_key_pem is required",
		},
		{
			name: "apns without a bundle id",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.BundleID = ""
			},
			wantErr: "push_notifications.apns.bundle_id is required when apns is enabled",
		},
		{
			name: "apns without a key id",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.TokenKeyID = ""
			},
			wantErr: "push_notifications.apns.token_key_id is required when apns is enabled",
		},
		{
			name: "apns without a team id",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.TokenTeamID = ""
			},
			wantErr: "push_notifications.apns.token_team_id is required when apns is enabled",
		},
		{
			name: "fcm without credentials",
			mutate: func(conf *Config) {
				conf.PushNotifications.FCM.CredentialsFile = ""
			},
			wantErr: "push_notifications.fcm.credentials_file is required when fcm is enabled",
		},
		{
			name: "apns settings are not required when apns is not enabled",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"fcm"}
				conf.PushNotifications.APNs = configtypes.PushAPNs{
					Endpoint:        "development",
					AuthType:        "token",
					TokensBatchSize: 100,
					MaxInflight:     500,
				}
			},
		},
		{
			name: "fcm settings are not required when fcm is not enabled",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"apns"}
				conf.PushNotifications.FCM = configtypes.PushFCM{
					TokensBatchSize: 500,
					MaxInflight:     200,
				}
			},
		},
		{
			name: "non-positive apns max inflight",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.MaxInflight = 0
			},
			wantErr: "push_notifications.apns.max_inflight must be greater than zero",
		},
		{
			name: "non-positive fcm tokens batch size",
			mutate: func(conf *Config) {
				conf.PushNotifications.FCM.TokensBatchSize = 0
			},
			wantErr: "push_notifications.fcm.tokens_batch_size must be greater than zero",
		},
		{
			name: "zero sync timeout",
			mutate: func(conf *Config) {
				conf.PushNotifications.SyncTimeout = configtypes.Duration(0)
			},
			wantErr: "push_notifications.sync_timeout must be greater than zero",
		},
		{
			name: "zero provider request timeout",
			mutate: func(conf *Config) {
				conf.PushNotifications.Providers.RequestTimeout = configtypes.Duration(0)
			},
			wantErr: "push_notifications.providers.request_timeout must be greater than zero",
		},
		{
			name: "zero concurrent requests",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxConcurrentRequests = 0
			},
			wantErr: "push_notifications.max_concurrent_requests must be greater than zero",
		},
		{
			name: "zero request bytes",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRequestBytes = 0
			},
			wantErr: "push_notifications.max_request_bytes must be greater than zero",
		},
		{
			name: "recipient limit above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRecipientItems = 1001
			},
			wantErr: "push_notifications.max_recipient_items must be between 1 and 1000, got 1001",
		},
		{
			name: "recipient limit of zero",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRecipientItems = 0
			},
			wantErr: "push_notifications.max_recipient_items must be between 1 and 1000, got 0",
		},
		{
			name: "recipient limit at the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRecipientItems = 1000
			},
		},
		{
			name: "request bytes above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRequestBytes = maxPushRequestBytes + 1
			},
			wantErr: "push_notifications.max_request_bytes must be at most 16777216, got 16777217",
		},
		{
			name: "request bytes at the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxRequestBytes = maxPushRequestBytes
			},
		},
		{
			name: "concurrent requests above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.MaxConcurrentRequests = maxPushConcurrentRequests + 1
			},
			wantErr: "push_notifications.max_concurrent_requests must be at most 256, got 257",
		},
		{
			name: "apns max inflight above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.MaxInflight = 100000
			},
			wantErr: "push_notifications.apns.max_inflight must be at most 10000, got 100000",
		},
		{
			name: "apns tokens batch size above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.APNs.TokensBatchSize = 100000
			},
			wantErr: "push_notifications.apns.tokens_batch_size must be at most 1000, got 100000",
		},
		{
			name: "fcm tokens batch size above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.FCM.TokensBatchSize = 1001
			},
			wantErr: "push_notifications.fcm.tokens_batch_size must be at most 1000, got 1001",
		},
		{
			name: "fcm max inflight above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.FCM.MaxInflight = 10001
			},
			wantErr: "push_notifications.fcm.max_inflight must be at most 10000, got 10001",
		},
		{
			name: "sync timeout above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.SyncTimeout = configtypes.Duration(24 * time.Hour)
			},
			wantErr: "push_notifications.sync_timeout must be at most 1m0s, got 24h0m0s",
		},
		{
			name: "provider request timeout above the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.Providers.RequestTimeout = configtypes.Duration(2 * time.Minute)
			},
			wantErr: "push_notifications.providers.request_timeout must be at most 1m0s, got 2m0s",
		},
		{
			name: "timeouts at the maximum",
			mutate: func(conf *Config) {
				conf.PushNotifications.SyncTimeout = configtypes.Duration(time.Minute)
				conf.PushNotifications.Providers.RequestTimeout = configtypes.Duration(time.Minute)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := pushTestConfig()
			test.mutate(&conf)
			err := validatePushNotifications(conf)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

// The settings are validated with the rest of the configuration, through Config.Validate.
func TestConfigValidateRefusesPushSettings(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.EnabledProviders = []string{"hms"}
	require.ErrorContains(t, conf.Validate(), "is not available in this build")
}

// While webpush is not enabled, its key forms, its ranges and its origin patterns are still checked,
// but its public key, subject and private key are not required and not checked.
func TestValidatePushWebPushWhenNotEnabled(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.WebPush.VAPIDPublicKey = "AAAA"
	conf.PushNotifications.WebPush.Subject = "push@example.com"
	require.NoError(t, validatePushNotifications(conf))

	conf.PushNotifications.WebPush.VAPIDPrivateKeyFile = "/nonexistent/vapid.key"
	require.ErrorContains(t, validatePushNotifications(conf), "mutually exclusive")

	conf = pushTestConfig()
	conf.PushNotifications.WebPush.AllowedEndpointOrigins = []string{"https://[abc"}
	require.ErrorContains(t, validatePushNotifications(conf), "malformed origin pattern")

	conf = pushTestConfig()
	conf.PushNotifications.WebPush.TokensBatchSize = 0
	require.ErrorContains(t, validatePushNotifications(conf), "push_notifications.webpush.tokens_batch_size must be greater than zero")
}

// The private key given as a file is not read by validation: the file does not exist and validation
// still accepts it, with webpush enabled.
func TestValidatePushWebPushFileKeyNotRead(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.EnabledProviders = []string{"webpush"}
	conf.PushNotifications.WebPush.VAPIDPrivateKey = configtypes.NewSecret("")
	conf.PushNotifications.WebPush.VAPIDPrivateKeyFile = "/nonexistent/vapid.key"
	require.NoError(t, conf.Validate())
}

// Validation accepts the files named by pushTestConfig as missing, with sender false and with sender
// true. It does not prove that no file is opened: the proof that sender false loads no file is
// TestSetupRejectsBadKeyAtStartup (D3 and D4, spec section 6.2).
func TestPushValidationAcceptsMissingFiles(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.Sender = false
	require.NoError(t, conf.Validate())
	conf.PushNotifications.Sender = true
	require.NoError(t, conf.Validate())
}

func TestCheckPushUnknownKeys(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.Enabled = false
	require.NoError(t, checkPushUnknownKeys(conf, []string{"push_notifications.queue"}, nil))

	conf.PushNotifications.Enabled = true
	require.NoError(t, checkPushUnknownKeys(conf, []string{"some_other.key"}, nil))
	require.NoError(t, checkPushUnknownKeys(conf, nil, nil))

	err := checkPushUnknownKeys(conf, []string{"push_notifications.apns.cert_p12_file", "push_notifications.queue"}, nil)
	require.EqualError(t, err, "push_notifications.apns.cert_p12_file is not available in this build")
}

// With push notifications enabled, an unknown push environment variable refuses the startup. Only the
// push variables of the unknown environment list are refused; other variables keep their warning.
func TestCheckPushUnknownEnvRefused(t *testing.T) {
	conf := pushTestConfig()
	conf.PushNotifications.Enabled = true

	err := checkPushUnknownKeys(conf, nil, []string{"CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE"})
	require.EqualError(t, err, "CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE is not available in this build")

	require.NoError(t, checkPushUnknownKeys(conf, nil, []string{"CENTRIFUGO_UNRELATED_VARIABLE"}))
	conf.PushNotifications.Enabled = false
	require.NoError(t, checkPushUnknownKeys(conf, nil, []string{"CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE"}))
}
