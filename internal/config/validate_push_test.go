package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config/envconfig"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// pushTestConfig returns the default configuration with push notifications enabled for APNs and FCM,
// with every required setting present. The files it names do not exist: validation must not read them.
func pushTestConfig() Config {
	conf := DefaultConfig()
	conf.PushNotifications.Enabled = true
	conf.PushNotifications.EnabledProviders = []string{"apns", "fcm"}
	conf.PushNotifications.APNs.BundleID = "com.example.app"
	conf.PushNotifications.APNs.TokenKeyID = "KEYID12345"
	conf.PushNotifications.APNs.TokenTeamID = "TEAMID1234"
	conf.PushNotifications.APNs.TokenKeyFile = "/nonexistent/AuthKey.p8"
	conf.PushNotifications.FCM.CredentialsFile = "/nonexistent/service-account.json"
	return conf
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
}

func TestValidatePushNotificationsRules(t *testing.T) {
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
			name: "webpush provider is refused",
			mutate: func(conf *Config) {
				conf.PushNotifications.EnabledProviders = []string{"fcm", "webpush"}
			},
			wantErr: `entry "webpush" is not available in this build`,
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
				conf.PushNotifications.APNs.TokenKeyPEM = configtypes.Secret("-----BEGIN PRIVATE KEY-----")
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
	conf.PushNotifications.EnabledProviders = []string{"webpush"}
	require.ErrorContains(t, conf.Validate(), "is not available in this build")
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

// With push notifications enabled, a push environment variable that matches no known key refuses the
// startup, and a known one does not.
func TestCheckPushUnknownEnvRefused(t *testing.T) {
	clearPushEnv(t)
	conf := pushTestConfig()
	conf.PushNotifications.Enabled = true

	t.Setenv("CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE", "/nonexistent/cert.p12")
	err := checkPushUnknownKeys(conf, nil, map[string]envconfig.VarInfo{})
	require.EqualError(t, err, "CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE is not available in this build")

	known := map[string]envconfig.VarInfo{"CENTRIFUGO_PUSH_NOTIFICATIONS_APNS_CERT_P12_FILE": {}}
	require.NoError(t, checkPushUnknownKeys(conf, nil, known))
}
