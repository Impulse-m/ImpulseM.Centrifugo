package configtypes

// PushNotifications configures the push notification subsystem. While Enabled is false nothing is
// loaded or created, and no value beyond decoding is checked. All keys are read at startup only.
type PushNotifications struct {
	// Enabled turns the push notification subsystem on. Off by default.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the push notification subsystem. Off by default: nothing is loaded or created when it is off. Requires restart."`
	// Sender makes this node a sender. Only sender nodes load provider credentials and send.
	Sender bool `mapstructure:"sender" json:"sender" envconfig:"sender" yaml:"sender" toml:"sender" doc:"Makes this node a sender: only sender nodes load provider credentials and send. Other nodes answer send calls with an unavailable error. Default <<false>>. Requires restart."`
	// EnabledProviders lists the push providers to enable: apns, fcm and webpush.
	EnabledProviders []string `mapstructure:"enabled_providers" json:"enabled_providers" envconfig:"enabled_providers" yaml:"enabled_providers" toml:"enabled_providers" doc:"Push providers to enable: <<apns>>, <<fcm>> and <<webpush>>. At least one is required when push notifications are enabled. Requires restart."`
	// DryRun answers valid sends as dry run items without calling any provider.
	DryRun bool `mapstructure:"dry_run" json:"dry_run" envconfig:"dry_run" yaml:"dry_run" toml:"dry_run" doc:"Answers valid sends as dry run items without any network call. Default <<false>>. Requires restart."`
	// SyncTimeout bounds the inline run of one send: no provider call starts after it.
	SyncTimeout Duration `mapstructure:"sync_timeout" default:"10s" json:"sync_timeout" envconfig:"sync_timeout" yaml:"sync_timeout" toml:"sync_timeout" doc:"How long one send waits for provider calls to start before the rest of its items are reported as not_sent, at most <<1m>>. Default <<10s>>. Requires restart."`
	// MaxRecipientItems is the maximum number of recipients in one send.
	MaxRecipientItems int `mapstructure:"max_recipient_items" default:"1000" json:"max_recipient_items" envconfig:"max_recipient_items" yaml:"max_recipient_items" toml:"max_recipient_items" doc:"Maximum number of recipients in one send, from <<1>> to <<1000>>. Default <<1000>>. Requires restart."`
	// MaxRequestBytes is the maximum size of one send request in bytes.
	MaxRequestBytes int `mapstructure:"max_request_bytes" default:"1048576" json:"max_request_bytes" envconfig:"max_request_bytes" yaml:"max_request_bytes" toml:"max_request_bytes" doc:"Maximum size of one send request in bytes, at most <<16777216>>. Default <<1048576>>. Requires restart."`
	// MaxConcurrentRequests bounds the send calls that run at the same time on the node.
	MaxConcurrentRequests int `mapstructure:"max_concurrent_requests" default:"64" json:"max_concurrent_requests" envconfig:"max_concurrent_requests" yaml:"max_concurrent_requests" toml:"max_concurrent_requests" doc:"Number of send calls that run at the same time on the node. A call over this limit is refused, since calls hold their request data in memory. At most <<256>>. Default <<64>>. Requires restart."`
	// Providers configures the provider client behavior shared by all providers.
	Providers PushProviders `mapstructure:"providers" json:"providers" envconfig:"providers" yaml:"providers" toml:"providers" doc:"Provider client behavior shared by all providers."`
	// APNs configures the APNs provider, used when apns is enabled.
	APNs PushAPNs `mapstructure:"apns" json:"apns" envconfig:"apns" yaml:"apns" toml:"apns" doc:"Configuration of the APNs provider, used when <<apns>> is enabled."`
	// FCM configures the FCM provider, used when fcm is enabled.
	FCM PushFCM `mapstructure:"fcm" json:"fcm" envconfig:"fcm" yaml:"fcm" toml:"fcm" doc:"Configuration of the FCM provider, used when <<fcm>> is enabled."`
	// WebPush configures the Web Push provider, used when webpush is enabled.
	WebPush PushWebPush `mapstructure:"webpush" json:"webpush" envconfig:"webpush" yaml:"webpush" toml:"webpush" doc:"Configuration of the Web Push provider, used when <<webpush>> is enabled."`
}

// PushProviders configures the provider client behavior shared by all providers.
type PushProviders struct {
	// RequestTimeout is the deadline of one provider call and of one FCM token wait.
	RequestTimeout Duration `mapstructure:"request_timeout" default:"10s" json:"request_timeout" envconfig:"request_timeout" yaml:"request_timeout" toml:"request_timeout" doc:"Deadline of one provider call and of one wait for an FCM access token, at most <<1m>>. Default <<10s>>. Requires restart."`
}

// PushAPNs configures the APNs provider.
type PushAPNs struct {
	// Endpoint selects the APNs host of the default credential: production or development.
	Endpoint string `mapstructure:"endpoint" default:"development" json:"endpoint" envconfig:"endpoint" yaml:"endpoint" toml:"endpoint" expose:"full" doc:"APNs host of the default credential: <<production>> or <<development>>. Default <<development>>. Requires restart."`
	// BundleID is the application bundle identifier, used as the default topic.
	BundleID string `mapstructure:"bundle_id" json:"bundle_id" envconfig:"bundle_id" yaml:"bundle_id" toml:"bundle_id" expose:"full" doc:"Application bundle identifier, used as the default topic. Required when APNs is enabled. Requires restart."`
	// AuthType is the provider authentication type. Only token is supported.
	AuthType string `mapstructure:"auth_type" default:"token" json:"auth_type" envconfig:"auth_type" yaml:"auth_type" toml:"auth_type" doc:"Provider authentication type. Only <<token>> is supported. Default <<token>>. Requires restart."`
	// TokenKeyFile is a path to the token signing key (.p8) of the default credential.
	TokenKeyFile string `mapstructure:"token_key_file" json:"token_key_file" envconfig:"token_key_file" yaml:"token_key_file" toml:"token_key_file" expose:"full" doc:"Path to the token signing key (.p8, PKCS#8 PEM) of the default credential. Exactly one of this and <<token_key_pem>> is set when APNs is enabled. Requires restart."`
	// TokenKeyPEM is the token signing key (.p8, PKCS#8 PEM) of the default credential, held inline.
	TokenKeyPEM Secret `mapstructure:"token_key_pem" json:"token_key_pem" envconfig:"token_key_pem" yaml:"token_key_pem" toml:"token_key_pem" doc:"Token signing key (.p8, PKCS#8 PEM) of the default credential, held inline. Masked in JSON, YAML and formatted output. Exactly one of this and <<token_key_file>> is set when APNs is enabled. Requires restart."`
	// TokenKeyID is the identifier of the token signing key.
	TokenKeyID string `mapstructure:"token_key_id" json:"token_key_id" envconfig:"token_key_id" yaml:"token_key_id" toml:"token_key_id" expose:"full" doc:"Identifier of the token signing key. Required when APNs is enabled. Requires restart."`
	// TokenTeamID is the team identifier that owns the token signing key.
	TokenTeamID string `mapstructure:"token_team_id" json:"token_team_id" envconfig:"token_team_id" yaml:"token_team_id" toml:"token_team_id" expose:"full" doc:"Team identifier that owns the token signing key. Required when APNs is enabled. Requires restart."`
	// TokensBatchSize is the number of targets of one call that are sent in parallel per provider group.
	TokensBatchSize int `mapstructure:"tokens_batch_size" default:"100" json:"tokens_batch_size" envconfig:"tokens_batch_size" yaml:"tokens_batch_size" toml:"tokens_batch_size" doc:"Number of targets of one send call that are sent in parallel, at most <<1000>>. Default <<100>>. Requires restart."`
	// MaxInflight bounds the APNs requests in flight at the same time on the node.
	MaxInflight int `mapstructure:"max_inflight" default:"500" json:"max_inflight" envconfig:"max_inflight" yaml:"max_inflight" toml:"max_inflight" doc:"Bound of the APNs requests in flight at the same time on the node, at most <<10000>>. Default <<500>>. Requires restart."`
}

// PushFCM configures the FCM provider.
type PushFCM struct {
	// CredentialsFile is a path to the service account file of the FCM project.
	CredentialsFile string `mapstructure:"credentials_file" json:"credentials_file" envconfig:"credentials_file" yaml:"credentials_file" toml:"credentials_file" expose:"full" doc:"Path to the service account file of the FCM project. Required when FCM is enabled. Requires restart."`
	// TokensBatchSize is the number of targets of one call that are sent in parallel.
	TokensBatchSize int `mapstructure:"tokens_batch_size" default:"500" json:"tokens_batch_size" envconfig:"tokens_batch_size" yaml:"tokens_batch_size" toml:"tokens_batch_size" doc:"Number of targets of one send call that are sent in parallel, at most <<1000>>. Default <<500>>. Requires restart."`
	// MaxInflight bounds the FCM requests in flight at the same time on the node.
	MaxInflight int `mapstructure:"max_inflight" default:"200" json:"max_inflight" envconfig:"max_inflight" yaml:"max_inflight" toml:"max_inflight" doc:"Bound of the FCM requests in flight at the same time on the node, at most <<10000>>. Default <<200>>. Requires restart."`
}

// PushWebPush configures the Web Push provider.
type PushWebPush struct {
	// VAPIDPublicKey is the base64url form of the uncompressed P-256 public key of the VAPID key pair.
	VAPIDPublicKey string `mapstructure:"vapid_public_key" json:"vapid_public_key" envconfig:"vapid_public_key" yaml:"vapid_public_key" toml:"vapid_public_key" expose:"full" doc:"Base64url form of the 65-byte uncompressed P-256 public key of the VAPID key pair. The frontend uses the same value as its application server key. Required when Web Push is enabled. Requires restart."`
	// VAPIDPrivateKey is the base64url form of the 32-byte private scalar of the VAPID key pair, held inline.
	VAPIDPrivateKey Secret `mapstructure:"vapid_private_key" json:"vapid_private_key" envconfig:"vapid_private_key" yaml:"vapid_private_key" toml:"vapid_private_key" doc:"Base64url form of the 32-byte private scalar of the VAPID key pair, held inline. Masked in JSON, YAML and formatted output. Exactly one of this and <<vapid_private_key_file>> is set when Web Push is enabled. Requires restart."`
	// VAPIDPrivateKeyFile is a path to a file that holds the base64url form of the private scalar.
	VAPIDPrivateKeyFile string `mapstructure:"vapid_private_key_file" json:"vapid_private_key_file" envconfig:"vapid_private_key_file" yaml:"vapid_private_key_file" toml:"vapid_private_key_file" expose:"full" doc:"Path to a file that holds the base64url form of the private scalar of the VAPID key pair, for a key mounted as a file. Exactly one of this and <<vapid_private_key>> is set when Web Push is enabled. Requires restart."`
	// Subject is the sub claim of the VAPID token: a mailto: or https: URL.
	Subject string `mapstructure:"subject" json:"subject" envconfig:"subject" yaml:"subject" toml:"subject" expose:"full" doc:"Contact of the VAPID token, a <<mailto:>> or <<https://>> URL. Required when Web Push is enabled. Requires restart."`
	// TokensBatchSize is the number of targets of one call that are sent in parallel.
	TokensBatchSize int `mapstructure:"tokens_batch_size" default:"100" json:"tokens_batch_size" envconfig:"tokens_batch_size" yaml:"tokens_batch_size" toml:"tokens_batch_size" doc:"Number of targets of one send call that are sent in parallel, at most <<1000>>. Default <<100>>. Requires restart."`
	// MaxInflight bounds the Web Push requests in flight at the same time on the node.
	MaxInflight int `mapstructure:"max_inflight" default:"500" json:"max_inflight" envconfig:"max_inflight" yaml:"max_inflight" toml:"max_inflight" doc:"Bound of the Web Push requests in flight at the same time on the node, at most <<10000>>. Default <<500>>. Requires restart."`
	// AllowedEndpointOrigins replaces the built-in list of allowed push service origins when it is not empty.
	AllowedEndpointOrigins []string `mapstructure:"allowed_endpoint_origins" json:"allowed_endpoint_origins" envconfig:"allowed_endpoint_origins" yaml:"allowed_endpoint_origins" toml:"allowed_endpoint_origins" doc:"Glob patterns of the push service origins that subscriptions may name. A non-empty list replaces the built-in list. An empty list means the built-in list. <<*>> allows any origin. Requires restart."`
	// ExtraAllowedEndpointOrigins are added to the effective list of allowed push service origins.
	ExtraAllowedEndpointOrigins []string `mapstructure:"extra_allowed_endpoint_origins" json:"extra_allowed_endpoint_origins" envconfig:"extra_allowed_endpoint_origins" yaml:"extra_allowed_endpoint_origins" toml:"extra_allowed_endpoint_origins" doc:"Glob patterns added to the effective list of allowed push service origins. Requires restart."`
}
