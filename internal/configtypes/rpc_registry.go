package configtypes

// Auth classes a registry method can require. The empty string means the method
// states no class, which is allowed only when defaults.auth_class is set to a class
// other than anonymous.
const (
	RPCAuthClassAnonymous     = "anonymous"
	RPCAuthClassPending2FA    = "pending_2fa"
	RPCAuthClassAuthenticated = "authenticated"
	RPCAuthClassBot           = "bot"
)

// RPCAuthClasses lists every auth class name. Validation and the registry both rely on it.
var RPCAuthClasses = []string{
	RPCAuthClassAnonymous,
	RPCAuthClassPending2FA,
	RPCAuthClassAuthenticated,
	RPCAuthClassBot,
}

// Load balancing policies a registry target can use.
const (
	RPCLoadBalancingRoundRobin = "round_robin"
	RPCLoadBalancingPickFirst  = "pick_first"
)

// RPCLoadBalancingPolicies lists every load balancing policy name.
var RPCLoadBalancingPolicies = []string{
	RPCLoadBalancingRoundRobin,
	RPCLoadBalancingPickFirst,
}

// RPCRegistry configures the RPC method registry: a table which maps client RPC method
// names to gRPC methods of backend targets. Entries are either read from a separate file
// (File) or given inline (Defaults, Targets, Methods, mostly useful in tests). Both at once
// is an error.
type RPCRegistry struct {
	// Enabled turns the registry on. When disabled, the registry is not consulted.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the RPC method registry. When disabled, client RPC calls do not use it."`
	// File is a path to a JSON or YAML file with the registry entries. Format is chosen by extension.
	File string `mapstructure:"file" json:"file" envconfig:"file" yaml:"file" toml:"file" expose:"full" doc:"Path to a JSON or YAML file with the registry defaults, targets and methods. Format is chosen by file extension. Must not be combined with inline defaults, targets or methods."`
	// PollInterval is how often the registry file is checked for changes. Zero means reload on SIGHUP only.
	PollInterval Duration `mapstructure:"poll_interval" json:"poll_interval" envconfig:"poll_interval" yaml:"poll_interval" toml:"poll_interval" doc:"How often the registry file is checked for changes. Zero means the file is reloaded on SIGHUP only."`
	// DrainTimeout is how long connections of removed or replaced targets are kept before they are closed.
	DrainTimeout Duration `mapstructure:"drain_timeout" default:"30s" json:"drain_timeout" envconfig:"drain_timeout" yaml:"drain_timeout" toml:"drain_timeout" doc:"How long connections of removed or replaced targets stay open before they are closed. Default <<30s>>."`
	// Errors maps registry-originated errors to Centrifugo protocol errors.
	Errors RPCRegistryErrors `mapstructure:"errors" json:"errors" envconfig:"errors" yaml:"errors" toml:"errors" doc:"Protocol errors returned for registry-originated failures: unknown method, auth class mismatch, request too large and cancelled call."`
	// AppError configures which backend trailers carry application error code and type.
	AppError RPCRegistryAppError `mapstructure:"app_error" json:"app_error" envconfig:"app_error" yaml:"app_error" toml:"app_error" doc:"Names of gRPC trailers which carry the application error code and type returned by the backend."`
	// GRPCStatusToCodeTransforms overrides the default mapping of gRPC status codes to protocol errors.
	GRPCStatusToCodeTransforms RPCGRPCStatusToCodeTransforms `mapstructure:"grpc_status_to_code_transforms" json:"grpc_status_to_code_transforms" envconfig:"grpc_status_to_code_transforms" yaml:"grpc_status_to_code_transforms" toml:"grpc_status_to_code_transforms" doc:"Rules overriding the default mapping of gRPC status codes from backend targets to protocol errors or disconnects."`

	// Defaults are applied to every method which does not set the field itself.
	Defaults RPCMethodDefaults `mapstructure:"defaults" json:"defaults" envconfig:"defaults" yaml:"defaults" toml:"defaults" doc:"Defaults applied to methods and targets which do not set the field themselves. Set <<auth_class>> here, or every method must state its own class."`
	// Targets are gRPC backends the methods call. Inline alternative to File.
	Targets []RPCTarget `mapstructure:"targets" json:"targets" envconfig:"targets" yaml:"targets" toml:"targets" doc:"gRPC backend targets the registry methods call. Use this or <<file>>, not both."`
	// Methods are the registry entries. Inline alternative to File.
	Methods []RPCMethod `mapstructure:"methods" json:"methods" envconfig:"methods" yaml:"methods" toml:"methods" doc:"Registry entries: client RPC method names mapped to gRPC methods of targets. Use this or <<file>>, not both."`
}

// RPCRegistryErrors holds the protocol errors returned for registry-originated failures.
type RPCRegistryErrors struct {
	// UnknownMethod is returned when a client calls a method missing from the registry.
	UnknownMethod RPCRegistryError `mapstructure:"unknown_method" json:"unknown_method" envconfig:"unknown_method" yaml:"unknown_method" toml:"unknown_method" doc:"Error returned when the called method is not in the registry."`
	// AuthClass is returned when the connection does not satisfy the method auth class.
	AuthClass RPCRegistryError `mapstructure:"auth_class" json:"auth_class" envconfig:"auth_class" yaml:"auth_class" toml:"auth_class" doc:"Error returned when the connection does not satisfy the auth class of the method."`
	// RequestTooLarge is returned when the request data exceeds the method limit.
	RequestTooLarge RPCRegistryError `mapstructure:"request_too_large" json:"request_too_large" envconfig:"request_too_large" yaml:"request_too_large" toml:"request_too_large" doc:"Error returned when the request exceeds the max request size of the method."`
	// Cancelled is returned when the client cancels a call.
	Cancelled RPCRegistryError `mapstructure:"cancelled" json:"cancelled" envconfig:"cancelled" yaml:"cancelled" toml:"cancelled" doc:"Error returned when the client cancels the call."`
}

// RPCRegistryError is a protocol error with optional application-level code and type.
type RPCRegistryError struct {
	// Code is the Centrifugo protocol error code.
	Code uint32 `mapstructure:"code" json:"code" envconfig:"code" yaml:"code" toml:"code" doc:"Centrifugo protocol error code."`
	// Message is a human-readable error message.
	Message string `mapstructure:"message" json:"message" envconfig:"message" yaml:"message" toml:"message" expose:"full" doc:"Human-readable error message sent to the client."`
	// AppCode is an application error code. Zero means not set.
	AppCode int32 `mapstructure:"app_code" json:"app_code" envconfig:"app_code" yaml:"app_code" toml:"app_code" doc:"Application-level error code. Zero means not set."`
	// AppType is an application error type. Empty means not set.
	AppType string `mapstructure:"app_type" json:"app_type" envconfig:"app_type" yaml:"app_type" toml:"app_type" expose:"full" doc:"Application-level error type. Empty means not set."`
}

// RPCRegistryAppError names the gRPC trailers carrying application error details.
type RPCRegistryAppError struct {
	// CodeKey is the name of the trailer with the application error code.
	CodeKey string `mapstructure:"code_key" default:"app-code" json:"code_key" envconfig:"code_key" yaml:"code_key" toml:"code_key" expose:"full" doc:"Name of the gRPC trailer carrying the application error code. Default <<app-code>>."`
	// TypeKey is the name of the trailer with the application error type.
	TypeKey string `mapstructure:"type_key" default:"app-type" json:"type_key" envconfig:"type_key" yaml:"type_key" toml:"type_key" expose:"full" doc:"Name of the gRPC trailer carrying the application error type. Default <<app-type>>."`
}

// RPCGRPCStatusToCodeTransforms is a list of gRPC status to protocol error rules.
type RPCGRPCStatusToCodeTransforms []RPCGRPCStatusToCodeTransform

// RPCGRPCStatusToCodeTransform maps one gRPC status to a protocol error or disconnect.
type RPCGRPCStatusToCodeTransform struct {
	// Status is a gRPC status code name, for example UNAUTHENTICATED.
	Status string `mapstructure:"status" json:"status" envconfig:"status" yaml:"status" toml:"status" expose:"full" doc:"gRPC status code name to match, for example <<UNAUTHENTICATED>>."`
	// ToError is a transform to protocol error.
	ToError TransformError `mapstructure:"to_error" json:"to_error" envconfig:"to_error" yaml:"to_error" toml:"to_error" doc:"Protocol error to return when the gRPC status matches."`
	// ToDisconnect is a transform to protocol disconnect.
	ToDisconnect TransformDisconnect `mapstructure:"to_disconnect" json:"to_disconnect" envconfig:"to_disconnect" yaml:"to_disconnect" toml:"to_disconnect" doc:"Disconnect to send when the gRPC status matches."`
}

// RPCMethodDefaults are defaults for registry methods and targets.
type RPCMethodDefaults struct {
	// AuthClass is the auth class of methods which do not set one.
	AuthClass string `mapstructure:"auth_class" json:"auth_class" envconfig:"auth_class" yaml:"auth_class" toml:"auth_class" expose:"full" doc:"Auth class of methods which do not set one: anonymous, pending_2fa, authenticated or bot. Empty means every method must set its own class. Never defaults to anonymous."`
	// Timeout is the call timeout of methods and targets which do not set one.
	Timeout Duration `mapstructure:"timeout" json:"timeout" envconfig:"timeout" yaml:"timeout" toml:"timeout" doc:"Call timeout for methods and targets which do not set one. Zero means the built-in default of <<5s>>."`
	// MaxRequestBytes is the request size limit of methods which do not set one.
	MaxRequestBytes int `mapstructure:"max_request_bytes" json:"max_request_bytes" envconfig:"max_request_bytes" yaml:"max_request_bytes" toml:"max_request_bytes" doc:"Request size limit in bytes for methods which do not set one. Zero means the built-in default of 64 KiB."`
	// MaxResponseBytes is the response size limit of methods and targets which do not set one.
	MaxResponseBytes int `mapstructure:"max_response_bytes" json:"max_response_bytes" envconfig:"max_response_bytes" yaml:"max_response_bytes" toml:"max_response_bytes" doc:"Response size limit in bytes for methods and targets which do not set one. Zero means the built-in default of 1 MiB."`
	// Idempotent is the default idempotency of methods.
	Idempotent bool `mapstructure:"idempotent" json:"idempotent" envconfig:"idempotent" yaml:"idempotent" toml:"idempotent" doc:"Whether methods are idempotent by default. Idempotent methods may be retried safely."`
}

// RPCTarget is a gRPC backend the registry methods call.
type RPCTarget struct {
	// Name is the unique name of the target, referenced by methods.
	Name string `mapstructure:"name" json:"name" envconfig:"name" yaml:"name" toml:"name" expose:"full" doc:"Unique name of the target. Methods reference the target by this name."`
	// Endpoint is the gRPC address of the target, for example dns:///service.example:9000.
	Endpoint string `mapstructure:"endpoint" json:"endpoint" envconfig:"endpoint" yaml:"endpoint" toml:"endpoint" expose:"full" doc:"gRPC address of the target, for example <<dns:///service.example:9000>>."`
	// LoadBalancing is the gRPC load balancing policy.
	LoadBalancing string `mapstructure:"load_balancing" default:"round_robin" json:"load_balancing" envconfig:"load_balancing" yaml:"load_balancing" toml:"load_balancing" expose:"full" doc:"gRPC load balancing policy: <<round_robin>> (default) or <<pick_first>>."`
	// Timeout is the call timeout for methods of this target which do not set one.
	Timeout Duration `mapstructure:"timeout" json:"timeout" envconfig:"timeout" yaml:"timeout" toml:"timeout" doc:"Call timeout for methods of this target which do not set one. Zero means the defaults apply."`
	// MaxRequestBytes is the request size limit for methods of this target which do not set one.
	MaxRequestBytes int `mapstructure:"max_request_bytes" json:"max_request_bytes" envconfig:"max_request_bytes" yaml:"max_request_bytes" toml:"max_request_bytes" doc:"Request size limit in bytes for methods of this target which do not set one. Zero means the defaults apply."`
	// MaxResponseBytes is the response size limit for methods of this target which do not set one.
	MaxResponseBytes int `mapstructure:"max_response_bytes" json:"max_response_bytes" envconfig:"max_response_bytes" yaml:"max_response_bytes" toml:"max_response_bytes" doc:"Response size limit in bytes for methods of this target which do not set one. Zero means the defaults apply."`
	// EmulatedHeaders are connect-time client headers forwarded to the target.
	EmulatedHeaders []string `mapstructure:"emulated_headers" json:"emulated_headers" envconfig:"emulated_headers" yaml:"emulated_headers" toml:"emulated_headers" expose:"full" doc:"Names of connect-time client headers forwarded to the target. Lower-case. Reserved names are rejected."`
	// GRPC is the gRPC client configuration of the target: TLS, credentials, compression and static metadata.
	GRPC ProxyCommonGRPC `mapstructure:"grpc" json:"grpc" envconfig:"grpc" yaml:"grpc" toml:"grpc" doc:"gRPC client configuration of the target: TLS, credentials, compression and static metadata."`
}

// RPCMethod is one registry entry: a client RPC method mapped to a gRPC method of a target.
type RPCMethod struct {
	// Method is the client RPC method name, unique in the registry.
	Method string `mapstructure:"method" json:"method" envconfig:"method" yaml:"method" toml:"method" expose:"full" doc:"Client RPC method name. Must be unique in the registry."`
	// GRPCMethod is the full gRPC method path, for example /example.v1.UserService/GetUser.
	GRPCMethod string `mapstructure:"grpc_method" json:"grpc_method" envconfig:"grpc_method" yaml:"grpc_method" toml:"grpc_method" expose:"full" doc:"Full gRPC method path, for example <</example.v1.UserService/GetUser>>."`
	// Target is the name of the target the method is called on.
	Target string `mapstructure:"target" json:"target" envconfig:"target" yaml:"target" toml:"target" expose:"full" doc:"Name of the target the method is called on."`
	// AuthClass is the auth class the connection must satisfy to call the method.
	AuthClass string `mapstructure:"auth_class" json:"auth_class" envconfig:"auth_class" yaml:"auth_class" toml:"auth_class" expose:"full" doc:"Auth class the connection must satisfy: anonymous, pending_2fa, authenticated or bot. Empty inherits defaults.auth_class."`
	// Timeout is the call timeout of the method.
	Timeout Duration `mapstructure:"timeout" json:"timeout" envconfig:"timeout" yaml:"timeout" toml:"timeout" doc:"Call timeout of the method. Zero inherits the target or defaults."`
	// MaxRequestBytes is the request size limit of the method.
	MaxRequestBytes int `mapstructure:"max_request_bytes" json:"max_request_bytes" envconfig:"max_request_bytes" yaml:"max_request_bytes" toml:"max_request_bytes" doc:"Request size limit in bytes of the method. Zero inherits the defaults."`
	// MaxResponseBytes is the response size limit of the method.
	MaxResponseBytes int `mapstructure:"max_response_bytes" json:"max_response_bytes" envconfig:"max_response_bytes" yaml:"max_response_bytes" toml:"max_response_bytes" doc:"Response size limit in bytes of the method. Zero inherits the target or defaults."`
	// Idempotent overrides the default idempotency of the method. Nil inherits defaults.idempotent.
	Idempotent *bool `mapstructure:"idempotent" json:"idempotent" envconfig:"idempotent" yaml:"idempotent" toml:"idempotent" doc:"Whether the method is idempotent. Unset inherits defaults.idempotent."`
	// Headers are per-call client headers forwarded to the target.
	Headers []string `mapstructure:"headers" json:"headers" envconfig:"headers" yaml:"headers" toml:"headers" expose:"full" doc:"Names of per-call client headers forwarded to the target. Lower-case. Reserved names are rejected."`
	// ErrorMetadata lists backend trailer keys copied into the protocol error metadata.
	ErrorMetadata []string `mapstructure:"error_metadata" json:"error_metadata" envconfig:"error_metadata" yaml:"error_metadata" toml:"error_metadata" expose:"full" doc:"Backend trailer keys copied into the protocol error metadata. Lower-case."`
}

// RPCRegistryFile is the content of the registry file.
type RPCRegistryFile struct {
	// Version is the format version of the file. Must be 1.
	Version int `mapstructure:"version" json:"version" envconfig:"version" yaml:"version" toml:"version" doc:"Format version of the registry file. Must be <<1>>."`
	// Defaults are applied to methods and targets which do not set the field themselves.
	Defaults RPCMethodDefaults `mapstructure:"defaults" json:"defaults" envconfig:"defaults" yaml:"defaults" toml:"defaults" doc:"Defaults applied to methods and targets which do not set the field themselves."`
	// Targets are gRPC backend targets.
	Targets []RPCTarget `mapstructure:"targets" json:"targets" envconfig:"targets" yaml:"targets" toml:"targets" doc:"gRPC backend targets the registry methods call."`
	// Methods are the registry entries.
	Methods []RPCMethod `mapstructure:"methods" json:"methods" envconfig:"methods" yaml:"methods" toml:"methods" doc:"Registry entries: client RPC method names mapped to gRPC methods of targets."`
}
