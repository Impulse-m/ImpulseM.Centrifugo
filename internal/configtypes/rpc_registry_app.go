package configtypes

import (
	"regexp"
)

// RPCAppErrorTypePattern is the format of an application error type, from a trailer, a status message or a transform.
var RPCAppErrorTypePattern = regexp.MustCompile(`^[A-Z0-9_]{1,64}$`)

// RPCRetryAfterPlaceholder is the placeholder of the wait in rpc.registry.errors.rate_limited.app_type. A refusal replaces
// each one with the whole-second wait of its x-centrifugo-retry-after metadata.
const RPCRetryAfterPlaceholder = "{retry_after}"

// RPCRetryAfterMaxValue is the widest value the placeholder takes. A wait is a duration of at most 9223372037 whole seconds,
// which has ten digits, so config checks the app type with this value in place of each placeholder.
const RPCRetryAfterMaxValue = "9999999999"

// RPCTransformError is the protocol error of a gRPC status transform. Besides the fields of TransformError it
// sets the application code and type. They fill what the backend did not send: its trailers win.
type RPCTransformError struct {
	TransformError `mapstructure:",squash" yaml:",inline"`
	// AppCode is the application error code used when the backend sends no valid code trailer. Zero means the default code of the status.
	AppCode int32 `mapstructure:"app_code" json:"app_code" envconfig:"app_code" yaml:"app_code" toml:"app_code" doc:"Application error code when the backend sends no valid code trailer. Zero means the default code of the status. Needs <<to_error.code>>."`
	// AppType is the application error type used when the backend gives none. Empty means none.
	AppType string `mapstructure:"app_type" json:"app_type" envconfig:"app_type" yaml:"app_type" toml:"app_type" expose:"full" doc:"Application error type when the backend gives none, neither in the type trailer nor as a status message matching ^[A-Z0-9_]{1,64}$. Must match that pattern. Empty means none. Needs <<to_error.code>>."`
}
