package accessrevoke

// Skip reasons of source records, used as metric label values.
const (
	skipUnknownMethod = "unknown_method"
	skipDecodeError   = "decode_error"
	skipValidation    = "validation_error"
	skipDisabled      = "disabled"
	skipStale         = "stale"
	skipApplyError    = "apply_error"
)

// skipReasons is the fixed set of skip reasons of source records. The source keeps one sampled warning log per reason.
var skipReasons = []string{
	skipUnknownMethod,
	skipDecodeError,
	skipValidation,
	skipDisabled,
	skipStale,
	skipApplyError,
}

// Reject reasons of connects refused by the source state, used as metric label values.
const (
	// RejectNotReady is used while the source is not ready.
	RejectNotReady = "not_ready"
	// RejectLagging is used while the source lags and reject_connect_on_lag is set.
	RejectLagging = "lagging"
)

// stageSweep is the rejects metric stage of the catch-up sweep.
const stageSweep = "sweep"

// stageRetry is the rejects metric stage of a retry of dropped disconnects.
const stageRetry = "retry"

// Kinds of a retried key, used as metric label values.
const (
	retryKindUser    = "user"
	retryKindSession = "session"
)

// Sources of an expire_at clamped to access_revoke.max_ttl, used as metric label values.
const (
	ClampSourceAPI      = "api"
	ClampSourceConsumer = "consumer"
	ClampSourceKafka    = "kafka"
	ClampSourceNotify   = "notify"
)
