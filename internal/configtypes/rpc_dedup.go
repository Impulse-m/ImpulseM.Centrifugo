package configtypes

// Behaviour of a tracked call when the dedup store fails. fail_closed answers the call with
// dedup_unavailable, so the client retries it later. fail_open runs the call without dedup.
const (
	RPCDedupFailClosed = "fail_closed"
	RPCDedupFailOpen   = "fail_open"
)

// RPCDedupStoreErrors lists the values of on_store_error.
var RPCDedupStoreErrors = []string{
	RPCDedupFailClosed,
	RPCDedupFailOpen,
}

// RPCDedup configures exactly-once RPC. A non-idempotent registry call with a msg_id is recorded, and a
// retransmit of it is answered from the record instead of running the call again. Off by default.
type RPCDedup struct {
	// Enabled turns the dedup on. It needs rpc.registry.enabled and client.reply_include_server_time.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables exactly-once RPC: a retransmitted call of a non-idempotent registry method is answered from the record of its first call instead of running again. Needs rpc.registry.enabled and client.reply_include_server_time. Read at start: a change of this value takes effect after a restart."`
	// Window is how long the answer of a call is kept, and how far back a msg_id may be.
	Window Duration `mapstructure:"window" default:"120s" json:"window" envconfig:"window" yaml:"window" toml:"window" doc:"How long the answer of a call is kept, and how far in the past a msg_id may be. Default <<120s>>."`
	// MaxSkew is how far in the future a msg_id may be, compared with the server time.
	MaxSkew Duration `mapstructure:"max_skew" default:"30s" json:"max_skew" envconfig:"max_skew" yaml:"max_skew" toml:"max_skew" doc:"How far in the future a msg_id may be, compared with the server time. Default <<30s>>."`
	// MaxReplySize is the size in bytes above which a reply is not kept.
	MaxReplySize int `mapstructure:"max_reply_size" default:"65536" json:"max_reply_size" envconfig:"max_reply_size" yaml:"max_reply_size" toml:"max_reply_size" doc:"Size in bytes above which the reply of a call is not kept: a retransmit of it gets rpc.registry.errors.result_evicted. Default <<65536>>."`
	// LocalMaxEntries bounds the number of answers kept by the node.
	LocalMaxEntries int `mapstructure:"local_max_entries" default:"200000" json:"local_max_entries" envconfig:"local_max_entries" yaml:"local_max_entries" toml:"local_max_entries" doc:"Maximum number of answers kept by the node. The least recently used are dropped first. Default <<200000>>."`
	// OnStoreError is what a tracked call does when the dedup store fails.
	OnStoreError string `mapstructure:"on_store_error" default:"fail_closed" json:"on_store_error" envconfig:"on_store_error" yaml:"on_store_error" toml:"on_store_error" expose:"full" doc:"What a tracked call does when the dedup store fails: fail_closed answers it with rpc.registry.errors.dedup_unavailable, fail_open runs it without dedup. Default <<fail_closed>>."`
}
