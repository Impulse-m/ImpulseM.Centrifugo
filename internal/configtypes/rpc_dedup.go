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
	MaxReplySize int `mapstructure:"max_reply_size" default:"2048" json:"max_reply_size" envconfig:"max_reply_size" yaml:"max_reply_size" toml:"max_reply_size" doc:"Size in bytes above which the reply of a call is not kept: a retransmit of it gets rpc.registry.errors.result_evicted. An answer is also kept only up to scope_reserve_bytes / scope_reserve bytes, which is 2048 bytes at the defaults, so the limit in effect is the smaller of the two. A reply over this limit is counted as reply_size in dedup_evictions_total, and a reply over the share as uncached. Default <<2048>>."`
	// LocalMaxEntries bounds the number of answers kept by the node, anonymous ones included.
	LocalMaxEntries int `mapstructure:"local_max_entries" default:"200000" json:"local_max_entries" envconfig:"local_max_entries" yaml:"local_max_entries" toml:"local_max_entries" doc:"Maximum number of answers kept by the node, anonymous ones included. Every admitted scope is guaranteed scope_reserve of them. Each shard admits at most its share of local_max_entries / scope_reserve scopes, so the node admits fewer when the scopes hash unevenly over the shards. Above its reserve, a scope keeps answers while the node has room, and a full node evicts the oldest answer of a scope which is over its reserve, the one which has been over it the longest. An evicted answer is gone: a retransmit of its call runs the call again, and each eviction is counted by reason in dedup_evictions_total. Default <<200000>>."`
	// LocalMaxBytes bounds the bytes of the answers kept by the node.
	LocalMaxBytes int `mapstructure:"local_max_bytes" default:"268435456" json:"local_max_bytes" envconfig:"local_max_bytes" yaml:"local_max_bytes" toml:"local_max_bytes" doc:"Maximum bytes of the answers kept by the node, counting 256 bytes per answer plus its reply or error message. Every admitted scope is guaranteed scope_reserve_bytes of them, and each answer is kept up to scope_reserve_bytes / scope_reserve bytes, an answer over that is kept as uncached. Each shard admits at most its share of local_max_bytes / scope_reserve_bytes scopes. A full node evicts only from a scope which is over its answer reserve, so no answer within its reserve is evicted. Default <<268435456>> (256 MiB)."`
	// LocalAnonymousMaxEntries is the part of LocalMaxEntries kept for anonymous connections.
	LocalAnonymousMaxEntries int `mapstructure:"local_anonymous_max_entries" default:"20000" json:"local_anonymous_max_entries" envconfig:"local_anonymous_max_entries" yaml:"local_anonymous_max_entries" toml:"local_anonymous_max_entries" doc:"Part of local_max_entries kept for anonymous connections, which are scoped by their install instance only. A flood of anonymous instances fills this part only. 0 turns the dedup off for anonymous connections, whose calls are then not deduplicated. Default <<20000>>."`
	// LocalAnonymousMaxBytes is the part of LocalMaxBytes kept for anonymous connections.
	LocalAnonymousMaxBytes int `mapstructure:"local_anonymous_max_bytes" default:"33554432" json:"local_anonymous_max_bytes" envconfig:"local_anonymous_max_bytes" yaml:"local_anonymous_max_bytes" toml:"local_anonymous_max_bytes" doc:"Part of local_max_bytes kept for anonymous connections. Must be 0 when local_anonymous_max_entries is 0. Default <<33554432>> (32 MiB)."`
	// ScopeReserve is the number of answers every scope keeps, which no other scope can evict.
	ScopeReserve int `mapstructure:"scope_reserve" default:"4" json:"scope_reserve" envconfig:"scope_reserve" yaml:"scope_reserve" toml:"scope_reserve" doc:"Answers every scope keeps which no other scope can evict. The scopes a shard admits are at most its share of local_max_entries / scope_reserve, and of local_max_bytes / scope_reserve_bytes, so the node admits at most 64 times that. For authenticated scopes the entries are the part of local_max_entries which is not local_anonymous_max_entries, and the bytes likewise. The anonymous pool uses local_anonymous_max_entries and local_anonymous_max_bytes. Default <<4>>."`
	// ScopeReserveBytes is the number of bytes every scope keeps, which no other scope can evict.
	ScopeReserveBytes int `mapstructure:"scope_reserve_bytes" default:"8192" json:"scope_reserve_bytes" envconfig:"scope_reserve_bytes" yaml:"scope_reserve_bytes" toml:"scope_reserve_bytes" doc:"Bytes every scope keeps which no other scope can evict. Each answer is kept up to scope_reserve_bytes / scope_reserve bytes, so the answers within a scope's reserve fit these bytes. It must be at least 1024 bytes, and at least scope_reserve times 256 bytes, the overhead of an answer. Default <<8192>>."`
	// MaxScopesPerUser bounds the scopes of one user which hold answers on the node.
	MaxScopesPerUser int `mapstructure:"max_scopes_per_user" default:"8" json:"max_scopes_per_user" envconfig:"max_scopes_per_user" yaml:"max_scopes_per_user" toml:"max_scopes_per_user" doc:"Sessions and installs of one user which may hold answers or running calls on the node at once. A call of a further scope gets rpc.registry.errors.dedup_unavailable under on_store_error fail_closed, or runs without dedup under fail_open. Default <<8>>."`
	// MaxRecordsPerScope bounds the answers kept for one scope.
	MaxRecordsPerScope int `mapstructure:"max_records_per_scope" default:"1024" json:"max_records_per_scope" envconfig:"max_records_per_scope" yaml:"max_records_per_scope" toml:"max_records_per_scope" doc:"Maximum number of answers kept for one scope, which is a user session or install. Over it, the oldest answer of the scope is dropped, and a retransmit of its call runs the call again. At least scope_reserve. Default <<1024>>."`
	// OnStoreError is what a tracked call does when the dedup store fails.
	OnStoreError string `mapstructure:"on_store_error" default:"fail_closed" json:"on_store_error" envconfig:"on_store_error" yaml:"on_store_error" toml:"on_store_error" expose:"full" doc:"What a tracked call does when the dedup store fails: fail_closed answers it with rpc.registry.errors.dedup_unavailable, fail_open runs it without dedup. It also applies to a call of a new scope which the node cannot admit within its budgets. A call which runs under fail_open is not recorded, so a retransmit of it on another connection, while it runs, can run it again. On the same connection the retransmit is answered with dedup_unavailable. Default <<fail_closed>>."`
}
