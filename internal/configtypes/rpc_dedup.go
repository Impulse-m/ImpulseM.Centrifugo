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

// RPCDedupRedis configures the shared tier of exactly-once RPC. It has the shape of the Redis config of
// client.rate_limit.redis_user_command: reuse_from_engine, or a prefix and a Redis config.
type RPCDedupRedis struct {
	// Enabled keeps the records in Redis, shared by all nodes.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Keeps the records of exactly-once RPC in Redis, shared by all nodes, with the node-local store as a cache. Off by default: records are then node-local. Read at start: a change takes effect after a restart."`
	// ReuseFromEngine uses the Redis of the engine.
	ReuseFromEngine bool `mapstructure:"reuse_from_engine" json:"reuse_from_engine" envconfig:"reuse_from_engine" yaml:"reuse_from_engine" toml:"reuse_from_engine" doc:"Use the Redis and the prefix of the Redis engine. Requires engine.type redis."`
	// RedisPrefixed is a Redis configuration. Ignored when ReuseFromEngine is set.
	RedisPrefixed `mapstructure:",squash" yaml:",inline"`
	// Wakeup is the wakeup of a duplicate which waits for a call on another node.
	Wakeup RPCDedupWakeup `mapstructure:"wakeup" json:"wakeup" envconfig:"wakeup" yaml:"wakeup" toml:"wakeup" doc:"Wakeup of duplicates which wait for a call on another node, see rpc.registry.dedup.redis.wakeup.enabled."`
}

// RPCDedupWakeup configures the wakeup of a duplicate which waits for a call running on another node: the node which
// runs the call writes a wake into a Redis stream of each waiting node when its record is written, and the waiting node
// reads the answer at once instead of polling for it.
type RPCDedupWakeup struct {
	// Enabled turns the wakeup on. Its default is true, set in the config defaults rather than by a default tag, so that
	// an explicit false is kept.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Wakes a duplicate which waits for a call running on another node by a wake which that node writes to a stream of this node in the dedup Redis when the answer is written. While a node has such duplicates waiting, and for a minute after, it keeps one blocking read per Redis shard on its stream; otherwise nothing. A wait reads the answer on each wake it gets, once about 2 seconds after it started, and once more when its deadline passes with no answer read, which covers a lost wake; a repeated wake, such as one after a restart, costs one more read. A wait never polls on a timer. Needs rpc.registry.dedup.redis.enabled, and is ignored without it. Read at start: a change takes effect after a restart. Default <<true>>. Off, the wait polls Redis every 50 to 500 ms for the answer instead, as the fallback. Keep it on every node."`
	// MaxWaiterNodes bounds the nodes which an in-flight record lists as waiting for its call.
	MaxWaiterNodes int `mapstructure:"max_waiter_nodes" default:"64" json:"max_waiter_nodes" envconfig:"max_waiter_nodes" yaml:"max_waiter_nodes" toml:"max_waiter_nodes" doc:"Nodes which an in-flight record lists as waiting for its call. A duplicate on a node beyond it does not wait: it is answered with the temporary later error at once, and counted as a refused wait. The list holds node ids, one per node which ran a duplicate of the call while it was in flight; a node which restarted in that time counts again with its new id. Size it to the nodes which can receive a retransmit of one call within its method timeout plus 10 seconds, which is the whole cluster behind one load balancer: keep the default unless refused waits are counted. At least 1, at most 1024. Default <<64>>."`
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
	MaxReplySize int `mapstructure:"max_reply_size" default:"2048" json:"max_reply_size" envconfig:"max_reply_size" yaml:"max_reply_size" toml:"max_reply_size" doc:"Size in bytes above which the reply of a call is not kept: a retransmit of it gets rpc.registry.errors.result_evicted. An answer is also kept only up to scope_reserve_bytes / scope_reserve bytes less the 256 bytes of a record's overhead, which is 1792 bytes at the defaults, so the limit in effect is the smaller of the two. A reply over this limit is counted as reply_size in dedup_evictions_total, and a reply over the share as uncached. Default <<2048>>."`
	// LocalMaxEntries bounds the number of answers kept by the node, anonymous ones included.
	LocalMaxEntries int `mapstructure:"local_max_entries" default:"200000" json:"local_max_entries" envconfig:"local_max_entries" yaml:"local_max_entries" toml:"local_max_entries" doc:"Maximum number of answers kept by the node, anonymous ones included. Every admitted scope is guaranteed scope_reserve of them. Each shard admits at most its share of local_max_entries / scope_reserve scopes, so the node admits fewer when the scopes hash unevenly over the shards. Above its reserve, a scope keeps answers while the node has room, and a full node evicts the oldest answer of a scope which is over its reserve, the one which has been over it the longest. An evicted answer is gone: a retransmit of its call runs the call again, and each eviction is counted by reason in dedup_evictions_total. Default <<200000>>."`
	// LocalMaxBytes bounds the bytes of the answers kept by the node.
	LocalMaxBytes int `mapstructure:"local_max_bytes" default:"268435456" json:"local_max_bytes" envconfig:"local_max_bytes" yaml:"local_max_bytes" toml:"local_max_bytes" doc:"Maximum bytes of the answers kept by the node, counting 256 bytes per answer plus its reply or the encoded form of its error. Every admitted scope is guaranteed scope_reserve_bytes of them, and each answer is kept up to scope_reserve_bytes / scope_reserve bytes, an answer over that is kept as uncached. Each shard admits at most its share of local_max_bytes / scope_reserve_bytes scopes. A full node evicts only from a scope which is over its answer reserve, so no answer within its reserve is evicted. Default <<268435456>> (256 MiB)."`
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
	OnStoreError string `mapstructure:"on_store_error" default:"fail_closed" json:"on_store_error" envconfig:"on_store_error" yaml:"on_store_error" toml:"on_store_error" expose:"full" doc:"What a tracked call does when the dedup store fails: fail_closed answers it with rpc.registry.errors.dedup_unavailable, fail_open runs it without dedup. It also applies to a call of a new scope which the node cannot admit within its budgets. A call which runs under fail_open is not recorded, so a retransmit of it on another connection, while it runs, can run it again. On the same connection the retransmit is answered with dedup_unavailable. With rpc.registry.dedup.redis.enabled, fail_open runs the call with node-local dedup when Redis fails. Default <<fail_closed>>."`
	// Redis is the shared tier of the records, see rpc.registry.dedup.redis.enabled.
	Redis RPCDedupRedis `mapstructure:"redis" json:"redis" envconfig:"redis" yaml:"redis" toml:"redis" doc:"Redis of the shared tier, see rpc.registry.dedup.redis.enabled."`
}

// RPCDedupRecordOverhead is the size counted for a record besides its answer, see rpcdedup.RecordOverhead. It is
// counted within the ScopeShare of a record.
const RPCDedupRecordOverhead = 256

// ScopeShare returns the bytes of a record which a scope keeps in its reserve: scope_reserve_bytes divided by
// scope_reserve. The configuration is validated before it is used.
func (d RPCDedup) ScopeShare() int {
	return d.ScopeReserveBytes / d.ScopeReserve
}

// ReplyLimit returns the largest answer which the stores keep, in both tiers: max_reply_size, and at most the ScopeShare
// less RPCDedupRecordOverhead, so that a record with such an answer fits its share. The limit applies to a reply, and
// to an error by the length of its encoded form. It is zero when the share is below the overhead, which the
// configuration validation refuses.
func (d RPCDedup) ReplyLimit() int {
	return max(min(d.MaxReplySize, d.ScopeShare()-RPCDedupRecordOverhead), 0)
}
