package clientstorage

const (
	KeyMeta = "meta"
	// KeyToken holds the verified connection token, kept for the RPC method registry when it is enabled.
	KeyToken     = "token"
	KeyInstance  = "instance"
	KeyAuthClass = "auth_class"
	// KeyRPCInflight holds the calls with a msg_id which run on the connection, for exactly-once RPC.
	KeyRPCInflight = "rpc_inflight"
)
