package clientstorage

const (
	KeyMeta = "meta"
	// KeyToken, KeyInstance and KeyAuthClass are reserved for the RPC method registry.
	// Later phases store the connection token, the instance identifier and the auth
	// class of the connection under these keys. Nothing reads them yet.
	KeyToken     = "token"
	KeyInstance  = "instance"
	KeyAuthClass = "auth_class"
)
