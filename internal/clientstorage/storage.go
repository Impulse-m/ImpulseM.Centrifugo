package clientstorage

const (
	KeyMeta = "meta"
	// KeyToken holds the verified connection token, kept for the RPC method registry when it is enabled.
	KeyToken     = "token"
	KeyInstance  = "instance"
	KeyAuthClass = "auth_class"
	// KeyTokenUser holds the user of the connection token, which a pending connection keeps even though Core sees no
	// user for it. It is stored with the auth class, and a connection without a token has none.
	KeyTokenUser = "token_user"
)
