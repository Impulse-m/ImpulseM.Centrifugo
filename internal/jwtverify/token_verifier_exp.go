package jwtverify

// tokenExpOf returns the exp claim of a connection token, or 0 when the token has none. It is the JWT expiry which the
// backend checks, and it is not the expiry of the connection, which expire_at may move.
func tokenExpOf(claims *ConnectTokenClaims) int64 {
	if claims.ExpiresAt == nil {
		return 0
	}
	return claims.ExpiresAt.Unix()
}
