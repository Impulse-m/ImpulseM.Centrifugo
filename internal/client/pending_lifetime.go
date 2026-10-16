package client

import (
	"math"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
)

// maxLifetimeSeconds is the largest number of seconds a time.Duration holds. A lifetime over it is over any limit.
const maxLifetimeSeconds = int64(math.MaxInt64 / int64(time.Second))

// pendingLifetimeExceeded reports whether a connection token of class is a pending token over the limit maxLifetime.
// Only a pending token has a limit, and a zero limit is no limit. The lifetime of the token is its exp minus its iat,
// or its exp minus now when it has no iat. A token without exp has no end, so it is over any limit.
func pendingLifetimeExceeded(
	class rpcregistry.AuthClass,
	token jwtverify.ConnectToken,
	maxLifetime time.Duration,
	now time.Time,
) bool {
	if class != rpcregistry.AuthClassPending2FA || maxLifetime <= 0 {
		return false
	}
	if token.ExpireAt == 0 {
		return true
	}
	start := now.Unix()
	if token.IssuedAt != 0 {
		start = token.IssuedAt
	}
	lifetime := token.ExpireAt - start
	if lifetime >= maxLifetimeSeconds {
		return true
	}
	return time.Duration(lifetime)*time.Second > maxLifetime
}
