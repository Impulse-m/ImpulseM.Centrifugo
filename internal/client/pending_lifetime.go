package client

import (
	"math"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"

	"github.com/rs/zerolog/log"
)

// maxLifetimeSeconds is the largest number of seconds a time.Duration holds. A lifetime over it is over any limit.
const maxLifetimeSeconds = int64(math.MaxInt64 / int64(time.Second))

// pendingLifetimeExceeded reports whether a connection token of class is a pending token over the limit maxLifetime.
// Only a pending token has a limit, and a zero limit is no limit. The lifetime of the token is its exp minus the earlier
// of its iat and now, so an iat in the future does not shorten it. A token without exp has no end, so it is over any
// limit. A token without iat, or with an iat of 0, has no issue time to measure from, so it is over any limit too: a
// token issued long before the check would otherwise be measured from the check. An iat below zero is absent too: it
// would make the lifetime overflow. The clock is read only for a pending token under a limit.
func pendingLifetimeExceeded(
	class rpcregistry.AuthClass,
	token jwtverify.ConnectToken,
	maxLifetime time.Duration,
	clock func() time.Time,
) bool {
	if class != rpcregistry.AuthClassPending2FA || maxLifetime <= 0 {
		return false
	}
	if token.ExpireAt == 0 || token.IssuedAt <= 0 {
		return true
	}
	start := clock().Unix()
	if token.IssuedAt < start {
		start = token.IssuedAt
	}
	lifetime := token.ExpireAt - start
	if lifetime >= maxLifetimeSeconds {
		return true
	}
	return time.Duration(lifetime)*time.Second > maxLifetime
}

// pendingLifetimeRefused reports whether a connection token of class of clientID is over the pending lifetime limit of
// the config. It is checked at connect and at refresh, and it logs the refusal.
func (h *Handler) pendingLifetimeRefused(clientID string, class rpcregistry.AuthClass, token jwtverify.ConnectToken) bool {
	if class != rpcregistry.AuthClassPending2FA {
		return false
	}
	maxLifetime := h.cfgContainer.ConfigRef().RPC.Registry.PendingMaxLifetime.ToDuration()
	if !pendingLifetimeExceeded(class, token, maxLifetime, time.Now) {
		return false
	}
	log.Info().Str("client", clientID).Msg("pending token lifetime over the limit")
	return true
}
