package accessrevoke

import (
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// OptionsFromConfig returns access revocation checks enabled in config.
func OptionsFromConfig(cfg *config.Config) CheckOptions {
	return CheckOptions{
		Block:      cfg.UserBlock.Enabled,
		Session:    cfg.TokenRevoke.Enabled,
		UserTokens: cfg.UserTokensInvalidate.Enabled,
	}
}

// CheckConnection checks a connection registered in the hub against the store. It returns the
// disconnect for a rejected connection and the reason of the rejection. Stage labels the rejects metric.
// A connection with zero token identity came from the connect proxy (no token), so only user block
// applies to it. The same is true for a token which has no session, generation and iat claims, which
// is a deliberate trade-off: the full check at connect time already ran for such a token.
func CheckConnection(
	store *Store,
	client *centrifuge.Client,
	opts CheckOptions,
	stage string,
) (centrifuge.Disconnect, Reason, bool) {
	identity := client.TokenIdentity()
	if identity == (centrifuge.TokenIdentity{}) {
		opts = CheckOptions{Block: opts.Block}
	}
	result := store.Check(client.UserID(), identity, opts)
	if result.Verdict != Allowed {
		metrics.AccessRevokeRejectsTotal.WithLabelValues(string(result.Reason), stage).Inc()
	}
	switch result.Verdict {
	case Blocked:
		return DisconnectUserBlocked, result.Reason, true
	case Revoked:
		return centrifuge.DisconnectExpired, result.Reason, true
	case LoopGuard:
		LogLoopGuard(client.ID(), client.UserID(), result)
		return centrifuge.DisconnectInvalidToken, result.Reason, true
	}
	return centrifuge.Disconnect{}, "", false
}

// LogLoopGuard logs a token which was issued after the revocation it is checked against.
func LogLoopGuard(clientID string, userID string, result Result) {
	log.Error().
		Str("client", clientID).
		Str("user", userID).
		Str("reason", string(result.Reason)).
		Msg("token issued after revocation, auth service and Centrifugo disagree, disconnecting")
}
