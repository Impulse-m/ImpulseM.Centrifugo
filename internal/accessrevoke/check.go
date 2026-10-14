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
	CountReject(result, stage)
	if result.Verdict == LoopGuard {
		LogLoopGuard(client.ID(), client.UserID(), result)
	}
	disconnect, rejected := RejectDisconnect(result, centrifuge.DisconnectExpired)
	if !rejected {
		return centrifuge.Disconnect{}, "", false
	}
	return disconnect, result.Reason, true
}

// RejectDisconnect returns the disconnect of a check result which rejects a connection, and reports whether the result
// rejects. A revoked connection gets revoked, which the caller chooses. An allowed result is not a rejection.
func RejectDisconnect(result Result, revoked centrifuge.Disconnect) (centrifuge.Disconnect, bool) {
	switch result.Verdict {
	case Blocked:
		return DisconnectUserBlocked, true
	case Revoked:
		return revoked, true
	case LoopGuard:
		return centrifuge.DisconnectInvalidToken, true
	}
	return centrifuge.Disconnect{}, false
}

// CountReject counts a rejected check result in the rejects metric under stage. An allowed result is not counted.
func CountReject(result Result, stage string) {
	if result.Verdict != Allowed {
		metrics.AccessRevokeRejectsTotal.WithLabelValues(string(result.Reason), stage).Inc()
	}
}

// LogLoopGuard logs a token which was issued after the revocation it is checked against.
func LogLoopGuard(clientID string, userID string, result Result) {
	log.Error().
		Str("client", clientID).
		Str("user", userID).
		Str("reason", string(result.Reason)).
		Msg("token issued after revocation, auth service and Centrifugo disagree, disconnecting")
}
