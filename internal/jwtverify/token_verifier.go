package jwtverify

import (
	"encoding/json"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
)

type Verifier interface {
	VerifyConnectToken(token string, skipVerify bool) (ConnectToken, error)
	VerifySubscribeToken(token string, skipVerify bool) (SubscribeToken, error)
}

type ConnectToken struct {
	// UserID tells an ID of connecting user.
	UserID string
	// AuthClass is the auth class of the connection. Taken from the claim configured by
	// rpc.registry.auth_class_claim. Empty when the claim is not configured or absent.
	AuthClass string
	// ExpireAt allows setting time in future when connection must be validated.
	// Validation can be server-side using On().Refresh callback or client-side
	// if On().Refresh not set.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Alive information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
	// Meta is custom data to append to a connection. Can be retrieved later
	// for connection inspection. Never sent in a client protocol and accessible
	// from a backend-side only.
	Meta json.RawMessage
	// Subs is a map of channels to subscribe server-side with options.
	Subs map[string]centrifuge.SubscribeOptions
	// Session identifies a session the token was issued for. Taken from the claim
	// configured by token_revoke.uid_claim (jti by default). Empty when absent.
	Session string
	// Generation is a revocation generation of the token. Taken from the claim configured
	// by user_tokens_invalidate.generation_claim. Zero when the claim is not configured or absent.
	Generation uint64
	// IssuedAt is a Unix time in seconds the token was issued at (iat claim). Zero when absent.
	IssuedAt int64
	// FilterTemplateClaims are the values of the claims which server tags filter templates refer to.
	// Zero when no namespace has a template.
	FilterTemplateClaims filtertemplate.Claims
}

type SubscribeToken struct {
	// UserID tells an ID of subscribing user.
	UserID string
	// Channel client wants to subscribe. Will be compared with channel in
	// subscribe command.
	Channel string
	// Client is a deprecated claim for compatibility with Centrifugo v3.
	Client string
	// Options for subscription.
	Options centrifuge.SubscribeOptions
}
