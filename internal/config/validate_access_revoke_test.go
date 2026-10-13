package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func TestValidateEd25519PublicKeys(t *testing.T) {
	// Only structure is checked here, PEM parsing happens when building verifier config.
	const pem = "placeholder-public-key"

	tests := []struct {
		name            string
		keys            configtypes.Ed25519PublicKeys
		jwksEndpoint    string
		subscriptionTok bool
		wantErr         string
	}{
		{
			name: "single key without kid is valid",
			keys: configtypes.Ed25519PublicKeys{
				{PublicKey: pem},
			},
		},
		{
			name: "multiple keys with unique kids are valid",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem},
				{KID: "k2", PublicKey: pem},
			},
		},
		{
			name: "empty public key is rejected",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1"},
			},
			wantErr: "ed25519_public_keys[0]: public_key is required",
		},
		{
			name: "kid is required when more than one key",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem},
				{PublicKey: pem},
			},
			wantErr: "ed25519_public_keys[1]: kid is required when more than one key is configured",
		},
		{
			name: "duplicate kid is rejected",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem},
				{KID: "k1", PublicKey: pem},
			},
			wantErr: "ed25519_public_keys[1]: duplicate kid \"k1\"",
		},
		{
			name: "jwks public endpoint conflicts with keys",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem},
			},
			jwksEndpoint: "https://example.com/jwks",
			wantErr:      "ed25519_public_keys can not be used together with jwks_public_endpoint",
		},
		{
			name: "subscription token keys are validated",
			keys: configtypes.Ed25519PublicKeys{
				{KID: "k1", PublicKey: pem},
				{KID: "k1", PublicKey: pem},
			},
			subscriptionTok: true,
			wantErr:         "in client.subscription_token.token: ed25519_public_keys[1]: duplicate kid \"k1\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tt.subscriptionTok {
				cfg.Client.SubscriptionToken.Token.Ed25519PublicKeys = tt.keys
			} else {
				cfg.Client.Token.Ed25519PublicKeys = tt.keys
				cfg.Client.Token.JWKSPublicEndpoint = tt.jwksEndpoint
			}
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
