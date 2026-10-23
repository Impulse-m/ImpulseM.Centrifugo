package confighelpers

import (
	"crypto/ed25519"
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/jwtutils"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
)

func MakeVerifierConfig(tokenConf configtypes.Token) (jwtverify.VerifierConfig, error) {
	cfg := jwtverify.VerifierConfig{}

	cfg.HMACSecretKey = tokenConf.HMACSecretKey
	cfg.HMACPreviousSecretKey = tokenConf.HMACPreviousSecretKey
	cfg.HMACPreviousSecretKeyValidUntil = tokenConf.HMACPreviousSecretKeyValidUntil

	rsaPublicKey := tokenConf.RSAPublicKey
	if rsaPublicKey != "" {
		pubKey, err := jwtutils.ParseRSAPublicKeyFromPEM([]byte(rsaPublicKey))
		if err != nil {
			return jwtverify.VerifierConfig{}, fmt.Errorf("error parsing RSA public key: %w", err)
		}
		cfg.RSAPublicKey = pubKey
	}

	ecdsaPublicKey := tokenConf.ECDSAPublicKey
	if ecdsaPublicKey != "" {
		pubKey, err := jwtutils.ParseECDSAPublicKeyFromPEM([]byte(ecdsaPublicKey))
		if err != nil {
			return jwtverify.VerifierConfig{}, fmt.Errorf("error parsing ECDSA public key: %w", err)
		}
		cfg.ECDSAPublicKey = pubKey
	}

	if len(tokenConf.Ed25519PublicKeys) > 0 {
		cfg.Ed25519PublicKeys = make(map[string]ed25519.PublicKey, len(tokenConf.Ed25519PublicKeys))
	}
	for i, key := range tokenConf.Ed25519PublicKeys {
		if _, ok := cfg.Ed25519PublicKeys[key.KID]; ok {
			return jwtverify.VerifierConfig{}, fmt.Errorf("duplicate Ed25519 public key kid %q at index %d", key.KID, i)
		}
		pubKey, err := jwtutils.ParseEd25519PublicKeyFromPEM([]byte(key.PublicKey))
		if err != nil {
			return jwtverify.VerifierConfig{}, fmt.Errorf("error parsing Ed25519 public key at index %d (kid %q): %w", i, key.KID, err)
		}
		cfg.Ed25519PublicKeys[key.KID] = pubKey
	}

	ed25519PublicKey := tokenConf.Ed25519PublicKey
	if ed25519PublicKey != "" {
		pubKey, err := jwtutils.ParseEd25519PublicKeyFromPEM([]byte(ed25519PublicKey))
		if err != nil {
			return jwtverify.VerifierConfig{}, fmt.Errorf("error parsing Ed25519 public key: %w", err)
		}
		cfg.Ed25519PublicKey = pubKey
	}

	cfg.JWKSPublicEndpoint = tokenConf.JWKSPublicEndpoint
	cfg.Audience = tokenConf.Audience
	cfg.AudienceRegex = tokenConf.AudienceRegex
	cfg.Issuer = tokenConf.Issuer
	cfg.IssuerRegex = tokenConf.IssuerRegex
	cfg.InsecureSkipJWKSEndpointSafetyCheck = tokenConf.InsecureSkipJWKSEndpointSafetyCheck

	if tokenConf.UserIDClaim != "" {
		cfg.UserIDClaim = tokenConf.UserIDClaim
	}

	return cfg, nil
}

// SetAuthClassClaim sets the claim which carries the auth class on a connection token verifier config
// made by MakeVerifierConfig. The claim is set only when the RPC registry is enabled. Call it both at
// startup and on reload.
func SetAuthClassClaim(verifierConfig *jwtverify.VerifierConfig, registry configtypes.RPCRegistry) {
	if !registry.Enabled {
		return
	}
	verifierConfig.AuthClassClaim = registry.AuthClassClaim
}

// SetRevokeClaims sets claims used for access revocation checks on a verifier config made by
// MakeVerifierConfig. Call it both at startup and on reload.
func SetRevokeClaims(
	verifierConfig *jwtverify.VerifierConfig,
	tokenRevoke configtypes.TokenRevoke,
	userTokensInvalidate configtypes.UserTokensInvalidate,
) {
	verifierConfig.SessionClaim = tokenRevoke.UIDClaim
	verifierConfig.GenerationClaim = userTokensInvalidate.GenerationClaim
}
