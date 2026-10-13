package jwtverify

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/cristalhq/jwt/v5"
	"github.com/rs/zerolog/log"
)

// errUnknownEd25519KeyID is returned when an EdDSA token selects a key which is not configured.
var errUnknownEd25519KeyID = errors.New("unknown Ed25519 key ID")

// verifyEd25519 selects Ed25519 key by token kid header and verifies the token with it.
// Token without kid is verified only when exactly one Ed25519 key is configured.
func (s *algorithms) verifyEd25519(token *jwt.Token) error {
	if len(s.Ed25519) == 0 {
		return fmt.Errorf("%w: %s", errDisabledAlgorithm, string(token.Header().Algorithm))
	}
	var verifier jwt.Verifier
	kid := token.Header().KeyID
	if kid == "" {
		if len(s.Ed25519) != 1 {
			return fmt.Errorf("%w: token without kid and %d Ed25519 keys configured", errUnknownEd25519KeyID, len(s.Ed25519))
		}
		for _, v := range s.Ed25519 {
			verifier = v
		}
	} else {
		verifier = s.Ed25519[kid]
		if verifier == nil {
			return fmt.Errorf("%w: %q", errUnknownEd25519KeyID, kid)
		}
	}
	return verifier.Verify(token)
}

// setEd25519Keys creates Ed25519 verifiers for the given keys, keyed by kid.
func (s *algorithms) setEd25519Keys(ed25519PubKeys map[string]ed25519.PublicKey) error {
	if len(ed25519PubKeys) == 0 {
		s.Ed25519 = nil
		return nil
	}
	ed25519Verifiers := make(map[string]jwt.Verifier, len(ed25519PubKeys))
	for kid, pubKey := range ed25519PubKeys {
		verifierEdDSA, err := jwt.NewVerifierEdDSA(pubKey)
		if err != nil {
			return fmt.Errorf("error initializing Ed25519 verifier for kid %q: %w", kid, err)
		}
		ed25519Verifiers[kid] = verifierEdDSA
	}
	s.Ed25519 = ed25519Verifiers
	log.Info().Int("keys", len(ed25519PubKeys)).Msg("enabled EdDSA verifiers")
	return nil
}
