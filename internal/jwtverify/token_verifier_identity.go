package jwtverify

import (
	"github.com/cristalhq/jwt/v5"
	"github.com/tidwall/gjson"
)

const defaultSessionClaim = "jti"

func sessionClaimOrDefault(claim string) string {
	if claim == "" {
		return defaultSessionClaim
	}
	return claim
}

// sessionFromClaims returns session ID of the token. The "jti" claim is taken from registered
// claims, any other claim is read from raw claims.
func (verifier *VerifierJWT) sessionFromClaims(rawClaims []byte, registered jwt.RegisteredClaims) string {
	if verifier.sessionClaim == defaultSessionClaim {
		return registered.ID
	}
	return gjson.GetBytes(rawClaims, verifier.sessionClaim).String()
}

// generationFromClaims returns revocation generation of the token. Non-numeric or negative
// values and absent claim give zero generation.
func (verifier *VerifierJWT) generationFromClaims(rawClaims []byte) uint64 {
	if verifier.generationClaim == "" {
		return 0
	}
	value := gjson.GetBytes(rawClaims, verifier.generationClaim)
	if value.Type != gjson.Number || value.Int() < 0 {
		return 0
	}
	return uint64(value.Int())
}
