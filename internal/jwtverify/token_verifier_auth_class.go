package jwtverify

import (
	"fmt"

	"github.com/tidwall/gjson"
)

// authClassFromClaims returns the auth class claim of the token, as configured by AuthClassClaim. An empty
// claim name or an absent claim gives an empty class. A claim which is not a string makes the token invalid.
// The value is not interpreted here.
func (verifier *VerifierJWT) authClassFromClaims(rawClaims []byte) (string, error) {
	if verifier.authClassClaim == "" {
		return "", nil
	}
	value := gjson.GetBytes(rawClaims, verifier.authClassClaim)
	if value.Exists() && value.Type != gjson.String {
		return "", fmt.Errorf("%w: auth class claim must be a string", ErrInvalidToken)
	}
	return value.String(), nil
}
