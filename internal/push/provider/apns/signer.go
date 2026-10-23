package apns

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/asn1"
	"errors"
	"math/big"

	"github.com/cristalhq/jwt/v5"
)

// es256Size is the length of an ES256 signature in JWS form: r and s, 32 bytes each.
const es256Size int = 64

// errSignatureForm is returned when the ECDSA signature of the standard library cannot be read as r and s.
var errSignatureForm error = errors.New("apns: unexpected ECDSA signature form")

// ecdsaSignature is the ASN.1 form of an ECDSA signature as the standard library returns it.
type ecdsaSignature struct {
	R *big.Int
	S *big.Int
}

// deterministicES256 signs ES256 with RFC 6979 deterministic ECDSA. The signer of cristalhq/jwt draws its nonce from a
// random source and so gives a different token for each signing. Nodes that share a key and an issue time must produce
// the same token bytes, so Apple sees one provider token per key and window whatever the node count. The builder of
// cristalhq/jwt is still used for the header and the claims.
type deterministicES256 struct {
	key *ecdsa.PrivateKey
}

// newDeterministicES256 returns the signer of a P-256 key.
func newDeterministicES256(key *ecdsa.PrivateKey) (*deterministicES256, error) {
	if key == nil {
		return nil, errors.New("apns: nil signing key")
	}
	if key.Curve != elliptic.P256() {
		return nil, errors.New("apns: signing key is not P-256")
	}
	return &deterministicES256{
		key: key,
	}, nil
}

// Algorithm returns the algorithm of the signer.
func (s *deterministicES256) Algorithm() jwt.Algorithm {
	return jwt.ES256
}

// SignSize returns the length of a signature in JWS form.
func (s *deterministicES256) SignSize() int {
	return es256Size
}

// Sign returns the ES256 signature of payload in JWS form, r followed by s.
func (s *deterministicES256) Sign(payload []byte) ([]byte, error) {
	digest := sha256.Sum256(payload)

	// A nil random source selects the deterministic nonce of RFC 6979 (Go 1.24 and later).
	der, err := s.key.Sign(nil, digest[:], crypto.SHA256)
	if err != nil {
		return nil, err
	}

	var signature ecdsaSignature
	rest, err := asn1.Unmarshal(der, &signature)
	if err != nil {
		return nil, errSignatureForm
	}
	if len(rest) > 0 {
		return nil, errSignatureForm
	}

	out := make([]byte, es256Size)
	signature.R.FillBytes(out[:es256Size/2])
	signature.S.FillBytes(out[es256Size/2:])
	return out, nil
}
