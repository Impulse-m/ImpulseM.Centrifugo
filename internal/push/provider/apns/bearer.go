package apns

import (
	"crypto/ecdsa"
	"sync"
	"time"

	"github.com/cristalhq/jwt/v5"
)

// Apple accepts a provider token issued within the last 60 minutes and limits token updates to one per 20 minutes.
// Every node signs its bearers with the same issue time, so the updates of one signing key stay within the limit
// whatever the node count: the issue time is the start of the current window of windowLength on the Unix clock, and
// a node moves to a new window bearerLag after the boundary, so the issue time is never in the future by normal clock
// skew. A token is at most windowLength plus bearerLag old.
const (
	windowLength = 40 * time.Minute
	bearerLag    = 60 * time.Second
)

// bearerClaims are the claims of an APNs provider token.
type bearerClaims struct {
	Issuer   string `json:"iss"`
	IssuedAt int64  `json:"iat"`
}

// Credential is the signing key of an APNs credential. The environments of the credential share it: the key id and
// the team id are the same for production and development, and the environment chooses only the host.
type Credential struct {
	bearer *bearer
}

// NewCredential returns the credential of a P-256 signing key with keyID and teamID.
func NewCredential(key *ecdsa.PrivateKey, keyID string, teamID string) (*Credential, error) {
	signer, err := jwt.NewSignerES(jwt.ES256, key)
	if err != nil {
		return nil, err
	}

	return &Credential{
		bearer: &bearer{
			keyID:  keyID,
			teamID: teamID,
			signer: signer,
			now:    time.Now,
		},
	}, nil
}

// bearer holds the provider token of a credential. The token is signed under the lock and cached until the window
// changes, so every call of a window uses one token.
type bearer struct {
	mu sync.Mutex

	keyID  string
	teamID string
	signer jwt.Signer
	now    func() time.Time

	// token is the cached provider token, issuedAt its issue time in Unix seconds.
	token    string
	issuedAt int64
}

// windowStart returns the issue time of the window that now is in, in Unix seconds.
func windowStart(now time.Time) int64 {
	length := int64(windowLength / time.Second)
	lagged := now.Add(-bearerLag).Unix()
	return lagged - lagged%length
}

// Window returns the issue time of the current window, in Unix seconds.
func (b *bearer) Window() int64 {
	return windowStart(b.now())
}

// Current returns the provider token of the current window and its issue time. A token is signed only when the
// window has changed since the cached one, so one credential signs at most once per window on a node.
func (b *bearer) Current() (string, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	issuedAt := windowStart(b.now())
	if b.token != "" && b.issuedAt == issuedAt {
		return b.token, issuedAt, nil
	}

	token, err := jwt.NewBuilder(b.signer, jwt.WithKeyID(b.keyID)).Build(bearerClaims{
		Issuer:   b.teamID,
		IssuedAt: issuedAt,
	})
	if err != nil {
		return "", 0, err
	}

	b.token = token.String()
	b.issuedAt = issuedAt
	return b.token, issuedAt, nil
}
