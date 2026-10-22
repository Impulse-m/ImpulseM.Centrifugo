package apns

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

// testWindow is the Unix time of a window boundary, a multiple of the window length.
var testWindow = time.Unix(2400*1000000, 0)

// fakeClock is a clock set by the test.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// countingSigner counts the signings of a credential. It is a test hook: production code does not use it.
type countingSigner struct {
	jwt.Signer
	signs atomic.Int64
}

func (s *countingSigner) Sign(payload []byte) ([]byte, error) {
	s.signs.Add(1)
	return s.Signer.Sign(payload)
}

func newTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

// newTestCredential returns a credential on the fake clock, with its signings counted.
func newTestCredential(t *testing.T, key *ecdsa.PrivateKey, clock *fakeClock) (*Credential, *countingSigner) {
	t.Helper()

	credential, err := NewCredential(key, "KEYID1234", "TEAMID1234")
	require.NoError(t, err)

	counter := &countingSigner{Signer: credential.signer}
	credential.signer = counter
	credential.now = clock.Now
	return credential, counter
}

// issuedAtOf returns the iat claim of a provider token.
func issuedAtOf(t *testing.T, token string) int64 {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims struct {
		IssuedAt int64 `json:"iat"`
	}
	require.NoError(t, json.Unmarshal(payload, &claims))
	return claims.IssuedAt
}

func TestWindowStartBoundary(t *testing.T) {
	boundary := testWindow.Unix()

	// Within the lag after the boundary the previous window still applies.
	require.Equal(t, boundary-2400, windowStart(time.Unix(boundary+59, 0)))
	require.Equal(t, boundary, windowStart(time.Unix(boundary+60, 0)))
	require.Equal(t, boundary, windowStart(time.Unix(boundary+2399, 0)))
	require.Equal(t, boundary+2400, windowStart(time.Unix(boundary+2400+60, 0)))
}

func TestBearerIATChangesOnlyAfterLagPastBoundary(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+59, 0)}
	credential, _ := newTestCredential(t, newTestKey(t), clock)

	token, iat, err := credential.Current()
	require.NoError(t, err)
	require.Equal(t, boundary-2400, iat)
	require.Equal(t, iat, issuedAtOf(t, token))

	clock.Set(time.Unix(boundary+60, 0))
	_, iat, err = credential.Current()
	require.NoError(t, err)
	require.Equal(t, boundary, iat)
}

func TestBearerTwoNodesShareIATInsideWindow(t *testing.T) {
	boundary := testWindow.Unix()
	key := newTestKey(t)
	clock := &fakeClock{now: time.Unix(boundary+500, 0)}

	first, _ := newTestCredential(t, key, clock)
	second, _ := newTestCredential(t, key, clock)

	_, firstIAT, err := first.Current()
	require.NoError(t, err)
	_, secondIAT, err := second.Current()
	require.NoError(t, err)

	require.Equal(t, firstIAT, secondIAT)
}

func TestBearerSignsOncePerWindow(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+100, 0)}
	credential, counter := newTestCredential(t, newTestKey(t), clock)

	for index := 0; index < 50; index++ {
		_, _, err := credential.Current()
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), counter.signs.Load())

	clock.Set(time.Unix(boundary+2400+100, 0))
	_, _, err := credential.Current()
	require.NoError(t, err)
	require.Equal(t, int64(2), counter.signs.Load())
}

func TestBearerKeyIDHeader(t *testing.T) {
	clock := &fakeClock{now: testWindow}
	credential, _ := newTestCredential(t, newTestKey(t), clock)

	token, _, err := credential.Current()
	require.NoError(t, err)

	header, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
	require.NoError(t, err)
	require.Contains(t, string(header), `"kid":"KEYID1234"`)
}
