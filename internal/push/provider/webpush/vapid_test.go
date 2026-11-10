package webpush

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristalhq/jwt/v5"
	"github.com/stretchr/testify/require"
)

// countingSigner counts the signatures of the table, and otherwise signs with the signer it wraps.
type countingSigner struct {
	jwt.Signer
	signs *atomic.Int64
}

// Sign counts the call and signs the payload.
func (s countingSigner) Sign(payload []byte) ([]byte, error) {
	s.signs.Add(1)
	return s.Signer.Sign(payload)
}

// newTestTable returns the VAPID table of a new key, with the clock that now returns.
func newTestTable(t *testing.T, now func() time.Time) (*vapidTable, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	table, err := newVAPIDTable(key, "mailto:push@example.com", now)
	require.NoError(t, err)
	return table, key
}

func TestVAPIDTokenClaimsAndSignature(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	table, key := newTestTable(t, func() time.Time { return now })

	token, err := table.token("https://fcm.googleapis.com")
	require.NoError(t, err)

	verifier, err := jwt.NewVerifierES(jwt.ES256, &key.PublicKey)
	require.NoError(t, err)
	var claims jwt.RegisteredClaims
	require.NoError(t, jwt.ParseClaims([]byte(token), verifier, &claims), "the signature must verify with the public key")

	require.Equal(t, jwt.Audience{"https://fcm.googleapis.com"}, claims.Audience)
	require.Equal(t, "mailto:push@example.com", claims.Subject)
	require.Equal(t, now.Add(12*time.Hour).Unix(), claims.ExpiresAt.Unix())
}

func TestVAPIDSignsOncePerOriginUnderConcurrency(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	table, _ := newTestTable(t, func() time.Time { return now })
	var signs atomic.Int64
	table.signer = countingSigner{Signer: table.signer, signs: &signs}

	const callers = 50
	tokens := make([]string, callers)
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			token, err := table.token("https://fcm.googleapis.com")
			if err == nil {
				tokens[index] = token
			}
		}(index)
	}
	wg.Wait()

	require.Equal(t, int64(1), signs.Load(), "one signature for one origin")
	require.NotEmpty(t, tokens[0])
	for _, token := range tokens {
		require.Equal(t, tokens[0], token)
	}
}

func TestVAPIDSignsAgainAfterAnHour(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	current := base
	table, _ := newTestTable(t, func() time.Time { return current })
	var signs atomic.Int64
	table.signer = countingSigner{Signer: table.signer, signs: &signs}

	first, err := table.token("https://fcm.googleapis.com")
	require.NoError(t, err)

	current = base.Add(59 * time.Minute)
	second, err := table.token("https://fcm.googleapis.com")
	require.NoError(t, err)
	require.Equal(t, first, second, "a token under an hour old is reused")
	require.Equal(t, int64(1), signs.Load())

	current = base.Add(61 * time.Minute)
	third, err := table.token("https://fcm.googleapis.com")
	require.NoError(t, err)
	require.NotEqual(t, first, third, "a token over an hour old is signed again")
	require.Equal(t, int64(2), signs.Load())
}

func TestVAPIDTokensAreKeptPerOrigin(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	table, _ := newTestTable(t, func() time.Time { return now })

	first, err := table.token("https://fcm.googleapis.com")
	require.NoError(t, err)
	second, err := table.token("https://web.push.apple.com")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	require.Len(t, table.entries, 2)
}

func TestVAPIDTableIsBounded(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	current := base
	table, _ := newTestTable(t, func() time.Time { return current })

	// Each origin is one second newer than the one before, so the oldest entry is the first one.
	const origins = maxVAPIDOrigins + 44
	for index := 0; index < origins; index++ {
		current = base.Add(time.Duration(index) * time.Second)
		_, err := table.token(fmt.Sprintf("https://push%d.example.com", index))
		require.NoError(t, err)
	}

	require.Len(t, table.entries, maxVAPIDOrigins)
	require.NotContains(t, table.entries, "https://push0.example.com", "the oldest entry is dropped when the table is full")
	require.Contains(t, table.entries, fmt.Sprintf("https://push%d.example.com", origins-1))
}

// TestEvictKeepsAnEntryThatIsBeingSigned checks that an entry whose lock is held, as it is while its token is signed, survives
// an eviction of a full table, even when it is older than an hour.
func TestEvictKeepsAnEntryThatIsBeingSigned(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	current := base
	table, _ := newTestTable(t, func() time.Time { return current })

	for index := 0; index < maxVAPIDOrigins; index++ {
		_, err := table.token(fmt.Sprintf("https://push%d.example.com", index))
		require.NoError(t, err)
	}
	held := table.entries["https://push0.example.com"]
	held.mu.Lock()
	defer held.mu.Unlock()

	current = base.Add(2 * time.Hour)
	_, err := table.token("https://late.example.com")
	require.NoError(t, err)

	require.Contains(t, table.entries, "https://push0.example.com", "an entry that is being signed is kept")
	require.Contains(t, table.entries, "https://late.example.com")
	require.Len(t, table.entries, 2, "every other entry older than an hour is dropped")
}

func TestVAPIDTableDropsEntriesOlderThanAnHourBeforeTheOldest(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	current := base
	table, _ := newTestTable(t, func() time.Time { return current })

	for index := 0; index < maxVAPIDOrigins; index++ {
		_, err := table.token(fmt.Sprintf("https://push%d.example.com", index))
		require.NoError(t, err)
	}

	current = base.Add(2 * time.Hour)
	_, err := table.token("https://late.example.com")
	require.NoError(t, err)

	require.Len(t, table.entries, 1, "every entry older than an hour is dropped, and only the new one is kept")
	require.Contains(t, table.entries, "https://late.example.com")
}
