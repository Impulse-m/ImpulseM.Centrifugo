package webpush

import (
	"crypto/ecdsa"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cristalhq/jwt/v5"
)

// VAPID token lifetime and reuse: a token is valid for 12 hours, and a cached token is signed again once it is older than
// an hour. maxVAPIDOrigins bounds the table, so one token per push service origin stays in memory.
const (
	vapidLifetime   = 12 * time.Hour
	vapidResign     = time.Hour
	maxVAPIDOrigins = 256
)

// vapidEntry is the cached VAPID token of one origin. Its lock serialises the signing, so concurrent calls for one origin
// sign once and the others wait for that signature. The eviction takes the lock with TryLock and skips an entry that is
// held. signedAt is atomic, so a read that does not hold the entry lock is still free of a data race.
type vapidEntry struct {
	mu       sync.Mutex
	token    string
	signedAt atomic.Int64
}

// vapidTable holds one VAPID token per origin for the signing key and the subject of the client. It runs no timer and no
// goroutine: the age of a token is read when the token is asked for. It holds at most maxVAPIDOrigins entries. When the
// table is full and every entry is locked, nothing can be evicted, so a token is signed for the new origin and not kept.
type vapidTable struct {
	mu      sync.Mutex
	signer  jwt.Signer
	subject string
	now     func() time.Time
	entries map[string]*vapidEntry
}

// newVAPIDTable returns the table of a P-256 signing key and a subject, with now as the clock.
func newVAPIDTable(key *ecdsa.PrivateKey, subject string, now func() time.Time) (*vapidTable, error) {
	signer, err := jwt.NewSignerES(jwt.ES256, key)
	if err != nil {
		return nil, err
	}
	return &vapidTable{
		signer:  signer,
		subject: subject,
		now:     now,
		entries: make(map[string]*vapidEntry),
	}, nil
}

// token returns the VAPID token of origin. The token is the one cached when it is less than an hour old, else a new one
// is signed and cached. The token carries the origin as its audience, so it cannot be replayed at another push service.
func (t *vapidTable) token(origin string) (string, error) {
	entry, held := t.entryOf(origin)
	if !held {
		entry.mu.Lock()
	}
	defer entry.mu.Unlock()

	now := t.now()
	if entry.token != "" && now.Sub(time.Unix(0, entry.signedAt.Load())) < vapidResign {
		return entry.token, nil
	}

	signed, err := t.sign(origin, now)
	if err != nil {
		return "", err
	}
	entry.token = signed
	entry.signedAt.Store(now.UnixNano())
	return signed, nil
}

// sign returns a new ES256 token for origin, issued at now, with the subject of the table.
func (t *vapidTable) sign(origin string, now time.Time) (string, error) {
	token, err := jwt.NewBuilder(t.signer).Build(jwt.RegisteredClaims{
		Audience:  jwt.Audience{origin},
		ExpiresAt: jwt.NewNumericDate(now.Add(vapidLifetime)),
		Subject:   t.subject,
	})
	if err != nil {
		return "", err
	}
	return token.String(), nil
}

// entryOf returns the entry of origin, and whether the caller already holds its lock. A missing entry is created and locked
// before the table lock is released, so an eviction of another goroutine skips it until its creator has signed. When the
// table is full, a new entry first makes room with evict. When nothing can be evicted, the new entry is locked and returned
// but not stored, so the table never holds more than maxVAPIDOrigins entries. The caller of a held entry unlocks it.
func (t *vapidTable) entryOf(origin string) (*vapidEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if entry, ok := t.entries[origin]; ok {
		return entry, false
	}
	if len(t.entries) >= maxVAPIDOrigins {
		t.evict(t.now())
	}
	entry := &vapidEntry{}
	// The entry is new and not yet shared, so this lock cannot block.
	entry.mu.Lock()
	if len(t.entries) < maxVAPIDOrigins {
		t.entries[origin] = entry
	}
	return entry, true
}

// evict makes room for one entry in a full table. It is one pass over the table. An entry whose lock is held is being
// signed right now, so it is kept. Every other entry that holds a token and is older than an hour is dropped. When none is
// dropped, the entry with the oldest signing time is dropped, and an entry that was never signed counts as the oldest. When
// every entry is held, nothing is dropped, and the caller checks the size of the table after the call. The caller holds the
// table lock.
func (t *vapidTable) evict(now time.Time) {
	dropped := false
	found := false
	var oldestOrigin string
	var oldestAt int64

	for origin, entry := range t.entries {
		if !entry.mu.TryLock() {
			continue
		}
		signedAt := entry.signedAt.Load()
		stale := entry.token != "" && now.Sub(time.Unix(0, signedAt)) > vapidResign
		entry.mu.Unlock()

		if stale {
			delete(t.entries, origin)
			dropped = true
			continue
		}
		if !found || signedAt < oldestAt {
			found = true
			oldestOrigin = origin
			oldestAt = signedAt
		}
	}

	if !dropped && found {
		delete(t.entries, oldestOrigin)
	}
}
