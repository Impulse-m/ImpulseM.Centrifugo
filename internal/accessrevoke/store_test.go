package accessrevoke

import (
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const (
	testOtherUUID = "0a9b8c7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d"
	testNow       = int64(1000)
)

func newClockStore(clock *int64) *Store {
	store := NewStore(20 * time.Minute)
	store.nowUnix = func() int64 {
		return *clock
	}
	return store
}

func blockUser(userID string, expireAt int64) func(*testing.T, *Store) {
	return func(t *testing.T, store *Store) {
		require.NoError(t, store.BlockUser(userID, expireAt))
	}
}

func revokeSession(sessionID string, revokedAt int64, expireAt int64) func(*testing.T, *Store) {
	return func(t *testing.T, store *Store) {
		require.NoError(t, store.RevokeSession(sessionID, revokedAt, expireAt, nil))
	}
}

func invalidateUser(userID string, minGeneration uint64, issuedBefore int64, revokedAt int64) func(*testing.T, *Store) {
	return func(t *testing.T, store *Store) {
		require.NoError(t, store.InvalidateUser(userID, minGeneration, issuedBefore, revokedAt, 0))
	}
}

func setupAll(setups ...func(*testing.T, *Store)) func(*testing.T, *Store) {
	return func(t *testing.T, store *Store) {
		for _, setup := range setups {
			setup(t, store)
		}
	}
}

func TestStoreBlockExpiresOnRead(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("user-1", 1100))

	result := store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true})
	require.Equal(t, Result{Verdict: Blocked, Reason: ReasonBlocked}, result)

	clock = 1099
	result = store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true})
	require.Equal(t, Blocked, result.Verdict)

	clock = 1100
	result = store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true})
	require.Equal(t, Allowed, result.Verdict)
}

func TestStoreBlockDefaultTTL(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("user-1", 0))

	clock = testNow + int64((20 * time.Minute).Seconds()) - 1
	require.Equal(t, Blocked, store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)

	clock = testNow + int64((20 * time.Minute).Seconds())
	require.Equal(t, Allowed, store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}

func TestStoreBlockKeepsLatestExpiration(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("user-1", 1500))
	require.NoError(t, store.BlockUser("user-1", 1200))

	clock = 1400
	require.Equal(t, Blocked, store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}

func TestStoreUnblockUser(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("user-1", 0))
	require.NoError(t, store.UnblockUser("user-1"))

	require.Equal(t, Allowed, store.Check("user-1", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
	require.NoError(t, store.UnblockUser("user-2"))
}

func TestStoreRevokeSessionKeepsMaximum(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.RevokeSession(testUUID, 950, 2000, nil))
	require.NoError(t, store.RevokeSession(testUUID, 900, 1500, nil))

	// Revoked at 950 is kept, so a token issued at 920 is still revoked.
	result := store.Check("user-1", Identity(testUUID, 0, 920), CheckOptions{Session: true})
	require.Equal(t, Result{Verdict: Revoked, Reason: ReasonSession}, result)

	// Expiration 2000 is kept.
	clock = 1800
	result = store.Check("user-1", Identity(testUUID, 0, 920), CheckOptions{Session: true})
	require.Equal(t, Revoked, result.Verdict)
}

func TestStoreInvalidateUserKeepsMaximum(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.InvalidateUser("user-1", 5, 900, 0, 0))
	require.NoError(t, store.InvalidateUser("user-1", 3, 800, 0, 0))

	// Minimum generation 5 is kept.
	result := store.Check("user-1", Identity("", 4, 1000), CheckOptions{UserTokens: true})
	require.Equal(t, Result{Verdict: Revoked, Reason: ReasonGeneration}, result)

	// Issued-before 900 is kept.
	result = store.Check("user-1", Identity("", 5, 850), CheckOptions{UserTokens: true})
	require.Equal(t, Result{Verdict: Revoked, Reason: ReasonIssuedBefore}, result)
}

func TestStoreApplyIsIdempotent(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	for range 3 {
		require.NoError(t, store.BlockUser("user-1", 1500))
		require.NoError(t, store.RevokeSession(testUUID, 950, 1500, nil))
		require.NoError(t, store.InvalidateUser("user-1", 5, 900, 950, 1500))
	}
	require.Equal(t, Sizes{Blocked: 1, Sessions: 1, Users: 1}, store.Sizes())

	result := store.Check("user-1", Identity("", 4, 940), CheckOptions{Session: true, UserTokens: true})
	require.Equal(t, Result{Verdict: Revoked, Reason: ReasonGeneration}, result)
}

func TestStoreRevokeSessionExpires(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.RevokeSession(testUUID, 950, 1100, nil))

	clock = 1099
	require.Equal(t, Revoked, store.Check("user-1", Identity(testUUID, 0, 900), CheckOptions{Session: true}).Verdict)

	clock = 1100
	require.Equal(t, Allowed, store.Check("user-1", Identity(testUUID, 0, 900), CheckOptions{Session: true}).Verdict)
}

func TestStoreCheck(t *testing.T) {
	allOn := CheckOptions{Block: true, Session: true, UserTokens: true}

	tests := []struct {
		name     string
		setup    func(*testing.T, *Store)
		userID   string
		identity centrifuge.TokenIdentity
		opts     CheckOptions
		expected Result
	}{
		{
			name:     "no entries",
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "blocked user",
			setup:    blockUser("user-1", 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Blocked, Reason: ReasonBlocked},
		},
		{
			name:     "revoked session",
			setup:    revokeSession(testUUID, 950, 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Revoked, Reason: ReasonSession},
		},
		{
			name:     "revoked session equal seconds is revoked",
			setup:    revokeSession(testUUID, 900, 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Revoked, Reason: ReasonSession},
		},
		{
			name:     "revoked session token issued later is loop guard",
			setup:    revokeSession(testUUID, 900, 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 901),
			opts:     allOn,
			expected: Result{Verdict: LoopGuard, Reason: ReasonLoopGuard},
		},
		{
			name:     "other session is not revoked",
			setup:    revokeSession(testOtherUUID, 950, 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "generation below minimum",
			setup:    invalidateUser("user-1", 5, 0, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 4, 900),
			opts:     allOn,
			expected: Result{Verdict: Revoked, Reason: ReasonGeneration},
		},
		{
			name:     "missing generation counts as zero",
			setup:    invalidateUser("user-1", 1, 0, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 0, 900),
			opts:     allOn,
			expected: Result{Verdict: Revoked, Reason: ReasonGeneration},
		},
		{
			name:     "issued before the cut-off",
			setup:    invalidateUser("user-1", 0, 950, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Revoked, Reason: ReasonIssuedBefore},
		},
		{
			name:     "issued at the cut-off is allowed",
			setup:    invalidateUser("user-1", 0, 900, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     allOn,
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "user token issued after revocation is loop guard",
			setup:    invalidateUser("user-1", 5, 0, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 4, 960),
			opts:     allOn,
			expected: Result{Verdict: LoopGuard, Reason: ReasonLoopGuard},
		},
		{
			name:     "empty user skips user checks",
			setup:    setupAll(blockUser("user-1", 2000), invalidateUser("user-1", 5, 0, 950)),
			userID:   "",
			identity: Identity("", 0, 900),
			opts:     allOn,
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "zero session skips session check",
			setup:    revokeSession(testUUID, 950, 2000),
			userID:   "user-1",
			identity: Identity("", 0, 900),
			opts:     allOn,
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "disabled block is not checked",
			setup:    blockUser("user-1", 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     CheckOptions{Session: true, UserTokens: true},
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "disabled session is not checked",
			setup:    revokeSession(testUUID, 950, 2000),
			userID:   "user-1",
			identity: Identity(testUUID, 1, 900),
			opts:     CheckOptions{Block: true, UserTokens: true},
			expected: Result{Verdict: Allowed},
		},
		{
			name:     "disabled user tokens are not checked",
			setup:    invalidateUser("user-1", 5, 0, 950),
			userID:   "user-1",
			identity: Identity(testUUID, 4, 900),
			opts:     CheckOptions{Block: true, Session: true},
			expected: Result{Verdict: Allowed},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := testNow
			store := newClockStore(&clock)
			if tt.setup != nil {
				tt.setup(t, store)
			}
			result := store.Check(tt.userID, tt.identity, tt.opts)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStoreSweepRemovesExpiredEntries(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.NoError(t, store.BlockUser("user-1", 1100))
	require.NoError(t, store.BlockUser("user-2", 1300))
	require.NoError(t, store.RevokeSession(testUUID, 0, 1100, nil))
	require.NoError(t, store.InvalidateUser("user-1", 1, 0, 0, 1100))
	require.Equal(t, Sizes{Blocked: 2, Sessions: 1, Users: 1}, store.Sizes())

	clock = 1200
	store.Sweep()
	require.Equal(t, Sizes{Blocked: 1, Sessions: 0, Users: 0}, store.Sizes())
	require.Equal(t, Blocked, store.Check("user-2", centrifuge.TokenIdentity{}, CheckOptions{Block: true}).Verdict)
}

func TestStoreEmptyIdentifierIsError(t *testing.T) {
	clock := testNow
	store := newClockStore(&clock)
	require.ErrorIs(t, store.BlockUser("", 0), ErrEmptyID)
	require.ErrorIs(t, store.UnblockUser(""), ErrEmptyID)
	require.ErrorIs(t, store.RevokeSession("", 0, 0, nil), ErrEmptyID)
	require.ErrorIs(t, store.InvalidateUser("", 1, 0, 0, 0), ErrEmptyID)
	require.Equal(t, Sizes{}, store.Sizes())
}

func TestStoreConcurrentAccess(t *testing.T) {
	store := NewStore(time.Minute)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = store.BlockUser("user-1", 0)
				_ = store.RevokeSession(testUUID, 0, 0, nil)
				_ = store.InvalidateUser("user-1", 1, 0, 0, 0)
				_ = store.Check("user-1", Identity(testUUID, 0, 1), CheckOptions{Block: true, Session: true, UserTokens: true})
				store.Sweep()
				_ = store.Sizes()
			}
		}()
	}
	wg.Wait()
}
