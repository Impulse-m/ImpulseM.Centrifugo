package accessrevoke

import (
	"errors"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
)

// ErrEmptyID is returned when an apply method gets an empty user or session identifier.
var ErrEmptyID = errors.New("empty identifier")

// Verdict is an outcome of a token check.
type Verdict int

const (
	// Allowed means that no revocation applies to the token.
	Allowed Verdict = iota
	// Blocked means that the user is blocked.
	Blocked
	// Revoked means that the token is revoked by session revocation or user token invalidation.
	Revoked
	// LoopGuard means that the token was issued after the revocation it is rejected by. Auth service
	// and Centrifugo contradict each other, so the client must not be asked for a new token.
	LoopGuard
)

// Reason explains why a token check did not allow the token. Used as metric label value.
type Reason string

const (
	ReasonBlocked      Reason = "blocked"
	ReasonSession      Reason = "session"
	ReasonGeneration   Reason = "generation"
	ReasonIssuedBefore Reason = "issued_before"
	ReasonLoopGuard    Reason = "loop_guard"
)

// Result is an outcome of a token check together with its reason. Disconnect is the close of a session revoked with
// one, nil for the default close.
type Result struct {
	Verdict    Verdict
	Reason     Reason
	Disconnect *Disconnect
}

// CheckOptions selects revocation kinds to check. Kinds of disabled features are not checked.
type CheckOptions struct {
	Block      bool
	Session    bool
	UserTokens bool
}

// Sizes contains number of entries in Store by kind.
type Sizes struct {
	Blocked  int
	Sessions int
	Users    int
}

type sessionEntry struct {
	expireAt   int64
	revokedAt  int64
	disconnect *Disconnect
}

type userEntry struct {
	minGeneration uint64
	issuedBefore  int64
	revokedAt     int64
	expireAt      int64
}

// Store keeps revocation entries in memory. Expired entries are ignored on read and removed by
// the expiry timer while the Sweeper runs, at most sweep_interval after they expire (store_expiry.go).
// Apply methods are idempotent: replaying an event does not change the result. Store is safe for
// concurrent use.
type Store struct {
	defaultTTL time.Duration
	nowUnix    func() int64

	blockedMu sync.RWMutex
	blocked   map[[16]byte]int64
	// blockedExpiry is guarded by blockedMu and holds an item for every blocked key.
	blockedExpiry expiryHeap
	// blockedVersion is guarded by blockedMu and counts the writes of the blocked kind.
	blockedVersion uint64

	sessionsMu sync.RWMutex
	sessions   map[[16]byte]sessionEntry
	// sessionsExpiry is guarded by sessionsMu and holds an item for every session.
	sessionsExpiry expiryHeap
	// sessionsVersion is guarded by sessionsMu and counts the writes of the sessions kind.
	sessionsVersion uint64

	usersMu sync.RWMutex
	users   map[[16]byte]userEntry
	// usersExpiry is guarded by usersMu and holds an item for every user.
	usersExpiry expiryHeap
	// usersVersion is guarded by usersMu and counts the writes of the users kind.
	usersVersion uint64

	sessionConns *SessionConnections

	// expiryMu guards the expiry timer fields below.
	expiryMu sync.Mutex
	// expiryOn is set while the Sweeper runs.
	expiryOn bool
	// expiryGap is the shortest time between two runs of the expiry timer.
	expiryGap time.Duration
	// expiryTimer is created by the first arm.
	expiryTimer *time.Timer
	// expiryAt is the time the timer is armed for, zero while it is not armed.
	expiryAt time.Time
	// expiryLast is the start of the previous run.
	expiryLast time.Time
}

// NewStore creates an empty Store. defaultTTL is used for entries which come without expiration time.
func NewStore(defaultTTL time.Duration) *Store {
	return &Store{
		defaultTTL: defaultTTL,
		nowUnix: func() int64 {
			return time.Now().Unix()
		},
		blocked:      make(map[[16]byte]int64),
		sessions:     make(map[[16]byte]sessionEntry),
		users:        make(map[[16]byte]userEntry),
		sessionConns: newSessionConnections(),
	}
}

// SessionConnections returns the index of connections without a user, by token session. The client
// handler keeps it up to date, and session revocation disconnects through it.
func (s *Store) SessionConnections() *SessionConnections {
	return s.sessionConns
}

// BlockUser blocks the user until expireAt (Unix seconds). Zero expireAt means now plus default TTL.
// The stored expiration is the maximum of the old and the new one.
func (s *Store) BlockUser(userID string, expireAt int64) error {
	key, ok := Key(userID)
	if !ok {
		return ErrEmptyID
	}
	now := s.nowUnix()
	expireAt = s.expireAtOrDefault(expireAt, now)

	s.blockedMu.Lock()
	current, existed := s.blocked[key]
	if current <= now {
		current = 0
	}
	stored := max(current, expireAt)
	s.blocked[key] = stored
	s.blockedVersion++
	if !existed {
		s.blockedExpiry.push(expiryItem{expireAt: stored, key: key})
	}
	s.blockedMu.Unlock()
	if !existed {
		s.armExpiry(stored)
	}
	return nil
}

// UnblockUser removes the block of the user. Unblocking a user which is not blocked does nothing. It never rebuilds the
// kind: the item of the block stays in the expiry order until the expiry pass compacts it, so an unblock which arrives
// on a reader goroutine costs one map delete.
func (s *Store) UnblockUser(userID string) error {
	key, ok := Key(userID)
	if !ok {
		return ErrEmptyID
	}
	s.blockedMu.Lock()
	delete(s.blocked, key)
	s.blockedVersion++
	s.blockedMu.Unlock()
	return nil
}

// RevokeSession revokes the session: tokens issued at or before revokedAt (Unix seconds) are rejected.
// Zero revokedAt means now, zero expireAt means now plus default TTL. Repeated calls keep the
// maximum of the stored and the new values. A non-nil disconnect replaces the stored one, and a nil
// one keeps it, so a repeated revocation does not weaken the close of an earlier one.
func (s *Store) RevokeSession(sessionID string, revokedAt int64, expireAt int64, disconnect *Disconnect) error {
	key, ok := Key(sessionID)
	if !ok {
		return ErrEmptyID
	}
	now := s.nowUnix()
	revokedAt = revokedAtOrNow(revokedAt, now)
	expireAt = s.expireAtOrDefault(expireAt, now)

	s.sessionsMu.Lock()
	current, exists := s.sessions[key]
	if !exists || current.expireAt <= now {
		current = sessionEntry{}
	}
	entry := sessionEntry{
		expireAt:   max(current.expireAt, expireAt),
		revokedAt:  max(current.revokedAt, revokedAt),
		disconnect: keptDisconnect(current.disconnect, disconnect),
	}
	s.sessions[key] = entry
	s.sessionsVersion++
	if !exists {
		s.sessionsExpiry.push(expiryItem{expireAt: entry.expireAt, key: key})
	}
	s.sessionsMu.Unlock()
	if !exists {
		s.armExpiry(entry.expireAt)
	}
	return nil
}

// InvalidateUser rejects tokens of the user with generation below minGeneration or issued before
// issuedBefore (Unix seconds). Zero issuedBefore adds no time check. Zero revokedAt means now,
// zero expireAt means now plus default TTL. Repeated calls keep the maximum of every field.
func (s *Store) InvalidateUser(userID string, minGeneration uint64, issuedBefore int64, revokedAt int64, expireAt int64) error {
	key, ok := Key(userID)
	if !ok {
		return ErrEmptyID
	}
	now := s.nowUnix()
	revokedAt = revokedAtOrNow(revokedAt, now)
	expireAt = s.expireAtOrDefault(expireAt, now)

	s.usersMu.Lock()
	current, exists := s.users[key]
	if !exists || current.expireAt <= now {
		current = userEntry{}
	}
	entry := userEntry{
		minGeneration: max(current.minGeneration, minGeneration),
		issuedBefore:  max(current.issuedBefore, issuedBefore),
		revokedAt:     max(current.revokedAt, revokedAt),
		expireAt:      max(current.expireAt, expireAt),
	}
	s.users[key] = entry
	s.usersVersion++
	if !exists {
		s.usersExpiry.push(expiryItem{expireAt: entry.expireAt, key: key})
	}
	s.usersMu.Unlock()
	if !exists {
		s.armExpiry(entry.expireAt)
	}
	return nil
}

// Check evaluates the token identity of the user against revocation entries. Empty userID skips
// user checks, zero identity Session skips session check. Checks of kinds not selected in opts
// are skipped.
func (s *Store) Check(userID string, identity centrifuge.TokenIdentity, opts CheckOptions) Result {
	now := s.nowUnix()
	userKey, hasUser := Key(userID)

	if opts.Block && hasUser && s.isBlocked(userKey, now) {
		return Result{Verdict: Blocked, Reason: ReasonBlocked}
	}
	if opts.Session && identity.Session != ([16]byte{}) {
		if entry, ok := s.session(identity.Session, now); ok {
			return revokedResult(identity.IssuedAt, entry.revokedAt, ReasonSession, entry.disconnect)
		}
	}
	if opts.UserTokens && hasUser {
		if entry, ok := s.user(userKey, now); ok {
			if reason := userTokensReason(identity, entry); reason != "" {
				return revokedResult(identity.IssuedAt, entry.revokedAt, reason, nil)
			}
		}
	}
	return Result{Verdict: Allowed}
}

// Sizes returns number of stored entries by kind, expired entries included until removed.
func (s *Store) Sizes() Sizes {
	s.blockedMu.RLock()
	blocked := len(s.blocked)
	s.blockedMu.RUnlock()

	s.sessionsMu.RLock()
	sessions := len(s.sessions)
	s.sessionsMu.RUnlock()

	s.usersMu.RLock()
	users := len(s.users)
	s.usersMu.RUnlock()

	return Sizes{Blocked: blocked, Sessions: sessions, Users: users}
}

func (s *Store) expireAtOrDefault(expireAt int64, now int64) int64 {
	if expireAt > 0 {
		return expireAt
	}
	return now + int64(s.defaultTTL/time.Second)
}

func (s *Store) isBlocked(key [16]byte, now int64) bool {
	s.blockedMu.RLock()
	defer s.blockedMu.RUnlock()
	expireAt, ok := s.blocked[key]
	return ok && expireAt > now
}

func (s *Store) session(key [16]byte, now int64) (sessionEntry, bool) {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	entry, ok := s.sessions[key]
	return entry, ok && entry.expireAt > now
}

// sessionDisconnect returns the disconnect the active revocation of the session has, or nil.
func (s *Store) sessionDisconnect(key [16]byte) *Disconnect {
	entry, ok := s.session(key, s.nowUnix())
	if !ok {
		return nil
	}
	return entry.disconnect
}

func (s *Store) user(key [16]byte, now int64) (userEntry, bool) {
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	entry, ok := s.users[key]
	return entry, ok && entry.expireAt > now
}

func revokedAtOrNow(revokedAt int64, now int64) int64 {
	if revokedAt > 0 {
		return revokedAt
	}
	return now
}

// userTokensReason returns why the token identity is rejected by the user entry, or empty Reason.
func userTokensReason(identity centrifuge.TokenIdentity, entry userEntry) Reason {
	if identity.Generation < entry.minGeneration {
		return ReasonGeneration
	}
	if identity.IssuedAt < entry.issuedBefore {
		return ReasonIssuedBefore
	}
	return ""
}

// revokedResult returns Revoked, or LoopGuard when the token was issued after the revocation.
// Equal seconds count as revoked. The disconnect is kept only for Revoked.
func revokedResult(issuedAt int64, revokedAt int64, reason Reason, disconnect *Disconnect) Result {
	if issuedAt > revokedAt {
		return Result{Verdict: LoopGuard, Reason: ReasonLoopGuard}
	}
	return Result{Verdict: Revoked, Reason: reason, Disconnect: disconnect}
}

// keptDisconnect returns the disconnect a session keeps after a revocation with next: a copy of next when it is set,
// otherwise the stored one.
func keptDisconnect(stored *Disconnect, next *Disconnect) *Disconnect {
	if absentDisconnect(next) {
		return stored
	}
	copied := *next
	return &copied
}
