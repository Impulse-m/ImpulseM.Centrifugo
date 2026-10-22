package accessrevoke

import (
	"fmt"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// Operation is an access revocation request with plain fields. It is built from API requests and
// from records of access revocation sources. Zero values mean "not set".
type Operation struct {
	Kind         string
	User         string
	Session      string
	Generation   uint64
	IssuedBefore int64
	Channel      string
	RevokedAt    int64
	ExpireAt     int64
	// Disconnect is the close of a revoke_token. Other kinds ignore it.
	Disconnect *Disconnect
}

// ValidationError is returned by Resolve when an Operation can't be turned into a Notification.
type ValidationError struct {
	// Message describes the problem.
	Message string
	// NotSupported is true when the operation is well formed, but this server does not support it.
	NotSupported bool
}

// Error implements error.
func (e *ValidationError) Error() string {
	return e.Message
}

// Resolve turns an Operation into a Notification with concrete times. Zero times are replaced using
// now: revoked_at defaults to now, expire_at defaults to now plus defaultTTL, and issued_before
// defaults to now only when neither generation nor issued_before is set. Callers pass the record
// timestamp as now when resolving events from sources, so all nodes get the same values.
func Resolve(operation Operation, now time.Time, defaultTTL time.Duration) (Notification, error) {
	switch operation.Kind {
	case KindBlockUser:
		if operation.User == "" {
			return Notification{}, &ValidationError{Message: "user required for block_user"}
		}
		return Notification{
			Kind:     KindBlockUser,
			User:     operation.User,
			ExpireAt: resolveExpireAt(operation.ExpireAt, now, defaultTTL),
		}, nil
	case KindUnblockUser:
		if operation.User == "" {
			return Notification{}, &ValidationError{Message: "user required for unblock_user"}
		}
		return Notification{
			Kind: KindUnblockUser,
			User: operation.User,
		}, nil
	case KindRevokeToken:
		if operation.Session == "" {
			return Notification{}, &ValidationError{Message: "uid required for revoke_token"}
		}
		err := validateDisconnect(operation.Disconnect)
		if err != nil {
			return Notification{}, &ValidationError{Message: err.Error()}
		}
		return Notification{
			Kind:       KindRevokeToken,
			User:       operation.User,
			Session:    operation.Session,
			RevokedAt:  resolveRevokedAt(operation.RevokedAt, now),
			ExpireAt:   resolveExpireAt(operation.ExpireAt, now, defaultTTL),
			Disconnect: presentDisconnect(operation.Disconnect),
		}, nil
	case KindInvalidateUserTokens:
		if operation.Channel != "" {
			// Per-channel subscription token invalidation is not supported yet.
			return Notification{}, &ValidationError{
				Message:      "channel is not supported for invalidate_user_tokens",
				NotSupported: true,
			}
		}
		if operation.User == "" {
			return Notification{}, &ValidationError{Message: "user required for invalidate_user_tokens"}
		}
		// When generation is set, issued_before is used only if given: defaulting it to now would reject
		// legitimate tokens of the new generation on nodes with lagging clocks.
		issuedBefore := operation.IssuedBefore
		if operation.Generation == 0 && issuedBefore == 0 {
			issuedBefore = now.Unix()
		}
		return Notification{
			Kind:         KindInvalidateUserTokens,
			User:         operation.User,
			Generation:   operation.Generation,
			IssuedBefore: issuedBefore,
			RevokedAt:    resolveRevokedAt(operation.RevokedAt, now),
			ExpireAt:     resolveExpireAt(operation.ExpireAt, now, defaultTTL),
		}, nil
	default:
		return Notification{}, fmt.Errorf("%w: %q", ErrUnknownKind, operation.Kind)
	}
}

// ExceedsMaxTTL reports whether expireAt is later than now plus maxTTL. A zero or negative maxTTL means no limit, and
// so does a zero expireAt, which means not set.
func ExceedsMaxTTL(expireAt int64, now time.Time, maxTTL time.Duration) bool {
	return maxTTL > 0 && expireAt != 0 && expireAt > now.Add(maxTTL).Unix()
}

// ClampExpireAt returns the notification with expire_at not later than now plus maxTTL, and counts the clamp under source.
// A revocation is clamped, not dropped.
func ClampExpireAt(notification Notification, now time.Time, maxTTL time.Duration, source string) Notification {
	if !ExceedsMaxTTL(notification.ExpireAt, now, maxTTL) {
		return notification
	}
	metrics.AccessRevokeExpireAtClampedTotal.WithLabelValues(source).Inc()
	notification.ExpireAt = now.Add(maxTTL).Unix()
	return notification
}

// resolveRevokedAt returns revokedAt or Unix time of now when it is zero.
func resolveRevokedAt(revokedAt int64, now time.Time) int64 {
	if revokedAt != 0 {
		return revokedAt
	}
	return now.Unix()
}

// resolveExpireAt returns expireAt or Unix time of now plus defaultTTL when it is zero.
func resolveExpireAt(expireAt int64, now time.Time, defaultTTL time.Duration) int64 {
	if expireAt != 0 {
		return expireAt
	}
	return now.Add(defaultTTL).Unix()
}
