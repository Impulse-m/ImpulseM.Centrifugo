package accessrevoke

import (
	"encoding/json"
	"errors"
)

// NotifyOp is an operation name of access revocation notifications sent to all nodes.
const NotifyOp = "access_revoke"

// Kinds of access revocation operations carried by Notification.
const (
	KindBlockUser            = "block_user"
	KindUnblockUser          = "unblock_user"
	KindRevokeToken          = "revoke_token"
	KindInvalidateUserTokens = "invalidate_user_tokens"
)

// ErrUnknownKind is returned when a Notification has a kind which is not known.
var ErrUnknownKind = errors.New("unknown notification kind")

// Notification is a resolved access revocation operation. Zero times must be replaced with concrete
// values before a Notification is applied or sent, so all nodes apply the same times.
type Notification struct {
	Kind         string `json:"kind"`
	User         string `json:"user,omitempty"`
	Session      string `json:"session,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
	IssuedBefore int64  `json:"issued_before,omitempty"`
	RevokedAt    int64  `json:"revoked_at,omitempty"`
	ExpireAt     int64  `json:"expire_at,omitempty"`
}

// EncodeNotification encodes Notification into the payload of NotifyOp.
func EncodeNotification(notification Notification) ([]byte, error) {
	return json.Marshal(notification)
}

// DecodeNotification decodes the payload of NotifyOp into Notification.
func DecodeNotification(data []byte) (Notification, error) {
	var notification Notification
	err := json.Unmarshal(data, &notification)
	return notification, err
}
