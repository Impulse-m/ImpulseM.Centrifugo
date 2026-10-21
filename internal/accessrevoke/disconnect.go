package accessrevoke

import (
	"encoding/json"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/closecode"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// Range of the codes of a revocation disconnect. The codes 4000-4499 let a client reconnect, 4500-4999 are terminal.
const (
	disconnectMinCode uint32 = 4000
	disconnectMaxCode uint32 = 4999
)

// Disconnect is the close code and reason of the connections a revocation disconnects. It is carried by Operation and
// Notification, so it has JSON tags. Nil means the default close of the revocation.
type Disconnect struct {
	Code   uint32 `json:"code"`
	Reason string `json:"reason,omitempty"`
}

// FromAPIDisconnect returns the Disconnect of a revoke token request, or nil when the request has none.
func FromAPIDisconnect(disconnect *apiproto.Disconnect) *Disconnect {
	if disconnect == nil {
		return nil
	}
	return &Disconnect{
		Code:   disconnect.Code,
		Reason: disconnect.Reason,
	}
}

// SanitizeRevokeTokenBody returns the body of a revoke_token source record without its disconnect when the disconnect
// is invalid, so the record is still applied with the default close of the revocation. An invalid disconnect is logged.
// A body which is not a JSON object is returned as it is: the request decoder rejects it.
func SanitizeRevokeTokenBody(body []byte) []byte {
	sanitized, invalid := closecode.SanitizeBody(body, "disconnect", validateDisconnectValue)
	if invalid != nil {
		log.Warn().
			Err(invalid).
			Msg("ignoring invalid disconnect of access revocation record, revocation is applied with the default disconnect")
	}
	return sanitized
}

// validateDisconnectValue returns an error when the raw disconnect of a revoke_token record is not a valid disconnect.
func validateDisconnectValue(value json.RawMessage) error {
	var disconnect Disconnect
	err := json.Unmarshal(value, &disconnect)
	if err != nil {
		return err
	}
	return validateDisconnect(&disconnect)
}

// validateDisconnect returns an error when the disconnect is not a valid application close. An absent disconnect is valid.
func validateDisconnect(disconnect *Disconnect) error {
	if absentDisconnect(disconnect) {
		return nil
	}
	return closecode.Validate(
		disconnect.Code,
		disconnect.Reason,
		closecode.Range{Min: disconnectMinCode, Max: disconnectMaxCode},
	)
}

// absentDisconnect reports whether the disconnect is not set: nil, or code 0 with an empty reason.
func absentDisconnect(disconnect *Disconnect) bool {
	return disconnect == nil || closecode.Absent(disconnect.Code, disconnect.Reason)
}

// presentDisconnect returns the disconnect, or nil when it is absent, so an absent one is not stored or sent.
func presentDisconnect(disconnect *Disconnect) *Disconnect {
	if absentDisconnect(disconnect) {
		return nil
	}
	return disconnect
}

// closeWith returns the close of a revocation disconnect, or fallback when the disconnect is nil.
func closeWith(disconnect *Disconnect, fallback centrifuge.Disconnect) centrifuge.Disconnect {
	if disconnect == nil {
		return fallback
	}
	return centrifuge.Disconnect{
		Code:   disconnect.Code,
		Reason: disconnect.Reason,
	}
}
