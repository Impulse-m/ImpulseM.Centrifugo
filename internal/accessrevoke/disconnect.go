package accessrevoke

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// Range of the codes of a revocation disconnect and the size limit of its reason. The codes 4000-4499 let a client
// reconnect, 4500-4999 are terminal. The reason is limited by the reason size of a WebSocket close frame.
const (
	disconnectMinCode       uint32 = 4000
	disconnectMaxCode       uint32 = 4999
	disconnectMaxReasonSize        = 123
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
	var record struct {
		Disconnect json.RawMessage `json:"disconnect"`
	}
	err := json.Unmarshal(body, &record)
	if err != nil || len(record.Disconnect) == 0 {
		return body
	}
	var disconnect Disconnect
	invalid := json.Unmarshal(record.Disconnect, &disconnect)
	if invalid == nil {
		invalid = validateDisconnect(&disconnect)
	}
	if invalid == nil {
		return body
	}
	var fields map[string]json.RawMessage
	err = json.Unmarshal(body, &fields)
	if err != nil {
		return body
	}
	// encoding/json matches keys without case, so every key which the decoder would take as disconnect is removed.
	for key := range fields {
		if strings.EqualFold(key, "disconnect") {
			delete(fields, key)
		}
	}
	sanitized, err := json.Marshal(fields)
	if err != nil {
		return body
	}
	log.Warn().
		Err(invalid).
		Msg("ignoring invalid disconnect of access revocation record, revocation is applied with the default disconnect")
	return sanitized
}

// validateDisconnect returns an error when the disconnect is not a valid application close. An absent disconnect is valid.
func validateDisconnect(disconnect *Disconnect) error {
	if absentDisconnect(disconnect) {
		return nil
	}
	if disconnect.Code < disconnectMinCode || disconnect.Code > disconnectMaxCode {
		return fmt.Errorf("disconnect code %d is not in %d-%d", disconnect.Code, disconnectMinCode, disconnectMaxCode)
	}
	if len(disconnect.Reason) > disconnectMaxReasonSize {
		return fmt.Errorf("disconnect reason is longer than %d bytes", disconnectMaxReasonSize)
	}
	return nil
}

// absentDisconnect reports whether the disconnect is not set: nil, or code 0 with an empty reason.
func absentDisconnect(disconnect *Disconnect) bool {
	return disconnect == nil || (disconnect.Code == 0 && disconnect.Reason == "")
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
