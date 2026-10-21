package api

import (
	"encoding/json"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/closecode"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// Ranges of the custom unsubscribe codes. Codes 2100-2499 leave a client unsubscribed, codes 2600-2999 make it
// subscribe again. The reason is limited to the reason size of a WebSocket close.
const (
	unsubscribeTerminalMinCode uint32 = 2100
	unsubscribeTerminalMaxCode uint32 = 2499
	unsubscribeResubMinCode    uint32 = 2600
	unsubscribeResubMaxCode    uint32 = 2999
)

// absentUnsubscribe reports whether the request has no custom unsubscribe: no unsubscribe at all, or code 0
// with an empty reason.
func absentUnsubscribe(unsubscribe *Unsubscribe) bool {
	return unsubscribe == nil || closecode.Absent(unsubscribe.Code, unsubscribe.Reason)
}

// validUnsubscribe returns an error when the custom unsubscribe is not one an application may send. An absent
// unsubscribe is valid.
func validUnsubscribe(unsubscribe *Unsubscribe) error {
	if absentUnsubscribe(unsubscribe) {
		return nil
	}
	return closecode.Validate(
		unsubscribe.Code,
		unsubscribe.Reason,
		closecode.Range{Min: unsubscribeTerminalMinCode, Max: unsubscribeTerminalMaxCode},
		closecode.Range{Min: unsubscribeResubMinCode, Max: unsubscribeResubMaxCode},
	)
}

// unsubscribeOptions returns the options of a server unsubscribe request. A custom unsubscribe of the request is
// applied with its code and reason, and nil means the default server unsubscribe. An invalid custom unsubscribe is
// returned as an error, so the request is refused.
func unsubscribeOptions(cmd *UnsubscribeRequest) ([]centrifuge.UnsubscribeOption, error) {
	opts := []centrifuge.UnsubscribeOption{centrifuge.WithUnsubscribeClient(cmd.Client), centrifuge.WithUnsubscribeSession(cmd.Session)}
	if absentUnsubscribe(cmd.Unsubscribe) {
		return opts, nil
	}
	err := validUnsubscribe(cmd.Unsubscribe)
	if err != nil {
		return nil, err
	}
	opts = append(opts, centrifuge.WithCustomUnsubscribe(centrifuge.Unsubscribe{
		Code:   cmd.Unsubscribe.Code,
		Reason: cmd.Unsubscribe.Reason,
	}))
	return opts, nil
}

// sanitizeUnsubscribeBody returns the body of an unsubscribe consumer record without its unsubscribe when the
// unsubscribe is invalid, so the record is still applied with the default unsubscribe. An invalid unsubscribe is
// logged. A body which is not a JSON object is returned as it is: the request decoder rejects it.
func sanitizeUnsubscribeBody(body []byte) []byte {
	sanitized, invalid := closecode.SanitizeBody(body, "unsubscribe", validateUnsubscribeValue)
	if invalid != nil {
		log.Warn().
			Err(invalid).
			Msg("ignoring invalid unsubscribe of a consumer unsubscribe command, it is applied with the default code")
	}
	return sanitized
}

// validateUnsubscribeValue returns an error when the raw unsubscribe of a consumer record is not a valid custom
// unsubscribe.
func validateUnsubscribeValue(value json.RawMessage) error {
	var unsubscribe Unsubscribe
	err := json.Unmarshal(value, &unsubscribe)
	if err != nil {
		return err
	}
	return validUnsubscribe(&unsubscribe)
}
