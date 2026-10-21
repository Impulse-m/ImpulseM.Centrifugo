package api

import (
	"encoding/json"
	"errors"
	"strings"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// Ranges of the custom unsubscribe codes. Codes 2100-2499 leave a client unsubscribed, codes 2600-2999
// make it subscribe again. The reason is limited to the reason size of a WebSocket close.
const (
	unsubscribeTerminalMinCode uint32 = 2100
	unsubscribeTerminalMaxCode uint32 = 2499
	unsubscribeResubMinCode    uint32 = 2600
	unsubscribeResubMaxCode    uint32 = 2999
	unsubscribeMaxReasonSize   int    = 123
)

// errInvalidUnsubscribe is returned for a custom unsubscribe with a code outside both ranges or a reason which is too long.
var errInvalidUnsubscribe = errors.New("invalid unsubscribe code or reason")

// absentUnsubscribe reports whether the request has no custom unsubscribe: no unsubscribe at all, or code 0
// with an empty reason.
func absentUnsubscribe(unsubscribe *Unsubscribe) bool {
	return unsubscribe == nil || (unsubscribe.Code == 0 && unsubscribe.Reason == "")
}

// validUnsubscribe returns an error when the custom unsubscribe is not one an application may send. An absent
// unsubscribe is valid.
func validUnsubscribe(unsubscribe *Unsubscribe) error {
	if absentUnsubscribe(unsubscribe) {
		return nil
	}
	inTerminalRange := unsubscribe.Code >= unsubscribeTerminalMinCode && unsubscribe.Code <= unsubscribeTerminalMaxCode
	inResubRange := unsubscribe.Code >= unsubscribeResubMinCode && unsubscribe.Code <= unsubscribeResubMaxCode
	if !inTerminalRange && !inResubRange {
		return errInvalidUnsubscribe
	}
	if len(unsubscribe.Reason) > unsubscribeMaxReasonSize {
		return errInvalidUnsubscribe
	}
	return nil
}

// unsubscribeOption returns the option which unsubscribes clients with the custom code and reason of the request.
// It returns nil when the request has no custom unsubscribe, so the default server unsubscribe applies.
func unsubscribeOption(unsubscribe *Unsubscribe) centrifuge.UnsubscribeOption {
	if absentUnsubscribe(unsubscribe) {
		return nil
	}
	return centrifuge.WithCustomUnsubscribe(centrifuge.Unsubscribe{
		Code:   unsubscribe.Code,
		Reason: unsubscribe.Reason,
	})
}

// sanitizeUnsubscribeBody returns the body of an unsubscribe consumer record without its unsubscribe when the
// unsubscribe is invalid, so the record is still applied with the default unsubscribe. An invalid unsubscribe is
// logged. A body which is not a JSON object is returned as it is: the request decoder rejects it.
func sanitizeUnsubscribeBody(body []byte) []byte {
	var record struct {
		Unsubscribe json.RawMessage `json:"unsubscribe"`
	}
	err := json.Unmarshal(body, &record)
	if err != nil || len(record.Unsubscribe) == 0 {
		return body
	}
	var unsubscribe Unsubscribe
	invalid := json.Unmarshal(record.Unsubscribe, &unsubscribe)
	if invalid == nil {
		invalid = validUnsubscribe(&unsubscribe)
	}
	if invalid == nil {
		return body
	}
	var fields map[string]json.RawMessage
	err = json.Unmarshal(body, &fields)
	if err != nil {
		return body
	}
	// encoding/json matches keys without case, so every key which the decoder would take as unsubscribe is removed.
	for key := range fields {
		if strings.EqualFold(key, "unsubscribe") {
			delete(fields, key)
		}
	}
	sanitized, err := json.Marshal(fields)
	if err != nil {
		return body
	}
	log.Warn().
		Err(invalid).
		Msg("ignoring invalid unsubscribe of a consumer unsubscribe command, it is applied with the default code")
	return sanitized
}
