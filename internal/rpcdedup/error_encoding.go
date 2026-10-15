package rpcdedup

import (
	"encoding/json"

	"github.com/centrifugal/centrifuge"
)

// redisError is the JSON form of a centrifuge.Error in the field e of a record. Empty fields are omitted.
type redisError struct {
	Code      uint32            `json:"c,omitempty"`
	Message   string            `json:"m,omitempty"`
	Temporary bool              `json:"t,omitempty"`
	AppCode   int32             `json:"ac,omitempty"`
	AppType   string            `json:"at,omitempty"`
	Metadata  map[string]string `json:"md,omitempty"`
}

// encodeError returns the JSON form of err for field e, or "" for no error. Both tiers count an error by the length of
// this form, see configtypes.RPCDedup.ReplyLimit.
func encodeError(err *centrifuge.Error) string {
	if err == nil {
		return ""
	}
	// Marshalling a struct of strings, numbers and a string map does not fail.
	data, _ := json.Marshal(redisError{
		Code:      err.Code,
		Message:   err.Message,
		Temporary: err.Temporary,
		AppCode:   err.AppCode,
		AppType:   err.AppType,
		Metadata:  err.Metadata,
	})
	return string(data)
}

// ErrorSize returns the size of err in the shared tier: the length of its encoded form, which is what both tiers count
// against the reply limit. It is zero for no error. Config validation checks the cancelled error with it.
func ErrorSize(err *centrifuge.Error) int {
	return len(encodeError(err))
}
