package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"
)

// statusToCodeTransformsKey is the config key of the HTTP status code transforms, used in messages.
const statusToCodeTransformsKey = "status_to_code_transforms"

// reasonOrMessageMaxLength limits disconnect reasons and error messages. The limit comes from the
// WebSocket close reason length limit. See https://datatracker.ietf.org/doc/html/rfc6455.
const reasonOrMessageMaxLength = 123

// validateStatusToCodeTarget checks what a status transform maps to: exactly one of a protocol error
// or a disconnect, with ASCII texts within the length limit. key is the config key of the transform
// list and index is the position of the transform in it. Both are used in error messages.
func validateStatusToCodeTarget(
	key string,
	index int,
	toError configtypes.TransformError,
	toDisconnect configtypes.TransformDisconnect,
) error {
	if toDisconnect.Code == 0 && toError.Code == 0 {
		return fmt.Errorf("no error or disconnect code set in %s[%d]", key, index)
	}
	if toDisconnect.Code > 0 && toError.Code > 0 {
		return fmt.Errorf("only error or disconnect code can be set in %s[%d], but not both", key, index)
	}
	if !tools.IsASCII(toDisconnect.Reason) {
		return fmt.Errorf("%s[%d] disconnect reason must be ASCII", key, index)
	}
	if !tools.IsASCII(toError.Message) {
		return fmt.Errorf("%s[%d] error message must be ASCII", key, index)
	}
	if len(toError.Message) > reasonOrMessageMaxLength {
		return fmt.Errorf("%s[%d] item error message can be up to %d characters long", key, index, reasonOrMessageMaxLength)
	}
	if len(toDisconnect.Reason) > reasonOrMessageMaxLength {
		return fmt.Errorf("%s[%d] disconnect reason can be up to %d characters long", key, index, reasonOrMessageMaxLength)
	}
	return nil
}
