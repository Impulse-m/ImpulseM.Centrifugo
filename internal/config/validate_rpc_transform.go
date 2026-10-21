package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// validateRPCTransformAppFields checks the application fields of the to_error of the gRPC status transform i. They
// need a code, and a type must match the pattern of an application error type.
func validateRPCTransformAppFields(i int, toError configtypes.RPCTransformError) error {
	if (toError.AppCode != 0 || toError.AppType != "") && toError.Code == 0 {
		return fmt.Errorf("grpc_status_to_code_transforms[%d]: to_error.app_code and to_error.app_type need to_error.code", i)
	}
	if toError.AppType != "" && !configtypes.RPCAppErrorTypePattern.MatchString(toError.AppType) {
		return fmt.Errorf("grpc_status_to_code_transforms[%d]: to_error.app_type must match ^[A-Z0-9_]{1,64}$, got %q", i, toError.AppType)
	}
	return nil
}
