package proxy

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MapGRPCRegistryError maps the error of a failed gRPC registry call to the protocol error or the
// disconnect the client receives. The configured status transforms come first, then the default
// table. The application code, type and allow-listed metadata are filled from the trailers in both
// cases. A transform's app_code and app_type fill the fields the backend did not send: a valid
// trailer wins, then a status message matching the type pattern, then the transform.
//
// Cancelled calls are not mapped here: the caller answers them from the registry config before it
// gets here.
func MapGRPCRegistryError(
	err error,
	trailers metadata.MD,
	transforms configtypes.RPCGRPCStatusToCodeTransforms,
	appKeys configtypes.RPCRegistryAppError,
	metadataKeys []string,
) (*centrifuge.Error, *centrifuge.Disconnect) {
	st := status.Convert(err)
	code := st.Code()

	rule, matched := transformGRPCStatus(code, transforms)
	var protoErr *proxyproto.Error
	if matched {
		var disconnect *proxyproto.Disconnect
		protoErr, disconnect = statusRuleResult(rule)
		if disconnect != nil {
			return nil, proxyproto.DisconnectFromProto(disconnect)
		}
	} else {
		protoErr = defaultGRPCStatusError(code, st.Message())
	}
	if protoErr == nil {
		// The default table has no protocol error for OK and Canceled. Canceled reaches here only
		// when the backend sent it: a cancelled client call is answered before the mapping. Such a
		// status is mapped to the internal class, as any other status without a rule.
		protoErr = defaultGRPCStatusError(codes.Internal, st.Message())
	}

	appError := extractGRPCAppError(trailers, code, st.Message(), appKeys, metadataKeys, rule.toError)
	result := proxyproto.ErrorFromProto(protoErr)
	result.AppCode = appError.AppCode
	result.AppType = appError.AppType
	result.Metadata = appError.Metadata
	return result, nil
}
