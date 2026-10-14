package proxy

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// registryInternalErrorCode is the protocol error code of a failed gRPC call which has no mapping
// at all. The default table never reaches it: every status code maps to a code.
const registryInternalErrorCode = 100

// MapGRPCRegistryError maps the error of a failed gRPC registry call to the protocol error or the
// disconnect the client receives. The configured status transforms come first, then the default
// table. The application code, type and allow-listed metadata are filled from the trailers in both
// cases, so a transform keeps the application fields of the backend.
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

	protoErr, disconnect, matched := transformGRPCStatus(code, transforms)
	if matched && disconnect != nil {
		return nil, proxyproto.DisconnectFromProto(disconnect)
	}
	if !matched {
		protoErr = defaultGRPCStatusError(code, st.Message())
	}
	if protoErr == nil {
		protoErr = &proxyproto.Error{Code: registryInternalErrorCode, Message: capGRPCErrorMessage(st.Message())}
	}

	appError := extractGRPCAppError(trailers, code, st.Message(), appKeys, metadataKeys)
	result := proxyproto.ErrorFromProto(protoErr)
	result.AppCode = appError.AppCode
	result.AppType = appError.AppType
	result.Metadata = appError.Metadata
	return result, nil
}
