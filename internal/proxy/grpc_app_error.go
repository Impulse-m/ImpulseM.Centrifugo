package proxy

import (
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// grpcAppError is the application part of a protocol error built from a failed gRPC call.
type grpcAppError struct {
	// AppCode is the application error code.
	AppCode int32
	// AppType is the application error type. Empty when neither a trailer nor the status message gives one.
	AppType string
	// Metadata holds the allow-listed trailers. Nil when none of them is set.
	Metadata map[string]string
}

// extractGRPCAppError reads the application error code and type from the trailers of a failed gRPC call.
// The code comes from the keys.CodeKey trailer as a decimal int32, else from the fallback code when it is
// set, else from the default of the status. The type comes from the keys.TypeKey trailer when it matches
// RPCAppErrorTypePattern. Otherwise the type is the status message when the message matches, else the
// fallback type. Metadata holds only the trailers named in metadataKeys.
func extractGRPCAppError(
	trailers metadata.MD,
	status codes.Code,
	message string,
	keys configtypes.RPCRegistryAppError,
	metadataKeys []string,
	fallback configtypes.RPCTransformError,
) grpcAppError {
	result := grpcAppError{
		AppCode: defaultAppErrorCode(status),
	}
	if fallback.AppCode != 0 {
		result.AppCode = fallback.AppCode
	}

	codeValue, ok := trailerValue(trailers, keys.CodeKey)
	if ok {
		appCode, err := strconv.ParseInt(codeValue, 10, 32)
		if err == nil {
			result.AppCode = int32(appCode)
		}
	}

	typeValue, ok := trailerValue(trailers, keys.TypeKey)
	switch {
	case ok && configtypes.RPCAppErrorTypePattern.MatchString(typeValue):
		result.AppType = typeValue
	case configtypes.RPCAppErrorTypePattern.MatchString(message):
		result.AppType = message
	default:
		result.AppType = fallback.AppType
	}

	for _, key := range metadataKeys {
		value, ok := trailerValue(trailers, key)
		if !ok {
			continue
		}
		if result.Metadata == nil {
			result.Metadata = make(map[string]string, len(metadataKeys))
		}
		result.Metadata[strings.ToLower(key)] = value
	}
	return result
}

// trailerValue returns the first value of a trailer. metadata.MD lower-cases the key.
func trailerValue(trailers metadata.MD, key string) (string, bool) {
	values := trailers.Get(key)
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// defaultAppErrorCode is the application error code of a failed call without an app code trailer.
func defaultAppErrorCode(status codes.Code) int32 {
	switch status {
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.ResourceExhausted:
		return 420
	case codes.InvalidArgument:
		return 400
	default:
		return 500
	}
}
