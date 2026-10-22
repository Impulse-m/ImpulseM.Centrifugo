package proxy

import (
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rpcRegistryNonStatusMessage is the client message of a call which failed with a plain Go error.
const rpcRegistryNonStatusMessage = "backend call failed"

// nonStatusErrLog throttles the server log of the plain Go errors of registry calls, so that a backend which fails
// every call does not write a line per call on the request path. At most one line per interval, across all targets.
var nonStatusErrLog = tools.NewIntervalLimiter(time.Minute)

// nonStatusErrSuppressed counts the plain Go errors which the throttle did not log since the last line, and the next
// line reports the count, so a second failure in the same interval is not silently dropped.
var nonStatusErrSuppressed atomic.Int64

// MapGRPCRegistryError maps the error of a failed gRPC registry call to the protocol error or the
// disconnect the client receives. The configured status transforms come first, then the default
// table. The application code, type and allow-listed metadata are filled from the trailers in both
// cases. A transform's app_code and app_type fill the fields the backend did not send: a valid
// trailer wins, then a status message matching the type pattern, then the transform.
//
// Cancelled calls are not mapped here: the caller answers them from the registry config before it
// gets here.
//
// A plain Go error, which is not a gRPC status, is logged here in full, and the client gets the fixed message
// rpcRegistryNonStatusMessage instead of its text: the text can name internal addresses.
func MapGRPCRegistryError(
	err error,
	trailers metadata.MD,
	transforms configtypes.RPCGRPCStatusToCodeTransforms,
	appKeys configtypes.RPCRegistryAppError,
	metadataKeys []string,
) (*centrifuge.Error, *centrifuge.Disconnect) {
	st, isStatus := status.FromError(err)
	code := st.Code()
	message := st.Message()
	if !isStatus {
		if nonStatusErrLog.Allow() {
			log.Warn().Err(err).Int64("suppressed", nonStatusErrSuppressed.Swap(0)).Msg("rpc registry call failed with a non-status error")
		} else {
			nonStatusErrSuppressed.Add(1)
		}
		message = rpcRegistryNonStatusMessage
	}

	rule, matched := transformGRPCStatus(code, transforms)
	var protoErr *proxyproto.Error
	if matched {
		var disconnect *proxyproto.Disconnect
		protoErr, disconnect = statusRuleResult(rule)
		if disconnect != nil {
			return nil, proxyproto.DisconnectFromProto(disconnect)
		}
	} else {
		protoErr = defaultGRPCStatusError(code, message)
	}
	if protoErr == nil {
		// The default table has no protocol error for OK and Canceled. Canceled reaches here only
		// when the backend sent it: a cancelled client call is answered before the mapping. Such a
		// status is mapped to the internal class, as any other status without a rule.
		protoErr = defaultGRPCStatusError(codes.Internal, message)
	}

	appError := extractGRPCAppError(trailers, code, message, appKeys, metadataKeys, rule.toError)
	result := proxyproto.ErrorFromProto(protoErr)
	result.AppCode = appError.AppCode
	result.AppType = appError.AppType
	result.Metadata = appError.Metadata
	return result, nil
}
