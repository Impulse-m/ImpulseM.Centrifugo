package proxy

import (
	"strconv"
	"unicode/utf8"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc/codes"
)

// grpcErrorMessageMaxLength limits the message of protocol errors built from gRPC statuses.
const grpcErrorMessageMaxLength = 256

// transformGRPCStatus maps a gRPC status code to a protocol error or disconnect with the configured
// transforms. ok is false when no transform matches the code.
func transformGRPCStatus(code codes.Code, transforms configtypes.RPCGRPCStatusToCodeTransforms) (*proxyproto.Error, *proxyproto.Disconnect, bool) {
	if len(transforms) == 0 {
		return nil, nil, false
	}
	return matchStatusRule(code, grpcStatusRules(transforms))
}

// grpcStatusRules converts gRPC status transforms to status rules. Status names which are not gRPC
// status codes are skipped, validation rejects them before they get here.
func grpcStatusRules(transforms configtypes.RPCGRPCStatusToCodeTransforms) []statusRule[codes.Code] {
	rules := make([]statusRule[codes.Code], 0, len(transforms))
	for _, t := range transforms {
		code, ok := grpcStatusByName(t.Status)
		if !ok {
			continue
		}
		rules = append(rules, statusRule[codes.Code]{
			status:       code,
			toError:      t.ToError,
			toDisconnect: t.ToDisconnect,
		})
	}
	return rules
}

// grpcStatusByName returns the gRPC status code with the exact name, for example UNAUTHENTICATED.
func grpcStatusByName(name string) (codes.Code, bool) {
	var code codes.Code
	// The same parser validates the names at config load, so both accept exactly the same names.
	err := code.UnmarshalJSON([]byte(strconv.Quote(name)))
	if err != nil {
		return codes.OK, false
	}
	return code, true
}

// defaultGRPCStatusError returns the protocol error for a gRPC status when no transform matches it.
// It returns nil for OK and for Canceled: the caller maps a cancelled call from its own configuration.
func defaultGRPCStatusError(code codes.Code, message string) *proxyproto.Error {
	protocolError := &proxyproto.Error{Message: capGRPCErrorMessage(message)}
	switch code {
	case codes.OK, codes.Canceled:
		return nil
	case codes.Unauthenticated:
		protocolError.Code = 101
	case codes.PermissionDenied:
		protocolError.Code = 103
	case codes.ResourceExhausted:
		protocolError.Code = 111
		protocolError.Temporary = true
	case codes.Unavailable, codes.DeadlineExceeded:
		protocolError.Code = 100
		protocolError.Temporary = true
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.AlreadyExists, codes.Aborted, codes.OutOfRange:
		protocolError.Code = 400
	default:
		// Internal class: Internal, Unknown, Unimplemented, DataLoss, and any code unknown to grpc.
		protocolError.Code = 100
	}
	return protocolError
}

// capGRPCErrorMessage limits the message to grpcErrorMessageMaxLength bytes without splitting a UTF-8 sequence.
func capGRPCErrorMessage(message string) string {
	if len(message) <= grpcErrorMessageMaxLength {
		return message
	}
	cut := grpcErrorMessageMaxLength
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut]
}
