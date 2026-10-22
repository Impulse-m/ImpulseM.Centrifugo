package proxy

import (
	"unicode/utf8"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc/codes"
)

// grpcErrorMessageMaxLength limits the message of protocol errors built from gRPC statuses.
const grpcErrorMessageMaxLength = 256

// transformGRPCStatus returns the first transform rule which matches a gRPC status code and sets a
// protocol error or a disconnect. ok is false when no transform matches the code. Status names which
// are not gRPC status codes are skipped, validation rejects them before they get here.
func transformGRPCStatus(code codes.Code, transforms configtypes.RPCGRPCStatusToCodeTransforms) (statusRule[codes.Code], bool) {
	for _, t := range transforms {
		status, ok := grpcStatusByName(t.Status)
		if !ok || status != code {
			continue
		}
		if t.ToError.Code > 0 || t.ToDisconnect.Code > 0 {
			return statusRule[codes.Code]{
				status:       status,
				toError:      t.ToError,
				toDisconnect: t.ToDisconnect,
			}, true
		}
	}
	return statusRule[codes.Code]{}, false
}

// grpcStatusCodesByName holds the names which codes.Code.UnmarshalJSON accepts, so a lookup needs no
// parse. The config validates the names with that parser, and a test keeps this table equal to it.
var grpcStatusCodesByName = map[string]codes.Code{
	"OK":                  codes.OK,
	"CANCELLED":           codes.Canceled,
	"UNKNOWN":             codes.Unknown,
	"INVALID_ARGUMENT":    codes.InvalidArgument,
	"DEADLINE_EXCEEDED":   codes.DeadlineExceeded,
	"NOT_FOUND":           codes.NotFound,
	"ALREADY_EXISTS":      codes.AlreadyExists,
	"PERMISSION_DENIED":   codes.PermissionDenied,
	"RESOURCE_EXHAUSTED":  codes.ResourceExhausted,
	"FAILED_PRECONDITION": codes.FailedPrecondition,
	"ABORTED":             codes.Aborted,
	"OUT_OF_RANGE":        codes.OutOfRange,
	"UNIMPLEMENTED":       codes.Unimplemented,
	"INTERNAL":            codes.Internal,
	"UNAVAILABLE":         codes.Unavailable,
	"DATA_LOSS":           codes.DataLoss,
	"UNAUTHENTICATED":     codes.Unauthenticated,
}

// grpcStatusByName returns the gRPC status code with the exact name, for example UNAUTHENTICATED.
func grpcStatusByName(name string) (codes.Code, bool) {
	code, ok := grpcStatusCodesByName[name]
	return code, ok
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
