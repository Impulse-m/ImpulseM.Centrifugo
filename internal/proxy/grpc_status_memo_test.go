package proxy

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// A status name is parsed once: a later lookup of the name answers from the memo, with no allocation.
func TestGRPCStatusByNameMemoized(t *testing.T) {
	code, ok := grpcStatusByName("UNAUTHENTICATED")
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, code)

	_, ok = grpcStatusByName("NOT_A_STATUS")
	require.False(t, ok)

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = grpcStatusByName("UNAUTHENTICATED")
	})
	require.Zero(t, allocs)
}

// A failed call with status transforms allocates the rule slice only, not a parse of each transform name.
func TestTransformGRPCStatusAllocatesOnlyTheRules(t *testing.T) {
	transforms := make(configtypes.RPCGRPCStatusToCodeTransforms, 10)
	for i := range transforms {
		transforms[i] = configtypes.RPCGRPCStatusToCodeTransform{
			Status:  "UNAVAILABLE",
			ToError: configtypes.RPCTransformError{TransformError: configtypes.TransformError{Code: 1000 + uint32(i)}},
		}
	}
	_, ok := transformGRPCStatus(codes.Unavailable, transforms)
	require.True(t, ok)

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = transformGRPCStatus(codes.Unavailable, transforms)
	})
	require.LessOrEqual(t, allocs, 1.0)
}
