package proxy

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistryMessageTooLarge(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		requestLen int
		bound      int
		compressed bool
		tooLarge   bool
		reply      bool
	}{
		{
			name:       "reply over the bound",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound after decompression",
			err:        status.Error(codes.ResourceExhausted, "grpc: message after decompression larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound without a size",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message after decompression larger than max 1024"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "target limit equal to the bound refuses an echo of the request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
			requestLen: 5000,
			bound:      4194304,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "reply over a large bound",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
			requestLen: 100,
			bound:      4194304,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "reply over the bound with the largest int named",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. "+strconv.Itoa(math.MaxInt)+")"),
			requestLen: 100,
			bound:      1024,
			tooLarge:   true,
			reply:      true,
		},
		{
			name:       "echo whose reply bound is below its request is a refused request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (20480 vs. 16384)"),
			requestLen: 20480,
			bound:      16384,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "compressed target is taken as a refused request",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
			compressed: true,
			tooLarge:   true,
			reply:      false,
		},
		{
			name:       "quota is not a message size",
			err:        status.Error(codes.ResourceExhausted, "quota exceeded"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "application text with the size words is not a message size",
			err:        status.Error(codes.ResourceExhausted, "quota: larger than max plan"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "grpc size text with sizes that match neither the request nor the bound",
			err:        status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2000 vs. 4096)"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "other code with the size text",
			err:        status.Error(codes.Unavailable, "grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
		},
		{
			name:       "not a status",
			err:        errors.New("grpc: received message larger than max (2000 vs. 1024)"),
			requestLen: 100,
			bound:      1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tooLarge, reply := registryMessageTooLarge(tt.err, tt.requestLen, tt.bound, tt.compressed)
			require.Equal(t, tt.tooLarge, tooLarge)
			require.Equal(t, tt.reply, reply)
		})
	}
}

func TestRegistryCallTargetRefusingRequestIsReported(t *testing.T) {
	backend := newRegistryTestBackend(t)
	cfg := registryTestConfig()
	cfg.Methods = append(cfg.Methods, configtypes.RPCMethod{
		Method:           "users.refuse",
		GRPCMethod:       "/example.v1.UserService/Refuse",
		Target:           "backend",
		MaxRequestBytes:  8192,
		MaxResponseBytes: 4194304,
	})
	backend.FailMethod("/example.v1.UserService/Refuse", tools.GRPCBytesFailure{
		Err: status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000 vs. 4194304)"),
	})
	r := startRegistryTest(t, cfg, registryTestConn(t, backend))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.refuse"), RegistryCall{
		ClientID: "client-1",
		Data:     make([]byte, 5000),
	})
	require.ErrorIs(t, err, ErrRPCTargetRequestTooLarge)
	require.NotErrorIs(t, err, ErrRPCResponseTooLarge)
}

func TestRegistryCallApplicationErrorWithSizeTextKeepsStatus(t *testing.T) {
	backend := newRegistryTestBackend(t)
	cfg := registryTestConfig()
	cfg.Methods = append(cfg.Methods, configtypes.RPCMethod{
		Method:           "users.quota",
		GRPCMethod:       "/example.v1.UserService/Quota",
		Target:           "backend",
		MaxRequestBytes:  8192,
		MaxResponseBytes: 4194304,
	})
	backend.FailMethod("/example.v1.UserService/Quota", tools.GRPCBytesFailure{
		Err: status.Error(codes.ResourceExhausted, "quota: larger than max plan"),
	})
	r := startRegistryTest(t, cfg, registryTestConn(t, backend))

	_, _, err := CallRegistryMethod(context.Background(), r.Lookup("users.quota"), RegistryCall{
		ClientID: "client-1",
		Data:     make([]byte, 5000),
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.NotErrorIs(t, err, ErrRPCTargetRequestTooLarge)
	require.NotErrorIs(t, err, ErrRPCResponseTooLarge)
}
