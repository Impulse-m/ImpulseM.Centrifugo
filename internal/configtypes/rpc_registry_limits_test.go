package configtypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveRPCMethodLimitsPrecedence(t *testing.T) {
	defaults := RPCMethodDefaults{
		Timeout:          Duration(7 * time.Second),
		MaxRequestBytes:  2048,
		MaxResponseBytes: 4096,
	}
	target := RPCTarget{
		Timeout:          Duration(3 * time.Second),
		MaxRequestBytes:  1024,
		MaxResponseBytes: 8192,
	}

	t.Run("method wins over target and defaults", func(t *testing.T) {
		method := RPCMethod{Timeout: Duration(time.Second), MaxRequestBytes: 10, MaxResponseBytes: 20}
		limits := ResolveRPCMethodLimits(method, target, defaults)
		require.Equal(t, RPCMethodLimits{Timeout: time.Second, MaxRequestBytes: 10, MaxResponseBytes: 20}, limits)
	})
	t.Run("target wins over defaults", func(t *testing.T) {
		limits := ResolveRPCMethodLimits(RPCMethod{}, target, defaults)
		require.Equal(t, RPCMethodLimits{Timeout: 3 * time.Second, MaxRequestBytes: 1024, MaxResponseBytes: 8192}, limits)
	})
	t.Run("defaults win over built-in", func(t *testing.T) {
		limits := ResolveRPCMethodLimits(RPCMethod{}, RPCTarget{}, defaults)
		require.Equal(t, RPCMethodLimits{Timeout: 7 * time.Second, MaxRequestBytes: 2048, MaxResponseBytes: 4096}, limits)
	})
	t.Run("built-in when nothing sets a value", func(t *testing.T) {
		limits := ResolveRPCMethodLimits(RPCMethod{}, RPCTarget{}, RPCMethodDefaults{})
		require.Equal(t, RPCMethodLimits{
			Timeout:          DefaultTimeout,
			MaxRequestBytes:  DefaultMaxRequestBytes,
			MaxResponseBytes: DefaultMaxResponseBytes,
		}, limits)
	})
}
