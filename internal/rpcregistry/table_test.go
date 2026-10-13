package rpcregistry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

const testGetUserMethod = "users.getUser"

func testRegistry() configtypes.RPCRegistry {
	idempotent := true
	return configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass:        configtypes.RPCAuthClassAuthenticated,
			Timeout:          configtypes.Duration(7 * time.Second),
			MaxRequestBytes:  1000,
			MaxResponseBytes: 2000,
		},
		Targets: []configtypes.RPCTarget{
			{
				Name:            "users",
				Endpoint:        "dns:///users.example:9000",
				EmulatedHeaders: []string{"x-example-init", "x-example-a"},
				Timeout:         configtypes.Duration(3 * time.Second),
			},
		},
		Methods: []configtypes.RPCMethod{
			{
				Method:     testGetUserMethod,
				GRPCMethod: "/example.v1.UserService/GetUser",
				Target:     "users",
				Idempotent: &idempotent,
				Headers:    []string{"x-example-export", "x-example-a"},
			},
			{
				Method:     "help.getConfig",
				GRPCMethod: "/example.v1.HelpService/GetConfig",
				Target:     "users",
				AuthClass:  configtypes.RPCAuthClassAnonymous,
				Timeout:    configtypes.Duration(time.Second),
			},
		},
	}
}

func TestRegistryLookupMissingReturnsNil(t *testing.T) {
	r := New()
	require.Nil(t, r.Lookup("users.getUser"))
}

func TestPrepareApplySwapsTable(t *testing.T) {
	r := New()
	apply, err := r.Prepare(testRegistry())
	require.NoError(t, err)
	require.Nil(t, r.Lookup(testGetUserMethod), "table must not change before apply")

	apply()
	method := r.Lookup(testGetUserMethod)
	require.NotNil(t, method)
	require.Equal(t, "/example.v1.UserService/GetUser", method.GRPCMethod)
	require.Equal(t, AuthClassAuthenticated, method.AuthClass)
	require.True(t, method.Idempotent)
	require.Equal(t, []string{"x-example-a", "x-example-export"}, method.Headers)
}

func TestMethodDefaultsPrecedence(t *testing.T) {
	r := New()
	apply, err := r.Prepare(testRegistry())
	require.NoError(t, err)
	apply()

	// Method timeout set on the entry wins over the target and defaults.
	helpMethod := r.Lookup("help.getConfig")
	require.NotNil(t, helpMethod)
	require.Equal(t, time.Second, helpMethod.Timeout)
	require.Equal(t, AuthClassAnonymous, helpMethod.AuthClass)
	// Unset method timeout falls back to the target, then defaults.
	getUser := r.Lookup(testGetUserMethod)
	require.Equal(t, 3*time.Second, getUser.Timeout)
	// Unset limits fall back to the defaults, then to built-in values.
	require.Equal(t, 1000, getUser.MaxRequestBytes)
	require.Equal(t, 2000, getUser.MaxResponseBytes)
	require.Equal(t, 1000, helpMethod.MaxRequestBytes)
	require.False(t, helpMethod.Idempotent)
	require.Equal(t, configtypes.RPCLoadBalancingRoundRobin, getUser.Target.Config.LoadBalancing)
}

func TestBuiltInDefaultsApplyWhenNothingIsSet(t *testing.T) {
	reg := configtypes.RPCRegistry{
		Enabled: true,
		Defaults: configtypes.RPCMethodDefaults{
			AuthClass: configtypes.RPCAuthClassBot,
		},
		Targets: []configtypes.RPCTarget{{Name: "users", Endpoint: "dns:///users:9000"}},
		Methods: []configtypes.RPCMethod{
			{Method: "bots.answer", GRPCMethod: "/example.v1.BotService/Answer", Target: "users"},
		},
	}
	r := New()
	apply, err := r.Prepare(reg)
	require.NoError(t, err)
	apply()

	method := r.Lookup("bots.answer")
	require.Equal(t, AuthClassBot, method.AuthClass)
	require.Equal(t, DefaultTimeout, method.Timeout)
	require.Equal(t, DefaultMaxRequestBytes, method.MaxRequestBytes)
	require.Equal(t, DefaultMaxResponseBytes, method.MaxResponseBytes)
}

func TestLookupDoesNotAllocate(t *testing.T) {
	r := New()
	apply, err := r.Prepare(testRegistry())
	require.NoError(t, err)
	apply()

	allocs := testing.AllocsPerRun(1000, func() {
		_ = r.Lookup(testGetUserMethod)
	})
	require.Zero(t, allocs)
}

func TestConcurrentLookupDuringReload(t *testing.T) {
	r := New()
	reg := testRegistry()
	apply, err := r.Prepare(reg)
	require.NoError(t, err)
	apply()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				method := r.Lookup(testGetUserMethod)
				if method == nil || method.Target == nil {
					panic("registered method disappeared during reload")
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		reg.Targets[0].Endpoint = fmt.Sprintf("dns:///users-%d.example:9000", i)
		next, err := r.Prepare(reg)
		require.NoError(t, err)
		next()
	}
	close(stop)
	wg.Wait()
}

func TestFailedPrepareKeepsOldTable(t *testing.T) {
	r := New()
	apply, err := r.Prepare(testRegistry())
	require.NoError(t, err)
	apply()
	before := r.Lookup(testGetUserMethod)

	broken := testRegistry()
	broken.Methods[0].Target = "missing"
	_, err = r.Prepare(broken)
	require.ErrorContains(t, err, `unknown target "missing"`)

	require.Same(t, before, r.Lookup(testGetUserMethod))
}

func TestDisabledRegistryPreparesEmptyTable(t *testing.T) {
	r := New()
	apply, err := r.Prepare(testRegistry())
	require.NoError(t, err)
	apply()
	require.NotNil(t, r.Lookup(testGetUserMethod))

	disabled := testRegistry()
	disabled.Enabled = false
	apply, err = r.Prepare(disabled)
	require.NoError(t, err)
	apply()
	require.Nil(t, r.Lookup(testGetUserMethod))
}

func TestPrepareFromFile(t *testing.T) {
	r := New()
	apply, err := r.Prepare(configtypes.RPCRegistry{
		Enabled: true,
		File:    "../config/testdata/rpc_registry.yaml",
	})
	require.NoError(t, err)
	apply()

	method := r.Lookup("users.signIn")
	require.NotNil(t, method)
	require.Equal(t, AuthClassAnonymous, method.AuthClass)
	require.Equal(t, "users", method.Target.Config.Name)
	require.Equal(t, []string{"example-token"}, method.ErrorMetadata)
	require.Equal(t, AuthClassPending2FA, r.Lookup("users.checkPassword").AuthClass)
}
