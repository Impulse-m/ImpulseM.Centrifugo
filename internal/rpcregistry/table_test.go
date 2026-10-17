package rpcregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
		Enabled:        true,
		AuthClassClaim: "auth_class",
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
	require.Equal(t, configtypes.DefaultTimeout, method.Timeout)
	require.Equal(t, configtypes.DefaultMaxRequestBytes, method.MaxRequestBytes)
	require.Equal(t, configtypes.DefaultMaxResponseBytes, method.MaxResponseBytes)
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
		Enabled:        true,
		File:           "../config/testdata/rpc_registry.yaml",
		AuthClassClaim: "auth_class",
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

func TestReloadWithPendingClassNeedsClaimAndKeepsOldTable(t *testing.T) {
	r := New()
	claimed := testRegistry()
	claimed.AuthClassClaim = "auth_class"
	reloadWith(t, r, claimed)

	reg := testRegistry()
	reg.Methods[0].AuthClass = configtypes.RPCAuthClassPending2FA
	_, _, err := r.PrepareReload(reg)
	require.ErrorContains(t, err, `method "users.getUser": auth_class: "pending_2fa" requires rpc.registry.auth_class_claim to be set`)
	require.Equal(t, AuthClassAuthenticated, r.Lookup(testGetUserMethod).AuthClass)
}

func TestReloadWithRingHashWithoutAffinityHeaderKeepsOldTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(registryContent("user.get")), 0o600))
	r := New()
	reloadWith(t, r, configtypes.RPCRegistry{Enabled: true, File: path})
	require.NotNil(t, r.Lookup("user.get"))

	ringHashContent := "version: 1\ntargets:\n  - name: users\n    endpoint: dns:///users.example:9000\n" +
		"    load_balancing: ring_hash\nmethods:\n  - method: user.get\n    grpc_method: /example.v1.UserService/Get\n" +
		"    target: users\n    auth_class: anonymous\n"
	require.NoError(t, os.WriteFile(path, []byte(ringHashContent), 0o600))
	_, _, err := r.PrepareReload(configtypes.RPCRegistry{Enabled: true, File: path})
	require.ErrorContains(t, err, `rpc registry target "users": ring_hash needs affinity_header`)
	require.True(t, isContentError(err))
	require.NotNil(t, r.Lookup("user.get"))
}

func TestReloadWithChangedAdmissionKeysKeepsOldTable(t *testing.T) {
	r := New()
	reloadWith(t, r, testRegistry())

	changed := testRegistry()
	changed.MaxInflightBytes = 1 << 20
	_, _, err := r.PrepareReload(changed)
	require.ErrorContains(t, err, "max_inflight_bytes and memory_watermark_bytes are read at start")
	require.NotNil(t, r.Lookup(testGetUserMethod))
}

func TestConnectionAuthClass(t *testing.T) {
	testCases := []struct {
		name     string
		claim    string
		userID   string
		wantOK   bool
		wantAuth AuthClass
	}{
		{name: "no claim without user is anonymous", claim: "", userID: "", wantOK: true, wantAuth: AuthClassAnonymous},
		{name: "no claim with user is authenticated", claim: "", userID: "42", wantOK: true, wantAuth: AuthClassAuthenticated},
		{name: "pending_2fa with user", claim: "pending_2fa", userID: "42", wantOK: true, wantAuth: AuthClassPending2FA},
		{name: "pending_2fa without user", claim: "pending_2fa", userID: "", wantOK: true, wantAuth: AuthClassPending2FA},
		{name: "bot with user", claim: "bot", userID: "42", wantOK: true, wantAuth: AuthClassBot},
		{name: "bot without user is invalid", claim: "bot", userID: "", wantOK: false},
		{name: "authenticated with user", claim: "authenticated", userID: "42", wantOK: true, wantAuth: AuthClassAuthenticated},
		{name: "authenticated without user is invalid", claim: "authenticated", userID: "", wantOK: false},
		{name: "anonymous without user", claim: "anonymous", userID: "", wantOK: true, wantAuth: AuthClassAnonymous},
		{name: "anonymous with user is invalid", claim: "anonymous", userID: "42", wantOK: false},
		{name: "unknown claim value is invalid", claim: "root", userID: "42", wantOK: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			auth, ok := ConnectionAuthClass(tc.claim, tc.userID)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantAuth, auth)
			}
		})
	}
}

func TestAuthClassAdmits(t *testing.T) {
	allClasses := []AuthClass{AuthClassAnonymous, AuthClassPending2FA, AuthClassAuthenticated, AuthClassBot}
	testCases := []struct {
		name   string
		method AuthClass
		admit  []AuthClass
	}{
		{
			name:   "anonymous admits all",
			method: AuthClassAnonymous,
			admit:  allClasses,
		},
		{
			name:   "pending_2fa admits pending_2fa, authenticated and bot",
			method: AuthClassPending2FA,
			admit: []AuthClass{
				AuthClassPending2FA,
				AuthClassAuthenticated,
				AuthClassBot,
			},
		},
		{
			name:   "authenticated admits authenticated and bot",
			method: AuthClassAuthenticated,
			admit: []AuthClass{
				AuthClassAuthenticated,
				AuthClassBot,
			},
		},
		{
			name:   "bot admits bot",
			method: AuthClassBot,
			admit: []AuthClass{
				AuthClassBot,
			},
		},
		{
			name:   "zero admits nothing",
			method: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, conn := range allClasses {
				require.Equal(t, slices.Contains(tc.admit, conn), tc.method.Admits(conn), "connection class %d", conn)
			}
		})
	}
}
