package clientaddr

import (
	"net/http"
	"net/netip"
	"runtime"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func mustResolver(t *testing.T, cfg configtypes.ClientAddress) *Resolver {
	t.Helper()
	r, err := New(cfg)
	require.NoError(t, err)
	return r
}

func enabledConfig(trusted ...string) configtypes.ClientAddress {
	return configtypes.ClientAddress{
		Enabled:        true,
		TrustedProxies: trusted,
		Header:         "x-forwarded-for",
	}
}

func headerWith(values ...string) http.Header {
	h := http.Header{}
	for _, v := range values {
		h.Add("X-Forwarded-For", v)
	}
	return h
}

func TestResolveWalk(t *testing.T) {
	testCases := []struct {
		name       string
		trusted    []string
		remoteAddr string
		header     http.Header
		want       string
	}{
		{
			name:       "untrusted peer ignores the header",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "203.0.113.5:9999",
			header:     headerWith("1.1.1.1"),
			want:       "203.0.113.5",
		},
		{
			name:       "trusted peer without a header is the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     http.Header{},
			want:       "10.0.0.9",
		},
		{
			name:       "single trusted hop takes the appended entry",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("1.1.1.1"),
			want:       "1.1.1.1",
		},
		{
			name:       "client entry left of the appended one is not used",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("9.9.9.9, 1.1.1.1"),
			want:       "1.1.1.1",
		},
		{
			name:       "multi-hop skips trusted proxies from the right",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("1.1.1.1, 10.0.0.2, 10.0.0.3"),
			want:       "1.1.1.1",
		},
		{
			name:       "multi-hop across several header lines",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("1.1.1.1, 10.0.0.2", "10.0.0.3"),
			want:       "1.1.1.1",
		},
		{
			name:       "all entries trusted is the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("10.0.0.2, 10.0.0.3"),
			want:       "10.0.0.9",
		},
		{
			name:       "junk left of the trusted hops stops the walk at the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("1.1.1.1, not-an-ip, 10.0.0.2"),
			want:       "10.0.0.9",
		},
		{
			name:       "empty entry stops the walk at the peer",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("1.1.1.1, "),
			want:       "10.0.0.9",
		},
		{
			name:       "ports and brackets are understood",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("2.2.2.2:4567, [2001:DB8::1]:443"),
			want:       "2001:db8::1",
		},
		{
			name:       "IPv4-mapped IPv6 entry is unmapped",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.9:1",
			header:     headerWith("::ffff:2.2.2.2"),
			want:       "2.2.2.2",
		},
		{
			name:       "IPv4-mapped IPv6 peer matches an IPv4 range",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "[::ffff:10.0.0.9]:1",
			header:     headerWith("1.1.1.1"),
			want:       "1.1.1.1",
		},
		{
			name:       "bare trusted address matches only itself",
			trusted:    []string{"10.0.0.1"},
			remoteAddr: "10.0.0.2:1",
			header:     headerWith("1.1.1.1"),
			want:       "10.0.0.2",
		},
		{
			name:       "IPv6 trusted range",
			trusted:    []string{"2001:db8::/32"},
			remoteAddr: "[2001:db8::5]:1",
			header:     headerWith("2.2.2.2"),
			want:       "2.2.2.2",
		},
		{
			name:       "unparseable peer is unknown",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "raw-addr",
			header:     headerWith("1.1.1.1"),
			want:       "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := mustResolver(t, enabledConfig(tc.trusted...))
			got := r.Resolve(tc.remoteAddr, tc.header)
			if tc.want == "" {
				require.False(t, got.IsValid())
				return
			}
			require.Equal(t, netip.MustParseAddr(tc.want), got)
		})
	}
}

// TestResolveReadsOnlyTheEntriesItWalks checks that a long header costs no memory in proportion to its entries: the
// walk stops at the client, the rightmost entry which is not a trusted proxy, and the entries left of it are never
// turned into a list. A list of the entries of this header would take 160 KB per call, 16 MB over the calls measured
// here, so the bound of 1 MiB fails for it and holds for the few bytes a call needs to parse the peer address.
func TestResolveReadsOnlyTheEntriesItWalks(t *testing.T) {
	const (
		entries int    = 10000
		calls   int    = 100
		bound   uint64 = 1 << 20
	)
	r := mustResolver(t, enabledConfig("10.0.0.0/8"))
	header := headerWith(strings.Repeat("junk,", entries) + "1.1.1.1, 10.0.0.2")
	want := netip.MustParseAddr("1.1.1.1")

	var before runtime.MemStats
	var after runtime.MemStats
	runtime.ReadMemStats(&before)
	for call := 0; call < calls; call++ {
		require.Equal(t, want, r.Resolve("10.0.0.9:1", header))
	}
	runtime.ReadMemStats(&after)

	require.Less(t, after.TotalAlloc-before.TotalAlloc, bound)
}

func TestResolveHeaderName(t *testing.T) {
	// A single-value vendor header is a one-entry list and needs no special case.
	cfg := enabledConfig("10.0.0.0/8")
	cfg.Header = "CF-Connecting-IP"
	r := mustResolver(t, cfg)
	h := http.Header{}
	h.Set("cf-connecting-ip", "2.2.2.2")
	h.Set("X-Forwarded-For", "3.3.3.3")
	require.Equal(t, netip.MustParseAddr("2.2.2.2"), r.Resolve("10.0.0.9:1", h))

	// X-Real-IP is not read unless it is the configured header.
	r = mustResolver(t, enabledConfig("10.0.0.0/8"))
	h = http.Header{}
	h.Set("X-Real-IP", "5.5.5.5")
	require.Equal(t, netip.MustParseAddr("10.0.0.9"), r.Resolve("10.0.0.9:1", h))
}
