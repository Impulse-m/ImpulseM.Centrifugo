package provider

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

func TestTruncateKeepsShortValues(t *testing.T) {
	value := strings.Repeat("a", MaxFieldBytes)

	require.Equal(t, value, Truncate(value))
	require.Equal(t, "ok", Truncate("ok"))
}

func TestTruncateCutsLongValues(t *testing.T) {
	result := Truncate(strings.Repeat("a", MaxFieldBytes+100))

	require.Len(t, result, MaxFieldBytes)
}

func TestTruncateNeverSplitsARune(t *testing.T) {
	// The two-byte rune of U+00E9 starts at byte 255 and ends at byte 256, so the byte limit falls inside it.
	value := strings.Repeat("a", MaxFieldBytes-1) + "é" + strings.Repeat("b", 10)

	result := Truncate(value)

	require.True(t, utf8.ValidString(result), "a proto3 string must stay valid UTF-8")
	require.LessOrEqual(t, len(result), MaxFieldBytes)
	require.Equal(t, strings.Repeat("a", MaxFieldBytes-1), result)
}

func TestTruncateKeepsWholeRuneAtTheLimit(t *testing.T) {
	// The three-byte rune of U+20AC ends exactly at the limit, so it is kept whole.
	value := strings.Repeat("a", MaxFieldBytes-3) + "€" + strings.Repeat("b", 10)

	result := Truncate(value)

	require.True(t, utf8.ValidString(result))
	require.Equal(t, strings.Repeat("a", MaxFieldBytes-3)+"€", result)
}

func TestExpiredBoundary(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	testCases := []struct {
		name     string
		expireAt int64
		expected bool
	}{
		{name: "unset", expireAt: 0, expected: false},
		{name: "before now", expireAt: now.Unix() - 1, expected: true},
		{name: "equal to now", expireAt: now.Unix(), expected: true},
		{name: "after now", expireAt: now.Unix() + 1, expected: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			job := &Job{
				Target:       &apiproto.PushTarget{Provider: ProviderAPNs},
				Notification: &apiproto.PushNotification{ExpireAt: testCase.expireAt},
			}

			require.Equal(t, testCase.expected, Expired(job, now))
		})
	}
}
