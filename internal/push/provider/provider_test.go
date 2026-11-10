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

func TestPrintableHeaderKeepsPrintableValues(t *testing.T) {
	require.Equal(t, "https://push.example/3/abc", PrintableHeader("https://push.example/3/abc"))
	require.Equal(t, "é€", PrintableHeader("é€"))
	require.Equal(t, strings.Repeat("a", MaxFieldBytes), PrintableHeader(strings.Repeat("a", MaxFieldBytes)))
}

func TestPrintableHeaderRefusesControlCharactersAndLength(t *testing.T) {
	testCases := map[string]string{
		"line feed":            "a\nb",
		"carriage return":      "a\rb",
		"tab":                  "a\tb",
		"nul":                  "a\x00b",
		"delete":               "a\x7fb",
		"c1 control":           "a\u0085b",
		"invalid utf-8":        "a\xffb",
		"one byte over limit":  strings.Repeat("a", MaxFieldBytes+1),
		"empty value is empty": "",
	}
	for name, value := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, PrintableHeader(value))
		})
	}
}

func TestWebPushPayloadBoundFollowsFromTheHeaderSizes(t *testing.T) {
	// The body is salt 16, record size 4, key id length 1 and key id 65, the plaintext, the padding delimiter and the tag.
	const headerBytes = 16 + 4 + 1 + 65
	const paddingDelimiterBytes = 1
	const tagBytes = 16

	require.Equal(t, 4096-headerBytes-paddingDelimiterBytes-tagBytes, MaxWebPushPayloadBytes)
}

func TestWebPushPayloadOfPrefersTheTarget(t *testing.T) {
	job := &Job{
		Target: &apiproto.PushTarget{
			Provider:       ProviderWebPush,
			WebpushPayload: []byte(`{"target":true}`),
		},
		Notification: &apiproto.PushNotification{
			Webpush: &apiproto.WebPushPushNotification{Payload: []byte(`{"request":true}`)},
		},
	}
	require.Equal(t, `{"target":true}`, string(WebPushPayloadOf(job)))

	job.Target.WebpushPayload = nil
	require.Equal(t, `{"request":true}`, string(WebPushPayloadOf(job)))

	job.Notification.Webpush = nil
	require.Empty(t, WebPushPayloadOf(job))
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
