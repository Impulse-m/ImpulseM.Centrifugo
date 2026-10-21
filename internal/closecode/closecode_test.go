package closecode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// testRange is the range of the codes the test validator accepts.
var testRange Range = Range{Min: 1, Max: 10}

// validateTestValue returns an error when the raw value is not a code of testRange.
func validateTestValue(value json.RawMessage) error {
	return ValidateValue(value, func(code *uint32) error {
		return Validate(*code, "", testRange)
	})
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		code    uint32
		reason  string
		ranges  []Range
		wantErr bool
	}{
		{
			name:   "code in a range",
			code:   5,
			ranges: []Range{testRange},
		},
		{
			name:    "no ranges",
			code:    5,
			ranges:  nil,
			wantErr: true,
		},
		{
			name:    "code outside the ranges",
			code:    11,
			ranges:  []Range{testRange},
			wantErr: true,
		},
		{
			name:   "reason at the limit",
			code:   5,
			reason: strings.Repeat("r", 123),
			ranges: []Range{testRange},
		},
		{
			name:    "reason above the limit",
			code:    5,
			reason:  strings.Repeat("r", 124),
			ranges:  []Range{testRange},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.code, tt.reason, tt.ranges...)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSanitizeBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		// want is the body after sanitizing, compared as JSON.
		want string
		// wantErr is true when the invalid value must be returned as an error.
		wantErr bool
		// same is true when the body must be returned as it is.
		same bool
	}{
		{
			name: "valid value",
			body: `{"channel":"c","key":5}`,
			same: true,
		},
		{
			name:    "invalid value",
			body:    `{"channel":"c","key":50}`,
			want:    `{"channel":"c"}`,
			wantErr: true,
		},
		{
			name:    "key with other case",
			body:    `{"channel":"c","Key":50}`,
			want:    `{"channel":"c"}`,
			wantErr: true,
		},
		{
			name:    "key with upper case",
			body:    `{"channel":"c","KEY":50}`,
			want:    `{"channel":"c"}`,
			wantErr: true,
		},
		{
			name:    "escaped key",
			body:    `{"channel":"c","key":50}`,
			want:    `{"channel":"c"}`,
			wantErr: true,
		},
		{
			name: "key absent",
			body: `{"channel":"c","other":50}`,
			same: true,
		},
		{
			name: "body is not an object",
			body: `[50]`,
			same: true,
		},
		{
			name: "body is not json",
			body: `not json`,
			same: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizeBody([]byte(tt.body), "key", validateTestValue)
			if tt.same {
				require.Equal(t, tt.body, string(got))
				require.NoError(t, err)
				return
			}
			require.JSONEq(t, tt.want, string(got))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSanitizeBodyAbsentKeyDoesNotAllocate(t *testing.T) {
	body := []byte(`{"channel":"c","user":"u","client":"id","session":"s"}`)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = SanitizeBody(body, "key", validateTestValue)
	})
	require.Zero(t, allocs)
}

func BenchmarkSanitizeBodyAbsentKey(b *testing.B) {
	body := []byte(`{"channel":"c","user":"u","client":"id","session":"s"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = SanitizeBody(body, "key", validateTestValue)
	}
}
