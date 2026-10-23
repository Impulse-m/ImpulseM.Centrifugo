package apns

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPNsHeaderLengthBounds(t *testing.T) {
	testCases := []struct {
		name        string
		headers     map[string]string
		expectError bool
	}{
		{name: "topic of 255 bytes accepted", headers: map[string]string{"apns-topic": strings.Repeat("t", 255)}},
		{name: "topic of 256 bytes refused", headers: map[string]string{"apns-topic": strings.Repeat("t", 256)}, expectError: true},
		{name: "thread id of 64 bytes accepted", headers: map[string]string{"apns-thread-id": strings.Repeat("h", 64)}},
		{name: "thread id of 65 bytes refused", headers: map[string]string{"apns-thread-id": strings.Repeat("h", 65)}, expectError: true},
		{name: "push type of 256 bytes refused", headers: map[string]string{"apns-push-type": strings.Repeat("p", 256)}, expectError: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := CheckHeaders(testCase.headers, "")
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
