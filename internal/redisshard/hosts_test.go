package redisshard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitAddresses(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    []string
	}{
		{
			name:    "host and port",
			address: "10.0.0.2:6379",
			want:    []string{"10.0.0.2:6379"},
		},
		{
			name:    "redis URL",
			address: "redis://user:secret@10.0.0.2:6379/3",
			want:    []string{"10.0.0.2:6379"},
		},
		{
			name:    "redis URL with addr parameters",
			address: "redis://10.0.0.2:6379?addr=10.0.0.3:6379&addr=10.0.0.4:6379",
			want:    []string{"10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"},
		},
		{
			name:    "malformed address is returned as written",
			address: "redis://10.0.0.2:6379/abc",
			want:    []string{"redis://10.0.0.2:6379/abc"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, InitAddresses(tt.address))
		})
	}
}
