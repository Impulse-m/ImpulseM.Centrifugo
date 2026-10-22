package kafkautil

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/stretchr/testify/require"
)

// fakeAssumeRole counts the AssumeRole calls and returns credentials which expire at expiresAt.
type fakeAssumeRole struct {
	calls     int
	expiresAt time.Time
}

// AssumeRole implements assumeRoler.
func (f *fakeAssumeRole) AssumeRole(
	_ context.Context,
	_ *sts.AssumeRoleInput,
	_ ...func(*sts.Options),
) (*sts.AssumeRoleOutput, error) {
	f.calls++
	return &sts.AssumeRoleOutput{
		Credentials: &ststypes.Credentials{
			AccessKeyId:     aws.String("key"),
			SecretAccessKey: aws.String("secret"),
			SessionToken:    aws.String("token"),
			Expiration:      aws.Time(f.expiresAt),
		},
	}, nil
}

// TestMSKAuthCachesCredentials runs two handshakes within the first part of the credentials lifetime, which share one
// AssumeRole call, and a third one after most of the lifetime, which assumes the role again.
func TestMSKAuthCachesCredentials(t *testing.T) {
	start := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	now := start
	fake := &fakeAssumeRole{expiresAt: start.Add(time.Hour)}
	auth := &mskAssumeRoleAuth{
		stsClient: fake,
		roleARN:   "arn:aws:iam::123456789012:role/msk",
		cache:     true,
		now:       func() time.Time { return now },
	}
	ctx := context.Background()

	for range 2 {
		creds, err := auth.auth(ctx)
		require.NoError(t, err)
		require.Equal(t, "key", creds.AccessKey)
	}
	require.Equal(t, 1, fake.calls)

	now = start.Add(50 * time.Minute)
	_, err := auth.auth(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, fake.calls)
}

// TestMSKAuthWithoutCacheAssumesRoleOnEveryHandshake pins the default of the consumers: each handshake calls AssumeRole.
func TestMSKAuthWithoutCacheAssumesRoleOnEveryHandshake(t *testing.T) {
	start := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	fake := &fakeAssumeRole{expiresAt: start.Add(time.Hour)}
	auth := &mskAssumeRoleAuth{
		stsClient: fake,
		roleARN:   "arn:aws:iam::123456789012:role/msk",
		now:       func() time.Time { return start },
	}
	ctx := context.Background()

	for range 2 {
		creds, err := auth.auth(ctx)
		require.NoError(t, err)
		require.Equal(t, "key", creds.AccessKey)
	}
	require.Equal(t, 2, fake.calls)
}
