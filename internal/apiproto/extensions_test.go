package apiproto

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// fieldSpec is the expected name, number and kind of one proto field.
type fieldSpec struct {
	name   protoreflect.Name
	number protoreflect.FieldNumber
	kind   protoreflect.Kind
}

// collectFields returns the fields of a message descriptor, in declaration order.
func collectFields(md protoreflect.MessageDescriptor) []fieldSpec {
	var out []fieldSpec
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		out = append(out, fieldSpec{
			name:   f.Name(),
			number: f.Number(),
			kind:   f.Kind(),
		})
	}
	return out
}

// forkFields returns the fields of a message descriptor whose number is 100 or
// above. Fork fields on existing PRO messages use these numbers.
func forkFields(md protoreflect.MessageDescriptor) []fieldSpec {
	var out []fieldSpec
	for _, f := range collectFields(md) {
		if f.number >= 100 {
			out = append(out, f)
		}
	}
	return out
}

// TestForkFieldNumbersAreExact checks that the fork fields added to existing PRO
// messages use exactly the numbers of the direct send design, and that no other
// field of these messages uses a number at or above 100.
func TestForkFieldNumbersAreExact(t *testing.T) {
	require.Equal(t, []fieldSpec{
		{name: "targets", number: 100, kind: protoreflect.MessageKind},
	}, forkFields((&PushRecipient{}).ProtoReflect().Descriptor()))

	require.Equal(t, []fieldSpec{
		{name: "collapse_id", number: 100, kind: protoreflect.StringKind},
		{name: "timeout_ms", number: 101, kind: protoreflect.Uint32Kind},
	}, forkFields((&SendPushNotificationRequest{}).ProtoReflect().Descriptor()))

	require.Equal(t, []fieldSpec{
		{name: "results", number: 100, kind: protoreflect.MessageKind},
	}, forkFields((&SendPushNotificationResult{}).ProtoReflect().Descriptor()))
}

// TestPROFieldNumbersUnchanged checks that the PRO fields these messages keep
// their numbers, so the fork additions do not shift upstream fields.
func TestPROFieldNumbersUnchanged(t *testing.T) {
	uidField := (&SendPushNotificationResult{}).ProtoReflect().Descriptor().Fields().ByName("uid")
	require.NotNil(t, uidField)
	require.Equal(t, protoreflect.FieldNumber(1), uidField.Number())

	webpushField := (&PushRecipient{}).ProtoReflect().Descriptor().Fields().ByName("webpush_tokens")
	require.NotNil(t, webpushField)
	require.Equal(t, protoreflect.FieldNumber(9), webpushField.Number())

	useMetaField := (&SendPushNotificationRequest{}).ProtoReflect().Descriptor().Fields().ByName("use_meta")
	require.NotNil(t, useMetaField)
	require.Equal(t, protoreflect.FieldNumber(10), useMetaField.Number())
}

// TestPushTargetFields checks the fields of the PushTarget message.
func TestPushTargetFields(t *testing.T) {
	require.Equal(t, []fieldSpec{
		{name: "provider", number: 1, kind: protoreflect.StringKind},
		{name: "token", number: 2, kind: protoreflect.StringKind},
		{name: "environment", number: 3, kind: protoreflect.StringKind},
		{name: "ref", number: 4, kind: protoreflect.StringKind},
		{name: "collapse_id", number: 5, kind: protoreflect.StringKind},
		{name: "apns_payload", number: 6, kind: protoreflect.BytesKind},
		{name: "fcm_message", number: 7, kind: protoreflect.BytesKind},
	}, collectFields((&PushTarget{}).ProtoReflect().Descriptor()))
}

// TestPushItemResultFields checks the fields of the PushItemResult message.
func TestPushItemResultFields(t *testing.T) {
	require.Equal(t, []fieldSpec{
		{name: "index", number: 1, kind: protoreflect.Uint32Kind},
		{name: "ref", number: 2, kind: protoreflect.StringKind},
		{name: "status", number: 3, kind: protoreflect.StringKind},
		{name: "code", number: 4, kind: protoreflect.StringKind},
		{name: "provider_code", number: 5, kind: protoreflect.StringKind},
		{name: "provider_detail", number: 6, kind: protoreflect.StringKind},
		{name: "http_status", number: 7, kind: protoreflect.Uint32Kind},
		{name: "provider_message_id", number: 8, kind: protoreflect.StringKind},
		{name: "provider_details", number: 9, kind: protoreflect.BytesKind},
		{name: "provider_details_truncated", number: 10, kind: protoreflect.BoolKind},
	}, collectFields((&PushItemResult{}).ProtoReflect().Descriptor()))
}
