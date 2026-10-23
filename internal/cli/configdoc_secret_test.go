package cli

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// TestConfigDocSecretIsStringField checks that the config doc shows the key material of push notifications as a string
// field: a scalar with no children, which gets an environment variable line. The field is the one a generated config
// shows, so the doc must not present it as an object.
func TestConfigDocSecretIsStringField(t *testing.T) {
	docs := documentStruct(&configtypes.PushAPNs{}, "push_notifications.apns", 0)

	var keyDoc *FieldDoc
	for i := range docs {
		if docs[i].Name == "token_key_pem" {
			keyDoc = &docs[i]
		}
	}
	if keyDoc == nil {
		t.Fatal("token_key_pem is not in the config doc")
	}
	if keyDoc.Type != "string" {
		t.Fatalf("token_key_pem type = %q, want string", keyDoc.Type)
	}
	if keyDoc.IsComplexType {
		t.Fatal("token_key_pem is an object in the config doc, want a scalar")
	}
	if len(keyDoc.Children) != 0 {
		t.Fatalf("token_key_pem has %d children, want none", len(keyDoc.Children))
	}
}
