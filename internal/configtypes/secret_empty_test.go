package configtypes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestEmptySecretShowsNothing checks that a Secret without a value prints and encodes as an empty string, so a generated
// config shows no bogus mask for an unset key.
func TestEmptySecretShowsNothing(t *testing.T) {
	var empty Secret

	if got := fmt.Sprintf("%v|%+v|%#v|%s", empty, empty, empty, empty); got != "|||" {
		t.Fatalf("formatted empty secret = %q, want no text", got)
	}

	text, err := empty.MarshalText()
	if err != nil || len(text) != 0 {
		t.Fatalf("MarshalText() = %q, %v; want empty text", text, err)
	}

	data, err := json.Marshal(struct {
		Key Secret `json:"key"`
	}{})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if string(data) != `{"key":""}` {
		t.Fatalf("json of empty secret = %s, want an empty string", data)
	}

	out, err := yaml.Marshal(struct {
		Key Secret `yaml:"key"`
	}{})
	if err != nil {
		t.Fatalf("yaml.Marshal: %v", err)
	}
	if strings.Contains(string(out), secretMask) {
		t.Fatalf("yaml of empty secret carries the mask: %s", out)
	}
}

// TestSetSecretShowsMask checks that a Secret with a value shows the mask in every form, as before.
func TestSetSecretShowsMask(t *testing.T) {
	secret := NewSecret("key-material")

	if got := fmt.Sprintf("%v", secret); got != secretMask {
		t.Fatalf("formatted secret = %q, want the mask", got)
	}
	if secret.String() != secretMask {
		t.Fatalf("String() = %q, want the mask", secret.String())
	}
}

// TestSecretIsScalarForConfigTooling checks that the config tooling treats Secret as one string value: the type label is
// string, and the field is not an object with children.
func TestSecretIsScalarForConfigTooling(t *testing.T) {
	secretType := reflect.TypeOf(Secret{})

	if label := TypeLabel(secretType); label != "string" {
		t.Fatalf("TypeLabel(Secret) = %q, want string", label)
	}
	if IsComplexType(secretType) {
		t.Fatal("IsComplexType(Secret) = true, want a scalar")
	}
	if IsComplexType(reflect.PointerTo(secretType)) {
		t.Fatal("IsComplexType(*Secret) = true, want a scalar")
	}
	if IsComplexType(reflect.SliceOf(secretType)) {
		t.Fatal("IsComplexType([]Secret) = true, want a scalar")
	}
}
