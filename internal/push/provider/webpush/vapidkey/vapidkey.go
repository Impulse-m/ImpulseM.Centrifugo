// Package vapidkey parses the keys of a VAPID key pair. The keys are P-256 keys in base64url form: the
// public key is the uncompressed point the frontend uses as its application server key, and the
// private key is the scalar that signs the Web Push token. It uses the standard library only, so that
// the configuration and the provider can both import it.
package vapidkey

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"errors"
	"strings"
)

// publicKeyLength is the length of an uncompressed P-256 point: a 0x04 prefix and two 32-byte coordinates.
const publicKeyLength int = 65

// privateKeyLength is the length of a P-256 private scalar.
const privateKeyLength int = 32

// Parse returns the private key of a VAPID key pair given as base64url strings. It checks that the
// public key is a point of P-256, that the private key is a scalar of P-256, and that the public key
// is the one of the private key. The strings are base64url with or without "=" padding. Errors name
// what is wrong and never contain key material.
func Parse(publicKey string, privateKey string) (*ecdsa.PrivateKey, error) {
	public, err := decodePublic(publicKey)
	if err != nil {
		return nil, err
	}
	raw, err := decodeBase64URL(privateKey)
	if err != nil {
		return nil, errors.New("private key is not base64url")
	}
	if len(raw) != privateKeyLength {
		return nil, errors.New("private key is not a P-256 scalar")
	}
	private, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), raw)
	if err != nil {
		return nil, errors.New("private key is not a P-256 scalar")
	}
	derived, err := private.PublicKey.ECDH()
	if err != nil {
		return nil, errors.New("private key does not match the public key")
	}
	if !bytes.Equal(derived.Bytes(), public) {
		return nil, errors.New("private key does not match the public key")
	}
	return private, nil
}

// CheckPublic reports whether publicKey is the base64url form of an uncompressed P-256 point. The
// string may carry "=" padding. The error names what is wrong and never contains the key.
func CheckPublic(publicKey string) error {
	_, err := decodePublic(publicKey)
	return err
}

// decodePublic decodes the base64url form of a public key and checks that it is a point of P-256.
func decodePublic(publicKey string) ([]byte, error) {
	raw, err := decodeBase64URL(publicKey)
	if err != nil {
		return nil, errors.New("public key is not base64url")
	}
	if len(raw) != publicKeyLength {
		return nil, errors.New("public key is not a P-256 point")
	}
	if _, err := ecdh.P256().NewPublicKey(raw); err != nil {
		return nil, errors.New("public key is not a P-256 point")
	}
	return raw, nil
}

// decodeBase64URL decodes base64url with or without "=" padding.
func decodeBase64URL(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
}
