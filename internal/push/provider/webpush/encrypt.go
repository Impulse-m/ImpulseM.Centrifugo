package webpush

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

// Sizes of the aes128gcm body of RFC 8188 and of the keys of RFC 8291. The record size is the one the body states: one
// record holds the whole payload, so the padding delimiter is the last byte of the record.
const (
	saltLength              = 16
	recordSize       uint32 = 4096
	paddingDelimiter byte   = 0x02
	ikmLength               = 32
	cekLength               = 16
	nonceLength             = 12
	keyIDLength             = 65
)

// keyInfoPrefix starts the info of the key combining step of RFC 8291.
const keyInfoPrefix = "WebPush: info\x00"

// cekInfo and nonceInfo are the infos of the content encryption key and of the nonce of RFC 8188.
const (
	cekInfo   = "Content-Encoding: aes128gcm\x00"
	nonceInfo = "Content-Encoding: nonce\x00"
)

// errSaltLength is the error of a salt that is not 16 bytes.
var errSaltLength error = errors.New("webpush salt must be 16 bytes")

// randomSource returns the ephemeral P-256 key and the 16-byte salt of one message. The default reads crypto/rand. A test
// injects the values of the example of RFC 8291, because the key of ecdh.GenerateKey does not depend on the bytes read.
type randomSource func() (*ecdh.PrivateKey, []byte, error)

// systemRandom returns a fresh ephemeral P-256 key and a fresh salt, both from crypto/rand.
func systemRandom() (*ecdh.PrivateKey, []byte, error) {
	ephemeral, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	salt := make([]byte, saltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, nil, err
	}
	return ephemeral, salt, nil
}

// encrypt returns the aes128gcm body of payload for a subscription with the receiver public key and the auth secret, as
// RFC 8291 gives it. The ephemeral key and the salt are the ones given, so the body is reproducible. The body is the header
// (salt, record size, the length of the key id and the ephemeral public key), then the ciphertext of the payload with the
// padding delimiter, and the tag.
func encrypt(payload []byte, receiver *ecdh.PublicKey, auth []byte, ephemeral *ecdh.PrivateKey, salt []byte) ([]byte, error) {
	if len(salt) != saltLength {
		return nil, errSaltLength
	}

	ephemeralPublic := ephemeral.PublicKey().Bytes()
	secret, err := ephemeral.ECDH(receiver)
	if err != nil {
		return nil, err
	}

	cek, nonce, err := deriveKeys(secret, receiver.Bytes(), ephemeralPublic, auth, salt)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(cek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintext := make([]byte, 0, len(payload)+1)
	plaintext = append(plaintext, payload...)
	plaintext = append(plaintext, paddingDelimiter)
	ciphertext := aead.Seal(nil, nonce, plaintext, nil)

	body := make([]byte, 0, saltLength+4+1+keyIDLength+len(ciphertext))
	body = append(body, salt...)
	body = binary.BigEndian.AppendUint32(body, recordSize)
	body = append(body, byte(len(ephemeralPublic)))
	body = append(body, ephemeralPublic...)
	body = append(body, ciphertext...)
	return body, nil
}

// deriveKeys returns the content encryption key and the nonce of one message. The shared secret is combined with the auth
// secret into the input keying material, which takes the two public keys in its info, and the content encryption key and
// the nonce are expanded from it with the salt.
func deriveKeys(secret []byte, receiverPublic []byte, ephemeralPublic []byte, auth []byte, salt []byte) ([]byte, []byte, error) {
	keyInfo := make([]byte, 0, len(keyInfoPrefix)+len(receiverPublic)+len(ephemeralPublic))
	keyInfo = append(keyInfo, keyInfoPrefix...)
	keyInfo = append(keyInfo, receiverPublic...)
	keyInfo = append(keyInfo, ephemeralPublic...)

	ikm, err := hkdf.Key(sha256.New, secret, auth, string(keyInfo), ikmLength)
	if err != nil {
		return nil, nil, err
	}
	cek, err := hkdf.Key(sha256.New, ikm, salt, cekInfo, cekLength)
	if err != nil {
		return nil, nil, err
	}
	nonce, err := hkdf.Key(sha256.New, ikm, salt, nonceInfo, nonceLength)
	if err != nil {
		return nil, nil, err
	}
	return cek, nonce, nil
}
