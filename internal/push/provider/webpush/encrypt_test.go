package webpush

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/webpush/vapidkey"
)

// Values of the example of RFC 8291 (rfc8291.txt). Each value is copied from the line numbers named in its comment. A value
// that the text wraps over two lines is joined without the wrap, and the body is the three lines of section 5 joined.
const (
	// Section 5, lines 414-415.
	rfcPlaintext = "When I grow up, I want to be a watermelon"
	// Line 422.
	rfcAuthSecret = "BTBZMqHH6r4Tts7J_aSIgg"
	// Line 424.
	rfcReceiverPrivate = "q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94"
	// Lines 425-426.
	rfcReceiverPublic = "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcx" +
		"aOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
	// Line 428.
	rfcSenderPrivate = "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw"
	// Lines 429-430.
	rfcSenderPublic = "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIg" +
		"Dll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8"
	// Line 646.
	rfcSalt = "DGv6ra1nlYgDCS1FRnbzlw"
	// Line 657.
	rfcECDHSecret = "kyrL1jIIOHEzg3sM2ZWRHDRB62YACZhhSlknJ672kSs"
	// Line 668.
	rfcIKM = "S4lYMb_L0FxCeq0WhDx813KgSYqU26kOyzWUdsXYyrg"
	// Line 685.
	rfcCEK = "oIhVW04MRdy2XN9CiKLxTg"
	// Line 690.
	rfcNonce = "4h_95klXJ5E_qnoN"
	// Lines 695-696, the 86-byte header.
	rfcHeader = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z" +
		"9KsN6nGRTbVYI_c7VJSPQTBtkgcy27ml" +
		"mlMoZIIgDll6e3vCYLocInmYWAmS6Tlz" +
		"AC8wEqKK6PBru3jl7A8"
	// Lines 410-412, the whole body.
	rfcBody = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27ml" +
		"mlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPT" +
		"pK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN"
)

// decodeRFC decodes a base64url value of the example, which carries no padding.
func decodeRFC(t *testing.T, value string) []byte {
	t.Helper()

	raw, err := base64.RawURLEncoding.DecodeString(value)
	require.NoError(t, err)
	return raw
}

// TestEncryptMatchesRFC8291Example checks the encryption against the example of RFC 8291 section 5 and the intermediate
// values of its appendix A: the ECDH secret, the input keying material, the content encryption key, the nonce, the header
// and the whole body.
func TestEncryptMatchesRFC8291Example(t *testing.T) {
	receiverPublic := decodeRFC(t, rfcReceiverPublic)
	senderPublic := decodeRFC(t, rfcSenderPublic)
	auth := decodeRFC(t, rfcAuthSecret)
	salt := decodeRFC(t, rfcSalt)

	ephemeral, err := ecdh.P256().NewPrivateKey(decodeRFC(t, rfcSenderPrivate))
	require.NoError(t, err)
	require.Equal(t, senderPublic, ephemeral.PublicKey().Bytes())
	receiver, err := ecdh.P256().NewPublicKey(receiverPublic)
	require.NoError(t, err)

	secret, err := ephemeral.ECDH(receiver)
	require.NoError(t, err)
	require.Equal(t, decodeRFC(t, rfcECDHSecret), secret)

	keyInfo := append([]byte("WebPush: info\x00"), receiverPublic...)
	keyInfo = append(keyInfo, senderPublic...)
	ikm, err := hkdf.Key(sha256.New, secret, auth, string(keyInfo), 32)
	require.NoError(t, err)
	require.Equal(t, decodeRFC(t, rfcIKM), ikm)

	cek, nonce, err := deriveKeys(secret, receiverPublic, senderPublic, auth, salt)
	require.NoError(t, err)
	require.Equal(t, decodeRFC(t, rfcCEK), cek)
	require.Equal(t, decodeRFC(t, rfcNonce), nonce)

	body, err := encrypt([]byte(rfcPlaintext), receiver, auth, ephemeral, salt)
	require.NoError(t, err)
	require.Equal(t, decodeRFC(t, rfcHeader), body[:saltLength+4+1+vapidkey.PublicKeyLength])
	require.Equal(t, decodeRFC(t, rfcBody), body)
}

// TestDecryptRFC8291ExampleWithTheReceiverKey checks that a receiver that holds the private key of the example opens the body
// of the example to its plaintext. The keys are derived on the receiver side from the header of the body: the salt and the
// sender public key.
func TestDecryptRFC8291ExampleWithTheReceiverKey(t *testing.T) {
	receiver, err := ecdh.P256().NewPrivateKey(decodeRFC(t, rfcReceiverPrivate))
	require.NoError(t, err)
	require.Equal(t, decodeRFC(t, rfcReceiverPublic), receiver.PublicKey().Bytes())

	plaintext := decryptForTest(t, decodeRFC(t, rfcBody), receiver, decodeRFC(t, rfcAuthSecret))

	require.Equal(t, rfcPlaintext, string(plaintext))
}

// TestEncryptBodyOfTheLargestPayloadIsOneRecord checks that the body of a payload of 3993 bytes, the largest one the
// subscription check accepts, is exactly 4096 bytes: the 86-byte header, the payload, the padding delimiter and the tag.
func TestEncryptBodyOfTheLargestPayloadIsOneRecord(t *testing.T) {
	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	auth := make([]byte, authLength)
	_, err = io.ReadFull(rand.Reader, auth)
	require.NoError(t, err)
	ephemeral, salt, err := systemRandom()
	require.NoError(t, err)

	body, err := encrypt(bytes.Repeat([]byte("p"), 3993), receiver.PublicKey(), auth, ephemeral, salt)

	require.NoError(t, err)
	require.Len(t, body, 4096)
}

// decryptForTest is a receiver written in the test: it reads the header of an aes128gcm body, derives the keys with its
// own calls of the HKDF, and opens the one record. It returns the payload without the padding delimiter.
func decryptForTest(t *testing.T, body []byte, receiver *ecdh.PrivateKey, auth []byte) []byte {
	t.Helper()

	salt := body[:saltLength]
	require.Equal(t, uint32(4096), binary.BigEndian.Uint32(body[16:20]))
	idLength := int(body[20])
	keyID := body[21 : 21+idLength]
	ciphertext := body[21+idLength:]

	sender, err := ecdh.P256().NewPublicKey(keyID)
	require.NoError(t, err)
	secret, err := receiver.ECDH(sender)
	require.NoError(t, err)

	keyInfo := append([]byte("WebPush: info\x00"), receiver.PublicKey().Bytes()...)
	keyInfo = append(keyInfo, keyID...)
	ikm, err := hkdf.Key(sha256.New, secret, auth, string(keyInfo), 32)
	require.NoError(t, err)
	cek, err := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: aes128gcm\x00", 16)
	require.NoError(t, err)
	nonce, err := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: nonce\x00", 12)
	require.NoError(t, err)

	block, err := aes.NewCipher(cek)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	require.NoError(t, err)

	require.NotEmpty(t, plaintext)
	require.Equal(t, byte(0x02), plaintext[len(plaintext)-1], "the last byte of the record is the padding delimiter")
	return plaintext[:len(plaintext)-1]
}

// TestEncryptRoundTripsThroughAReceiver checks that a receiver written in the test reads what the encryption produces, for
// a short payload, an empty one and a payload of the largest size.
func TestEncryptRoundTripsThroughAReceiver(t *testing.T) {
	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	auth := make([]byte, authLength)
	_, err = io.ReadFull(rand.Reader, auth)
	require.NoError(t, err)

	payloads := []string{
		`{"title":"hello"}`,
		"",
		strings.Repeat("p", provider.MaxWebPushPayloadBytes),
	}
	for _, payload := range payloads {
		ephemeral, salt, err := systemRandom()
		require.NoError(t, err)

		body, err := encrypt([]byte(payload), receiver.PublicKey(), auth, ephemeral, salt)
		require.NoError(t, err)

		require.Equal(t, payload, string(decryptForTest(t, body, receiver, auth)))
	}
}

// TestEncryptRefusesAWrongSalt checks that a salt that is not 16 bytes is an error, not a body with a wrong header.
func TestEncryptRefusesAWrongSalt(t *testing.T) {
	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	ephemeral, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)

	_, err = encrypt([]byte(`{}`), receiver.PublicKey(), make([]byte, authLength), ephemeral, make([]byte, 15))
	require.Error(t, err)
}

// BenchmarkEncryptPayload measures one target: a fresh ephemeral key and salt, then the encryption of a payload of the
// largest size. The figure is the cost of one message in the documentation.
func BenchmarkEncryptPayload(b *testing.B) {
	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	auth := make([]byte, authLength)
	if _, err := io.ReadFull(rand.Reader, auth); err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("p"), provider.MaxWebPushPayloadBytes)

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ephemeral, salt, err := systemRandom()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := encrypt(payload, receiver.PublicKey(), auth, ephemeral, salt); err != nil {
			b.Fatal(err)
		}
	}
}
