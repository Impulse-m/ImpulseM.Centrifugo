package provider

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// fcmScope is the OAuth scope of the FCM HTTP v1 API.
const fcmScope = "https://www.googleapis.com/auth/firebase.messaging"

// maxKeyFileBytes bounds the APNs key file and the FCM credentials file. A P-256 PKCS#8 key is well under one kilobyte
// and a service account is a few kilobytes, so a larger file is a misconfiguration and is refused before it is read.
const maxKeyFileBytes = 64 << 10

// projectIDPattern is the shape of an FCM project id: a letter or digit, then letters, digits, dots, colons and hyphens,
// which also covers the domain-scoped form. A slash, a question mark or a dot-segment is refused, because the id is part
// of the endpoint path that carries the OAuth bearer.
var projectIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.:-]{0,127}$`)

// Configuration keys named in the load errors. An error names the key and never its content.
const (
	apnsKeyFileKey = "push_notifications.apns.token_key_file"
	apnsKeyPEMKey  = "push_notifications.apns.token_key_pem"
	fcmCredsKey    = "push_notifications.fcm.credentials_file"
)

// LoadAPNSKey loads the APNs signing key of a credential, from keyFile or, when keyFile is empty, from keyPEM. The
// key must be a PKCS#8 PEM P-256 private key. The error names the key configuration key, and never the key content
// or the library error text, which could carry it.
func LoadAPNSKey(keyFile string, keyPEM configtypes.Secret) (*ecdsa.PrivateKey, error) {
	if keyFile != "" {
		return loadAPNSKeyFile(keyFile)
	}

	data, _, err := configtypes.PEMData(keyPEM.Value()).Load(os.Stat, os.ReadFile)
	if err != nil {
		return nil, fmt.Errorf("%s: the key cannot be loaded", apnsKeyPEMKey)
	}
	return parseAPNSKey(data, apnsKeyPEMKey)
}

// loadAPNSKeyFile loads the key file through configtypes.PEMData. Its stat is capped: a file above maxKeyFileBytes is
// refused by the stat, before the read, and the error says so.
func loadAPNSKeyFile(keyFile string) (*ecdsa.PrivateKey, error) {
	tooLarge := false
	capStat := func(name string) (fs.FileInfo, error) {
		info, err := os.Stat(name)
		if err != nil {
			return nil, err
		}
		if info.Size() > maxKeyFileBytes {
			tooLarge = true
			return nil, errors.New("the key file is larger than the bound")
		}
		return info, nil
	}

	data, _, err := configtypes.PEMData(keyFile).Load(capStat, os.ReadFile)
	if err != nil {
		if tooLarge {
			return nil, fmt.Errorf("%s (%s): the key file is larger than %d bytes", apnsKeyFileKey, keyFile, maxKeyFileBytes)
		}
		return nil, fmt.Errorf("%s (%s): the key file cannot be loaded", apnsKeyFileKey, keyFile)
	}
	return parseAPNSKey(data, apnsKeyFileKey+" ("+keyFile+")")
}

// parseAPNSKey parses a PKCS#8 PEM P-256 private key. The source names the key in the error: its configuration key,
// with the file path when the key is a file.
func parseAPNSKey(data []byte, source string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%s: the key is not PEM", source)
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: the key is not a PKCS#8 private key", source)
	}

	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%s: the key is not a P-256 private key", source)
	}
	return key, nil
}

// FCMServiceAccount is the parsed service account of FCM: the project id of the endpoint and the JWT config of the
// token source.
type FCMServiceAccount struct {
	ProjectID string
	JWT       *jwt.Config
}

// LoadFCMServiceAccount parses the service account JSON of FCM from data read from credentials_file. The project id
// must be present. The error never carries the file content.
func LoadFCMServiceAccount(data []byte) (*FCMServiceAccount, error) {
	var fields struct {
		ProjectID string `json:"project_id"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return nil, fmt.Errorf("%s: the file is not a service account JSON", fcmCredsKey)
	}
	if fields.ProjectID == "" {
		return nil, errors.New(fcmCredsKey + ": the service account has no project_id")
	}
	if !projectIDPattern.MatchString(fields.ProjectID) {
		return nil, errors.New(fcmCredsKey + ": the service account project_id is not valid")
	}

	conf, err := google.JWTConfigFromJSON(data, fcmScope)
	if err != nil {
		return nil, fmt.Errorf("%s: the service account cannot be used", fcmCredsKey)
	}
	return &FCMServiceAccount{
		ProjectID: fields.ProjectID,
		JWT:       conf,
	}, nil
}

// ReadFCMServiceAccount reads the service account file named by credentials_file, refused above maxKeyFileBytes before
// it is read, and parses it with LoadFCMServiceAccount. The error names the key and the path, never the content.
func ReadFCMServiceAccount(path string) (*FCMServiceAccount, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s (%s): the file cannot be read", fcmCredsKey, path)
	}
	if info.Size() > maxKeyFileBytes {
		return nil, fmt.Errorf("%s (%s): the file is larger than %d bytes", fcmCredsKey, path, maxKeyFileBytes)
	}
	data, err := os.ReadFile(path) //nolint:gosec // The file is named by the operator's configuration.
	if err != nil {
		return nil, fmt.Errorf("%s (%s): the file cannot be read", fcmCredsKey, path)
	}
	return LoadFCMServiceAccount(data)
}
