package jwks

import (
	"errors"
	"io"
)

// maxBodyBytes bounds the body of a JWKS response. A key set is a few kilobytes, so a larger body is a broken or hostile
// endpoint, and it is refused instead of read into memory on each refresh.
const maxBodyBytes = 1 << 20

// errBodyTooLarge is returned when the body of a JWKS response is over maxBodyBytes.
var errBodyTooLarge = errors.New("jwks: response body too large")

// readBoundedBody reads a JWKS response body of at most maxBodyBytes. One byte over the limit is read, so a body of
// exactly the limit is accepted and a larger one is seen and refused with errBodyTooLarge.
func readBoundedBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBodyBytes {
		return nil, errBodyTooLarge
	}
	return data, nil
}
