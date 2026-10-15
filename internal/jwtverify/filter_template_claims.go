package jwtverify

import (
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"

	"github.com/tidwall/gjson"
)

// filterTemplateClaims reads the claims which the server tags filter templates of the config refer to. The
// paths are the ones of the config the token is verified under, and the values are kept with them, so a
// connection renders its templates with the values its token had. Without templates nothing is read and the
// zero value is returned.
func (verifier *VerifierJWT) filterTemplateClaims(claims []byte) filtertemplate.Claims {
	paths := verifier.cfgContainer.ConfigRef().ServerTagsFilterClaims()
	if len(paths) == 0 {
		return filtertemplate.Claims{}
	}
	values := make([]string, len(paths))
	for i, path := range paths {
		values[i] = scalarClaimValue(gjson.GetBytes(claims, path))
	}
	return filtertemplate.Claims{Paths: paths, Values: values}
}

// scalarClaimValue returns the text of a scalar claim. A number is its text as written in the token, as String
// rebuilds the text of a fraction or an exponent. Absent, null, object and array claims, and an empty string,
// have no value, which is "".
func scalarClaimValue(value gjson.Result) string {
	switch value.Type {
	case gjson.Number:
		return value.Raw
	case gjson.String, gjson.True, gjson.False:
		return value.String()
	}
	return ""
}
