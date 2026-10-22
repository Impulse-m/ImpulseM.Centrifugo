package provider

import (
	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// DryRun returns the result of a valid job in dry run mode. No provider is called, so the HTTP status is zero and
// there is no message id.
func DryRun(job *Job) *apiproto.PushItemResult {
	return newResult(job, StatusDryRun)
}
