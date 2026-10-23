package push

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/apns"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/fcm"
)

// Length bounds of the request fields.
const (
	maxUIDBytes        = 128
	maxRefBytes        = 128
	maxCollapseIDBytes = 64
)

// Size thresholds above which a payload is not checked for its content. Such a payload is over the size limit of every
// provider, so the provider refuses its item as payload_too_large, and it is never scanned: a shared request payload
// would otherwise be scanned once per target. The largest limit is the VoIP ceiling of APNs.
const (
	maxScannedAPNsPayloadBytes = 5120
	maxScannedFCMMessageBytes  = 4096
)

// errNotArray reports a recipient list that is not a JSON array.
var errNotArray = errors.New("recipient list is not an array")

// notEnabledError returns the answer of a node with push notifications off.
func notEnabledError() *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorNotAvailable.Code,
		Message: "push notifications are not enabled",
	}
}

// senderOnlyError returns the answer of a node that is not a sender.
func senderOnlyError() *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorNotAvailable.Code,
		Message: "direct send is available on sender nodes only",
	}
}

// tooManyCallsError returns the answer of a call over max_concurrent_requests. There is no queue, so the caller retries.
func tooManyCallsError() *apiproto.Error {
	return &apiproto.Error{
		Code:      apiproto.ErrorTooManyRequests.Code,
		Message:   apiproto.ErrorTooManyRequests.Message,
		Temporary: true,
	}
}

// internalError returns the answer of a call that cannot run for a reason of the node.
func internalError() *apiproto.Error {
	return &apiproto.Error{
		Code:      apiproto.ErrorInternal.Code,
		Message:   apiproto.ErrorInternal.Message,
		Temporary: true,
	}
}

// badRequest returns a refusal with the bad request code and a message that names the field.
func badRequest(message string) *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorBadRequest.Code,
		Message: message,
	}
}

// notAvailable returns a refusal with the not available code and a message that names the field.
func notAvailable(message string) *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorNotAvailable.Code,
		Message: message,
	}
}

// setRecipientFields returns the names of the recipient fields that are set in req. A field counts as set when it has
// an entry or a non-empty value, so an empty list is not a recipient kind.
func setRecipientFields(req *apiproto.SendPushNotificationRequest) []string {
	recipient := req.GetRecipient()
	var set []string
	if len(recipient.GetTargets()) > 0 {
		set = append(set, "targets")
	}
	if len(recipient.GetFcmTokens()) > 0 {
		set = append(set, "fcm_tokens")
	}
	if len(recipient.GetApnsTokens()) > 0 {
		set = append(set, "apns_tokens")
	}
	if recipient.GetFilter() != nil {
		set = append(set, "filter")
	}
	if recipient.GetFcmTopic() != "" {
		set = append(set, "fcm_topic")
	}
	if recipient.GetFcmCondition() != "" {
		set = append(set, "fcm_condition")
	}
	if len(recipient.GetHmsTokens()) > 0 {
		set = append(set, "hms_tokens")
	}
	if recipient.GetHmsTopic() != "" {
		set = append(set, "hms_topic")
	}
	if recipient.GetHmsCondition() != "" {
		set = append(set, "hms_condition")
	}
	if len(recipient.GetWebpushTokens()) > 0 {
		set = append(set, "webpush_tokens")
	}
	return set
}

// plan runs the request checks of the send path in their order, and returns the jobs of the targets. The first refusal
// is returned with no job, and no provider is called for the request. Checks that are per item, such as the size of a
// payload and an expired notification, are not refused here: they are the result of the item.
func (s *Sender) plan(req *apiproto.SendPushNotificationRequest) ([]*provider.Job, *apiproto.Error) {
	if int64(proto.Size(req)) > s.MaxRequestBytes() {
		return nil, badRequest("request is larger than max_request_bytes")
	}

	set := setRecipientFields(req)
	if len(set) != 1 {
		return nil, badRequest("recipient must be exactly one of targets, fcm_tokens or apns_tokens")
	}
	kind := set[0]
	switch kind {
	case "targets", "fcm_tokens", "apns_tokens":
	default:
		return nil, notAvailable(fmt.Sprintf("recipient %s is not available in this build", kind))
	}

	targets := s.recipientTargets(req)
	if len(targets) > s.maxRecipients {
		return nil, badRequest("recipient count is above max_recipient_items")
	}

	for index, target := range targets {
		switch target.GetProvider() {
		case provider.ProviderAPNs, provider.ProviderFCM:
		default:
			return nil, badRequest(fmt.Sprintf("targets[%d].provider must be apns or fcm", index))
		}
	}
	for _, target := range targets {
		if !s.providers[target.GetProvider()] {
			return nil, notAvailable(fmt.Sprintf("provider %s is not enabled", target.GetProvider()))
		}
	}

	if req.GetSendAt() != 0 {
		return nil, badRequest("send_at is not available in direct send mode")
	}
	if req.GetLimitStrategy() != nil {
		return nil, notAvailable("limit_strategy is not available in this build")
	}
	if len(req.GetLocalizations()) > 0 {
		return nil, notAvailable("localizations is not available in this build")
	}
	if req.GetUseTemplating() {
		return nil, notAvailable("use_templating is not available in this build")
	}
	if req.GetUseMeta() {
		return nil, notAvailable("use_meta is not available in this build")
	}
	if len(req.GetUid()) > maxUIDBytes {
		return nil, badRequest("uid is longer than 128 bytes")
	}

	jobs := jobsOf(targets, req)
	for _, job := range jobs {
		refusal := s.checkTarget(job, req)
		if refusal != nil {
			return nil, refusal
		}
	}
	return jobs, nil
}

// checkTarget runs the checks of one target: its fields, its payload and its headers.
func (s *Sender) checkTarget(job *provider.Job, req *apiproto.SendPushNotificationRequest) *apiproto.Error {
	target := job.Target
	index := job.Index

	if target.GetToken() == "" {
		return badRequest(fmt.Sprintf("targets[%d].token is required", index))
	}
	if len(target.GetRef()) > maxRefBytes {
		return badRequest(fmt.Sprintf("targets[%d].ref is longer than 128 bytes", index))
	}
	if len(target.GetCollapseId()) > maxCollapseIDBytes {
		return badRequest(fmt.Sprintf("targets[%d].collapse_id is longer than 64 bytes", index))
	}

	switch target.GetProvider() {
	case provider.ProviderAPNs:
		return s.checkAPNsTarget(job, req)
	case provider.ProviderFCM:
		return checkFCMTarget(job)
	default:
		return badRequest(fmt.Sprintf("targets[%d].provider must be apns or fcm", index))
	}
}

// checkAPNsTarget checks the environment, the payload and the headers of an APNs target.
func (s *Sender) checkAPNsTarget(job *provider.Job, req *apiproto.SendPushNotificationRequest) *apiproto.Error {
	index := job.Index
	environment := job.Target.GetEnvironment()
	if environment == "" {
		return badRequest(fmt.Sprintf("targets[%d].environment is required for apns", index))
	}
	if environment != environmentProduction && environment != environmentDevelopment {
		return badRequest(fmt.Sprintf("targets[%d].environment must be production or development", index))
	}

	payload := provider.ApnsPayloadOf(job)
	if len(payload) == 0 {
		return badRequest(fmt.Sprintf("targets[%d] needs an apns payload: notification.apns.payload or apns_payload", index))
	}
	if len(payload) <= maxScannedAPNsPayloadBytes && !isJSONObject(payload) {
		return badRequest(fmt.Sprintf("targets[%d] apns payload is not a JSON object", index))
	}

	err := apns.CheckHeaders(req.GetNotification().GetApns().GetHeaders(), job.CollapseID)
	if err != nil {
		return badRequest(fmt.Sprintf("targets[%d] %s", index, err.Error()))
	}
	return nil
}

// checkFCMTarget checks the message of an FCM target. A message over the scan threshold is left to its item.
func checkFCMTarget(job *provider.Job) *apiproto.Error {
	index := job.Index
	if job.Target.GetEnvironment() != "" {
		return badRequest(fmt.Sprintf("targets[%d].environment is refused for fcm", index))
	}

	message := provider.FCMMessageOf(job)
	if len(message) == 0 {
		return badRequest(fmt.Sprintf("targets[%d] needs an fcm message: notification.fcm.message or fcm_message", index))
	}
	if len(message) > maxScannedFCMMessageBytes {
		return nil
	}

	err := fcm.CheckMessage(message, job.CollapseID)
	if err != nil {
		return badRequest(fmt.Sprintf("targets[%d] fcm message: %s", index, err.Error()))
	}
	return nil
}

// isJSONObject reports whether data is one valid JSON object. The check of gjson looks at the first byte only, so the
// whole value is validated too.
func isJSONObject(data []byte) bool {
	return gjson.ValidBytes(data) && gjson.ParseBytes(data).IsObject()
}

// recipientCounter counts the entries of a recipient list while the body is decoded, without storing them. It stops at
// its limit, so a body with more entries costs no more than the limit to refuse.
type recipientCounter struct {
	limit int
	count int
	over  bool
}

// UnmarshalJSON counts the elements of the JSON array in data. A null is an empty list. A value that is not an array is
// an error, and the generated decoder then answers the request.
func (c *recipientCounter) UnmarshalJSON(data []byte) error {
	c.count = 0
	c.over = false

	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return errNotArray
	}

	for decoder.More() {
		var entry struct{}
		err = decoder.Decode(&entry)
		if err != nil {
			return err
		}
		c.count++
		if c.count > c.limit {
			c.over = true
			return nil
		}
	}
	return nil
}

// CheckRecipientCount refuses a request body whose recipient list is above max_recipient_items. It runs on the raw body
// before the generated decoder allocates the entries. A body that does not decode here is left to the generated decoder,
// which answers it.
func (s *Sender) CheckRecipientCount(body []byte) *apiproto.Error {
	var request struct {
		Recipient struct {
			Targets    recipientCounter `json:"targets"`
			FcmTokens  recipientCounter `json:"fcm_tokens"`
			ApnsTokens recipientCounter `json:"apns_tokens"`
		} `json:"recipient"`
	}
	request.Recipient.Targets.limit = s.maxRecipients
	request.Recipient.FcmTokens.limit = s.maxRecipients
	request.Recipient.ApnsTokens.limit = s.maxRecipients

	err := json.Unmarshal(body, &request)
	if err != nil {
		return nil
	}
	if request.Recipient.Targets.over || request.Recipient.FcmTokens.over || request.Recipient.ApnsTokens.over {
		return badRequest("recipient count is above max_recipient_items")
	}
	return nil
}
