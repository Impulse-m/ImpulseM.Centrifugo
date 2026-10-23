package push

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

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
	maxScannedAPNsPayloadBytes = provider.MaxAPNsVoIPPayloadBytes
	maxScannedFCMMessageBytes  = provider.MaxFCMMessageBytes
)

// NotEnabledError returns the answer of a node with push notifications off.
func NotEnabledError() *apiproto.Error {
	return &apiproto.Error{
		Code:    apiproto.ErrorNotAvailable.Code,
		Message: "push notifications are not enabled",
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
	// The field checks of every target run before the content checks of any target, in the order of the spec.
	for _, job := range jobs {
		refusal := checkTargetFields(job)
		if refusal != nil {
			return nil, refusal
		}
	}
	for _, job := range jobs {
		refusal := checkTargetContent(job, req)
		if refusal != nil {
			return nil, refusal
		}
	}
	return jobs, nil
}

// checkTargetFields runs the field checks of one target: its token, its environment, its ref and its collapse id.
func checkTargetFields(job *provider.Job) *apiproto.Error {
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

	environment := target.GetEnvironment()
	switch target.GetProvider() {
	case provider.ProviderFCM:
		if environment != "" {
			return badRequest(fmt.Sprintf("targets[%d].environment is refused for fcm", index))
		}
	default:
		if environment == "" {
			return badRequest(fmt.Sprintf("targets[%d].environment is required for apns", index))
		}
		if environment != provider.EnvironmentProduction && environment != provider.EnvironmentDevelopment {
			return badRequest(fmt.Sprintf("targets[%d].environment must be production or development", index))
		}
	}
	return nil
}

// checkTargetContent runs the content checks of one target: its payload or message, and for APNs its headers.
func checkTargetContent(job *provider.Job, req *apiproto.SendPushNotificationRequest) *apiproto.Error {
	switch job.Target.GetProvider() {
	case provider.ProviderFCM:
		return checkFCMTarget(job)
	default:
		return checkAPNsTarget(job, req)
	}
}

// checkAPNsTarget checks the payload and the headers of an APNs target.
func checkAPNsTarget(job *provider.Job, req *apiproto.SendPushNotificationRequest) *apiproto.Error {
	index := job.Index

	payload := provider.ApnsPayloadOf(job)
	if len(payload) == 0 {
		return badRequest(fmt.Sprintf("targets[%d] needs an apns payload: notification.apns.payload or apns_payload", index))
	}
	if len(payload) <= maxScannedAPNsPayloadBytes && !provider.IsJSONObject(payload) {
		return badRequest(fmt.Sprintf("targets[%d] apns payload is not a JSON object", index))
	}

	err := apns.CheckHeaders(req.GetNotification().GetApns().GetHeaders(), job.CollapseID)
	if err != nil {
		return badRequest(fmt.Sprintf("targets[%d] %s", index, err.Error()))
	}
	return nil
}

// checkFCMTarget checks the message of an FCM target. A message over the scan threshold is left to its item, which is
// answered payload_too_large. This is the one exception to the content checks of step 10 of the spec.
func checkFCMTarget(job *provider.Job) *apiproto.Error {
	index := job.Index

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

// errOverRecipientLimit reports that a recipient list of the body is above max_recipient_items.
var errOverRecipientLimit = errors.New("recipient count is above max_recipient_items")

// errMalformedRequest reports a request body that the recipient walk cannot read as the request: not one JSON object,
// a recipient that is not an object, a recipient list that is not an array, or a trailing value.
var errMalformedRequest = errors.New("request body is not a valid send request")

// Keys of the request body that the recipient walk reads. encoding/json matches a field name without regard to case,
// so the walk does too.
const (
	recipientKey  = "recipient"
	targetsKey    = "targets"
	fcmTokensKey  = "fcm_tokens"
	apnsTokensKey = "apns_tokens"
)

// Indexes of the recipient lists in a recipientWalk.
const (
	listTargets = iota
	listFCMTokens
	listAPNsTokens
	listCount
)

// recipientListKeys names the recipient lists by their index.
var recipientListKeys = [listCount]string{targetsKey, fcmTokensKey, apnsTokensKey}

// walkRole is the role of a JSON value in the recipient walk of a request body.
type walkRole int

const (
	// roleRequest is the request object itself.
	roleRequest walkRole = iota
	// roleRecipient is the recipient object of the request.
	roleRecipient
	// roleList is a recipient list: each of its elements is one entry.
	roleList
	// roleOther is any other value, which is skipped.
	roleOther
)

// maxWalkDepth bounds the nesting of the recipient walk. It is the depth limit of encoding/json, so a body the walk
// accepts is one the generated decoder can read, and a deeper body is refused without growing the walk stack.
const maxWalkDepth = 10000

// walkFrame is one open object or array of the recipient walk.
type walkFrame struct {
	object bool
	role   walkRole
	list   int
}

// recipientWalk counts the entries of the recipient lists of a request body. It reads the body as a token stream, so no
// entry is decoded into a value, and it sums the entries of every occurrence of a list key, because the generated
// decoder may keep or merge any of them.
type recipientWalk struct {
	limit  int
	counts [listCount]int
}

// CheckRecipientCount refuses a request body whose recipient list is above max_recipient_items, before the generated
// decoder allocates the entries. A body that the walk cannot read as a send request is refused too, so a body the
// check does not understand is never passed on.
func (s *Sender) CheckRecipientCount(body []byte) *apiproto.Error {
	walk := recipientWalk{limit: s.maxRecipients}
	err := walk.run(body)
	if errors.Is(err, errOverRecipientLimit) {
		return badRequest(errOverRecipientLimit.Error())
	}
	if err != nil {
		return badRequest(errMalformedRequest.Error())
	}
	return nil
}

// run walks body once and returns errOverRecipientLimit at the first recipient list over the limit, or an error for a
// body that is not a send request.
func (w *recipientWalk) run(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil {
		return errMalformedRequest
	}
	if !isDelim(token, '{') {
		return errMalformedRequest
	}

	stack := []walkFrame{{object: true, role: roleRequest}}
	for len(stack) > 0 {
		top := len(stack) - 1
		var role walkRole
		var list int

		if stack[top].object {
			token, err = decoder.Token()
			if err != nil {
				return errMalformedRequest
			}
			if isDelim(token, '}') {
				stack = stack[:top]
				continue
			}
			key, ok := token.(string)
			if !ok {
				return errMalformedRequest
			}
			role, list = childRole(stack[top].role, key)
			token, err = decoder.Token()
			if err != nil {
				return errMalformedRequest
			}
		} else {
			token, err = decoder.Token()
			if err != nil {
				return errMalformedRequest
			}
			if isDelim(token, ']') {
				stack = stack[:top]
				continue
			}
			// An element of a recipient list is counted by its list. Its value is walked with the role of other values.
			role = roleOther
			if stack[top].role == roleList {
				list = stack[top].list
				w.counts[list]++
				if w.counts[list] > w.limit {
					return errOverRecipientLimit
				}
			}
		}

		stack, err = w.value(stack, token, role, list)
		if err != nil {
			return err
		}
	}

	_, err = decoder.Token()
	if !errors.Is(err, io.EOF) {
		return errMalformedRequest
	}
	return nil
}

// value opens the frame of a value token, or checks a scalar, for a value of the given role. A recipient must be an
// object and a recipient list must be an array; null is allowed in both.
func (w *recipientWalk) value(stack []walkFrame, token json.Token, role walkRole, list int) ([]walkFrame, error) {
	delim, isDelimToken := token.(json.Delim)
	if isDelimToken {
		if len(stack) >= maxWalkDepth {
			return nil, errMalformedRequest
		}
		switch delim {
		case '{':
			if role == roleList {
				return nil, errMalformedRequest
			}
			return append(stack, walkFrame{object: true, role: role}), nil
		case '[':
			if role == roleRecipient {
				return nil, errMalformedRequest
			}
			return append(stack, walkFrame{role: role, list: list}), nil
		default:
			return nil, errMalformedRequest
		}
	}
	if token != nil && (role == roleRecipient || role == roleList) {
		return nil, errMalformedRequest
	}
	return stack, nil
}

// childRole returns the role of the value of key in an object of role parent, and the index of the list when it is a
// recipient list.
func childRole(parent walkRole, key string) (walkRole, int) {
	switch parent {
	case roleRequest:
		if strings.EqualFold(key, recipientKey) {
			return roleRecipient, 0
		}
	case roleRecipient:
		for index, name := range recipientListKeys {
			if strings.EqualFold(key, name) {
				return roleList, index
			}
		}
	}
	return roleOther, 0
}

// isDelim reports whether token is the delimiter delim.
func isDelim(token json.Token, delim json.Delim) bool {
	value, ok := token.(json.Delim)
	return ok && value == delim
}
