package webpush

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

func TestCheckHeadersAcceptsTheThreeNames(t *testing.T) {
	require.NoError(t, checkHeaders(map[string]string{
		"TTL":     "60",
		"Urgency": "high",
		"Topic":   "chat-1",
	}))
	require.NoError(t, checkHeaders(nil))
}

func TestCheckHeadersRefusesOtherNames(t *testing.T) {
	for _, name := range []string{"ttl", "Ttl", "urgency", "topic", "Authorization", "Content-Length"} {
		err := checkHeaders(map[string]string{name: "1"})

		require.Error(t, err, name)
		require.Contains(t, err.Error(), name)
		require.NotContains(t, err.Error(), "target", "a request-level error names no target")
	}
}

func TestCheckHeadersValueRules(t *testing.T) {
	testCases := []struct {
		name   string
		header map[string]string
		valid  bool
	}{
		{name: "TTL zero", header: map[string]string{"TTL": "0"}, valid: true},
		{name: "TTL four weeks", header: map[string]string{"TTL": "2419200"}, valid: true},
		{name: "TTL empty", header: map[string]string{"TTL": ""}, valid: false},
		{name: "TTL negative", header: map[string]string{"TTL": "-1"}, valid: false},
		{name: "TTL fraction", header: map[string]string{"TTL": "1.5"}, valid: false},
		{name: "TTL with a plus sign", header: map[string]string{"TTL": "+5"}, valid: false},
		{name: "urgency very-low", header: map[string]string{"Urgency": "very-low"}, valid: true},
		{name: "urgency low", header: map[string]string{"Urgency": "low"}, valid: true},
		{name: "urgency normal", header: map[string]string{"Urgency": "normal"}, valid: true},
		{name: "urgency high", header: map[string]string{"Urgency": "high"}, valid: true},
		{name: "urgency in capitals", header: map[string]string{"Urgency": "HIGH"}, valid: false},
		{name: "urgency unknown", header: map[string]string{"Urgency": "urgent"}, valid: false},
		{name: "topic of 32 characters", header: map[string]string{"Topic": strings.Repeat("a", 32)}, valid: true},
		{name: "topic of 33 characters", header: map[string]string{"Topic": strings.Repeat("a", 33)}, valid: false},
		{name: "topic with the URL-safe alphabet", header: map[string]string{"Topic": "A-z_09"}, valid: true},
		{name: "topic with a space", header: map[string]string{"Topic": "a b"}, valid: false},
		{name: "topic with a plus sign", header: map[string]string{"Topic": "a+b"}, valid: false},
		{name: "topic empty", header: map[string]string{"Topic": ""}, valid: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := checkHeaders(testCase.header)

			if testCase.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestCheckTopicOfTargetMustMatchTheCollapseID(t *testing.T) {
	require.Error(t, checkTopicOfTarget(map[string]string{"Topic": "first"}, "second", 0))
	require.NoError(t, checkTopicOfTarget(map[string]string{"Topic": "first"}, "first", 0))
	require.NoError(t, checkTopicOfTarget(map[string]string{"Topic": "first"}, "", 0))
	require.NoError(t, checkTopicOfTarget(nil, "second", 0))
}

func TestTopicComesFromTheCallerOrTheCollapseID(t *testing.T) {
	fromCollapse := newTestJob(0, "", nil, "{}")
	fromCollapse.CollapseID = "collapse-1"
	require.Equal(t, "collapse-1", topicOf(fromCollapse))

	fromCaller := newTestJob(0, "", map[string]string{"Topic": "caller-1"}, "{}")
	require.Equal(t, "caller-1", topicOf(fromCaller))

	none := newTestJob(0, "", nil, "{}")
	require.Empty(t, topicOf(none))
}

func TestInvalidResolvedTopicIsAnItemResult(t *testing.T) {
	job := newTestJob(0, newTokenFor(t, "https://fcm.googleapis.com/x"), nil, `{"a":1}`)
	job.CollapseID = "not a valid topic"

	require.Equal(t, provider.CodeInvalidTopic, Refusal(job, defaultChecker(t)))
	require.NoError(t, CheckRequest(job), "a resolved collapse id is an item result, not a request error")

	tooLong := newTestJob(0, newTokenFor(t, "https://fcm.googleapis.com/x"), nil, `{"a":1}`)
	tooLong.CollapseID = strings.Repeat("c", 33)
	require.Equal(t, provider.CodeInvalidTopic, Refusal(tooLong, defaultChecker(t)))
}

func TestRequestHeadersTTLFromExpireAt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	job := newTestJob(0, "", nil, "{}")
	job.Notification.ExpireAt = now.Unix() + 3600
	require.Equal(t, "3600", requestHeaders(job, now)[headerTTL])
}

func TestRequestHeadersTTLCallerWinsOverExpireAt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	job := newTestJob(0, "", map[string]string{"TTL": "60"}, "{}")
	job.Notification.ExpireAt = now.Unix() + 3600
	require.Equal(t, "60", requestHeaders(job, now)[headerTTL])
}

func TestRequestHeadersTTLDefaultsToFourWeeks(t *testing.T) {
	job := newTestJob(0, "", nil, "{}")

	require.Equal(t, strconv.FormatInt(2419200, 10), requestHeaders(job, time.Unix(1_700_000_000, 0))[headerTTL])
}

func TestRequestHeadersCarriesUrgencyAndTopic(t *testing.T) {
	job := newTestJob(0, "", map[string]string{"Urgency": "low"}, "{}")
	job.CollapseID = "collapse-9"

	headers := requestHeaders(job, time.Unix(1_700_000_000, 0))

	require.Equal(t, "low", headers[headerUrgency])
	require.Equal(t, "collapse-9", headers[headerTopic])
	require.Len(t, headers, 3)
}

func TestRequestHeadersOmitsUnsetOptionalHeaders(t *testing.T) {
	headers := requestHeaders(newTestJob(0, "", nil, "{}"), time.Unix(1_700_000_000, 0))

	require.Equal(t, map[string]string{headerTTL: "2419200"}, headers)
}
