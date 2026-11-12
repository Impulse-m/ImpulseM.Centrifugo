package api

import (
	"strings"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/rs/zerolog/log"
)

// serverTagsReservedPrefix is the prefix of the keys which Centrifugo keeps for itself. A server tags key must not start
// with it.
const serverTagsReservedPrefix = "__"

// tagsSize is the size of the tags of one publish request: the number of entries of both maps, and the sum of the
// lengths of their keys and values.
type tagsSize struct {
	count int
	bytes int
}

// measureTags returns the size of tags and server tags together.
func measureTags(tags map[string]string, serverTags map[string]string) tagsSize {
	var size tagsSize
	for key, value := range tags {
		size.count++
		size.bytes += len(key) + len(value)
	}
	for key, value := range serverTags {
		size.count++
		size.bytes += len(key) + len(value)
	}
	return size
}

// requestTags are the tags of one publish or broadcast request. Their size is measured at most once, and only when a
// channel of the request has a cap, since the channels of a broadcast ask for it concurrently.
type requestTags struct {
	tags       map[string]string
	serverTags map[string]string
	once       sync.Once
	size       tagsSize
}

// newRequestTags returns the tags of one request, which the channels of the request check.
func newRequestTags(tags map[string]string, serverTags map[string]string) *requestTags {
	return &requestTags{tags: tags, serverTags: serverTags}
}

// measured returns the size of the tags, measuring them on the first call.
func (t *requestTags) measured() tagsSize {
	t.once.Do(func() {
		t.size = measureTags(t.tags, t.serverTags)
	})
	return t.size
}

// checkServerTagsKeys refuses server tags keys which are empty, which start with the reserved prefix, or which are also
// a key of tags, with error 107. It checks nothing when the request sends no server tags. The error names no key, and
// neither does any log line of the check.
func checkServerTagsKeys(tags map[string]string, serverTags map[string]string) *apiproto.Error {
	for key := range serverTags {
		if key == "" || strings.HasPrefix(key, serverTagsReservedPrefix) {
			return apiproto.ErrorBadRequest
		}
		if _, ok := tags[key]; ok {
			return apiproto.ErrorBadRequest
		}
	}
	return nil
}

// checkServerTagsAllowed refuses a publication with server tags into a channel of a namespace which does not allow them,
// with error 108. Server tags are carried by stream channels only, so a channel of another subscription type is refused
// the same way. A request without server tags is not checked.
func checkServerTagsAllowed(chOpts *configtypes.ChannelOptions, serverTags map[string]string) *apiproto.Error {
	if len(serverTags) == 0 {
		return nil
	}
	if !chOpts.AllowServerTags {
		return apiproto.ErrorNotAvailable
	}
	switch chOpts.SubscriptionType {
	case "", "stream":
		return nil
	default:
		return apiproto.ErrorNotAvailable
	}
}

// checkTagsLimits refuses a publication whose tags are over the publish_tags_max_count or the publish_tags_max_bytes
// option of its namespace, with error 1000. Zero means no limit, and the tags are not measured when neither option is
// set. The limits count tags and server tags together, and they apply to a publication which sends no server tags too,
// since the tags cost storage and fan-out bytes either way.
func checkTagsLimits(chOpts *configtypes.ChannelOptions, tags *requestTags) *apiproto.Error {
	if chOpts.PublishTagsMaxCount <= 0 && chOpts.PublishTagsMaxBytes <= 0 {
		return nil
	}
	size := tags.measured()
	if chOpts.PublishTagsMaxCount > 0 && size.count > chOpts.PublishTagsMaxCount {
		return apiproto.ErrorPublishTagsLimit
	}
	if chOpts.PublishTagsMaxBytes > 0 && size.bytes > chOpts.PublishTagsMaxBytes {
		return apiproto.ErrorPublishTagsLimit
	}
	return nil
}

// checkPublishTags runs the checks of the tags of one publication which depend on its channel: the server tags gate of
// the namespace, then the caps. The keys are checked once per request by checkServerTagsKeys.
func checkPublishTags(chOpts *configtypes.ChannelOptions, tags *requestTags) *apiproto.Error {
	if err := checkServerTagsAllowed(chOpts, tags.serverTags); err != nil {
		return err
	}
	return checkTagsLimits(chOpts, tags)
}

// checkPublishServerTags checks the server tags of a publish request before anything is published, so a refused publish
// stores nothing. It checks the keys, then the gate and the caps of its channel. Its log lines name no key.
func checkPublishServerTags(cmd *apiproto.PublishRequest, chOpts *configtypes.ChannelOptions) *apiproto.Error {
	if respError := checkServerTagsKeys(cmd.GetTags(), cmd.GetServerTags()); respError != nil {
		log.Error().Str("channel", cmd.Channel).Msg("bad publish request: server tags keys refused")
		return respError
	}
	if respError := checkPublishTags(chOpts, newRequestTags(cmd.GetTags(), cmd.GetServerTags())); respError != nil {
		log.Warn().Str("channel", cmd.Channel).Msg("publish refused: server tags gate or tags limit")
		return respError
	}
	return nil
}

// checkBroadcastServerTags checks the server tags keys of a broadcast request. The keys are the same for every channel,
// so a bad key refuses the whole request. It returns the tags which the channels of the request check.
func checkBroadcastServerTags(cmd *apiproto.BroadcastRequest) (*requestTags, *apiproto.Error) {
	if respError := checkServerTagsKeys(cmd.GetTags(), cmd.GetServerTags()); respError != nil {
		log.Error().Msg("bad broadcast request: server tags keys refused")
		return nil, respError
	}
	return newRequestTags(cmd.GetTags(), cmd.GetServerTags()), nil
}

// checkBroadcastChannelServerTags checks the gate and the caps of one channel of a broadcast. A refusal is for that
// channel only: it is counted in the metrics, and the caller returns it as the response of the channel.
func (h *Executor) checkBroadcastChannelServerTags(ch string, chOpts *configtypes.ChannelOptions, tags *requestTags) *apiproto.Error {
	if respError := checkPublishTags(chOpts, tags); respError != nil {
		metrics.IncAPIError(h.config.Protocol, "broadcast_publish", respError.Code)
		log.Warn().Str("channel", ch).Msg("broadcast refused for channel: server tags gate or tags limit")
		return respError
	}
	return nil
}
