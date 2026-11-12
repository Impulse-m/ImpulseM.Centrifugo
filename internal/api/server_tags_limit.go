package api

import (
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
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

// measureTags returns the size of tags and server tags together. A request computes it once, not once per channel.
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
// with error 108. Server tags are carried by stream channels only in this phase, so a channel of another subscription
// type is refused the same way. A request without server tags is not checked.
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
// option of its namespace, with error 1000. Zero means no limit. The limits count tags and server tags together, and
// they apply to a publication which sends no server tags too, since the tags cost storage and fan-out bytes either way.
func checkTagsLimits(chOpts *configtypes.ChannelOptions, size tagsSize) *apiproto.Error {
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
func checkPublishTags(chOpts *configtypes.ChannelOptions, serverTags map[string]string, size tagsSize) *apiproto.Error {
	if err := checkServerTagsAllowed(chOpts, serverTags); err != nil {
		return err
	}
	return checkTagsLimits(chOpts, size)
}
