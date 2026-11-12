package apiproto

// ErrorPublishTagsLimit means that a publication has more tags, or more bytes of tags, than the
// publish_tags_max_count or publish_tags_max_bytes option of its namespace allows.
var ErrorPublishTagsLimit = &Error{
	Code:      1000,
	Message:   "publish tags limit exceeded",
	Temporary: false,
}
