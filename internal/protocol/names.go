package protocol

import (
	"regexp"
)

// #ephemeral为后缀代表临时channel，topic
var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

// 名称校验规则
func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
