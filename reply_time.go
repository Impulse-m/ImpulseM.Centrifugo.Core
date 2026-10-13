package centrifuge

import (
	"time"
)

// replyTime returns server time in Unix milliseconds for command replies, or zero
// when Config.ReplyIncludeServerTime is disabled.
func (c *Client) replyTime() int64 {
	if !c.node.config.ReplyIncludeServerTime {
		return 0
	}
	return time.Now().UnixMilli()
}
