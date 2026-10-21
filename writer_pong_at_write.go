package centrifuge

import (
	"slices"

	"github.com/centrifugal/protocol"

	"github.com/centrifugal/centrifuge/internal/queue"
)

// pingWrittenIn reports whether a drain of items wrote a ping. It is false at once when hook is
// nil, so with pong-at-write off the writer does no work per drain.
func pingWrittenIn(hook func(), items []queue.Item) bool {
	if hook == nil {
		return false
	}
	return slices.ContainsFunc(items, func(item queue.Item) bool {
		return item.FrameType == protocol.FrameTypeServerPing
	})
}
