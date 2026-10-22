package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifuge/internal/filter"
)

// newTagsFilterLimitNode returns a running node that lets its clients use tags filters, with the given config.
func newTagsFilterLimitNode(t *testing.T, config Config) *Node {
	t.Helper()
	config.LogLevel = LogLevelError
	config.LogHandler = func(entry LogEntry) {}
	node, err := New(config)
	require.NoError(t, err)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{AllowTagsFilter: true}}, nil)
		})
	})
	return node
}

// subscribeWithTagsFilter subscribes a new connection of node to a channel with the filter and returns the error of
// the subscription.
func subscribeWithTagsFilter(t *testing.T, node *Node, tf *protocol.FilterNode) error {
	t.Helper()
	client := newTestClientV2(t, node, "42")
	connectClientV2(t, client)
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "test", Tf: tf}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	if err != nil {
		return err
	}
	// A refused filter is answered with an error reply, not with a Go error.
	require.Len(t, rwWrapper.replies, 1)
	if rwWrapper.replies[0].Error != nil {
		return errors.New(rwWrapper.replies[0].Error.Message)
	}
	return nil
}

// tagsFilterInValues returns a filter of one comparison with the given number of values.
func tagsFilterInValues(count int) *protocol.FilterNode {
	vals := make([]string, count)
	for i := range vals {
		vals[i] = fmt.Sprint("v", i)
	}
	return &protocol.FilterNode{Cmp: filter.CompareIn, Key: "env", Vals: vals}
}

// tagsFilterNotChain returns a filter of the given number of NOT nodes over one comparison.
func tagsFilterNotChain(depth int) *protocol.FilterNode {
	node := &protocol.FilterNode{Cmp: filter.CompareEQ, Key: "env", Val: "prod"}
	for range depth {
		node = &protocol.FilterNode{Op: filter.OpNot, Nodes: []*protocol.FilterNode{node}}
	}
	return node
}

// TestClientTagsFilterLimitsRefuseLargeFilter checks that a configured limit refuses a client filter over it, and
// accepts one at the limit.
func TestClientTagsFilterLimitsRefuseLargeFilter(t *testing.T) {
	t.Parallel()
	node := newTagsFilterLimitNode(t, Config{ClientTagsFilterMaxNodes: 3, ClientTagsFilterMaxValues: 3})

	require.NoError(t, subscribeWithTagsFilter(t, node, tagsFilterInValues(3)))
	require.Error(t, subscribeWithTagsFilter(t, node, tagsFilterInValues(4)))
	require.NoError(t, subscribeWithTagsFilter(t, node, tagsFilterNotChain(2)))
	require.Error(t, subscribeWithTagsFilter(t, node, tagsFilterNotChain(3)))
}

// TestClientTagsFilterZeroLimitsAreUnlimited checks that a node without the limits accepts a client filter of any
// size that the filter validation accepts, so the limits are off unless they are set.
func TestClientTagsFilterZeroLimitsAreUnlimited(t *testing.T) {
	t.Parallel()
	node := newTagsFilterLimitNode(t, Config{})

	require.NoError(t, subscribeWithTagsFilter(t, node, tagsFilterInValues(2000)))
	require.NoError(t, subscribeWithTagsFilter(t, node, tagsFilterNotChain(1000)))

	negative := newTagsFilterLimitNode(t, Config{ClientTagsFilterMaxNodes: -1, ClientTagsFilterMaxValues: -1})
	require.NoError(t, subscribeWithTagsFilter(t, negative, tagsFilterInValues(2000)))
	require.NoError(t, subscribeWithTagsFilter(t, negative, tagsFilterNotChain(1000)))
}
