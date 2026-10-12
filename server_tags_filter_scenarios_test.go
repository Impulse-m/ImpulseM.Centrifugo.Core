package centrifuge

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifuge/internal/filter"
)

// Scenarios of the server tags filter which the fork kept from its own implementation,
// ported onto upstream's API. History and recovery scenarios are covered by upstream tests.

// tagsFilterTestNode returns node which applies the same subscribe options to every subscription.
func tagsFilterTestNode(t *testing.T, options SubscribeOptions) *Node {
	t.Helper()
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{
				UserID: "user",
			},
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: options}, nil)
		})
	})
	return node
}

// hubServerTagsFilter returns server tags filter registered in hub for client subscription on channel.
func hubServerTagsFilter(t *testing.T, client *Client, channel string) *protocol.FilterNode {
	t.Helper()
	shard := client.node.hub.subShards[index(channel, numHubShards)]
	shard.mu.RLock()
	sub, ok := shard.subs[channel][client.uid]
	shard.mu.RUnlock()
	require.True(t, ok, "subscription must be registered in hub")
	if sub.serverTagsFilter == nil {
		return nil
	}
	return sub.serverTagsFilter.filter
}

// newTagsFilterRefreshClient returns connected client with client-side subscription refresh
// on channel "test" subscribed with the given options.
func newTagsFilterRefreshClient(t *testing.T, options SubscribeOptions) *Client {
	t.Helper()
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{
				UserID: "user",
			},
			ClientSideRefresh: true,
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           options,
				ClientSideRefresh: true,
			}, nil)
		})
	})

	transport := newTestTransport(func() {})
	client := newTestClientCustomTransport(t, context.Background(), node, transport, "user")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "test"}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	return client
}

// refreshTagsFilterSub performs client-side subscription refresh on channel "test"
// replying with the given server tags filter (nil keeps current one).
func refreshTagsFilterSub(t *testing.T, client *Client, serverTf *protocol.FilterNode) {
	t.Helper()
	client.OnSubRefresh(func(_ SubRefreshEvent, cb SubRefreshCallback) {
		cb(SubRefreshReply{
			ExpireAt:         time.Now().Unix() + 60,
			ServerTagsFilter: serverTf,
		}, nil)
	})
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestServerTagsFilter_LiveDeliveryWithoutClientFilter(t *testing.T) {
	node := tagsFilterTestNode(t, SubscribeOptions{
		// Server filter must not depend on AllowTagsFilter.
		AllowTagsFilter:  false,
		ServerTagsFilter: filter.Eq("env", "prod"),
	})

	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	client := newTestClientCustomTransport(t, context.Background(), node, transport, "user")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	_, err := node.Publish("test", []byte(`{"text": "staging message"}`), WithTags(map[string]string{"env": "staging"}))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "prod message"}`), WithTags(map[string]string{"env": "prod"}))
	require.NoError(t, err)

	// Publications are delivered in order, so if the staging one was delivered it arrives first.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case data := <-transport.sink:
			require.NotContains(t, string(data), "staging message", "publication not matching server filter must not be delivered")
			if strings.Contains(string(data), "prod message") {
				return
			}
		case <-timeout:
			require.FailNow(t, "matching publication was not delivered")
		}
	}
}

func TestServerTagsFilter_DisablesDelta(t *testing.T) {
	tests := []struct {
		name          string
		serverFilter  *protocol.FilterNode
		expectedDelta bool
	}{
		{
			name:          "delta negotiated without server filter",
			serverFilter:  nil,
			expectedDelta: true,
		},
		{
			name:          "delta not negotiated with server filter",
			serverFilter:  filter.Eq("env", "prod"),
			expectedDelta: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := tagsFilterTestNode(t, SubscribeOptions{
				AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				ServerTagsFilter:  tt.serverFilter,
			})

			transport := newTestTransport(func() {})
			client := newTestClientCustomTransport(t, context.Background(), node, transport, "user")
			connectClientV2(t, client)

			rwWrapper := testReplyWriterWrapper()
			err := client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "test",
				Delta:   string(DeltaTypeFossil),
			}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
			require.NoError(t, err)
			require.Nil(t, rwWrapper.replies[0].Error)
			require.NotNil(t, rwWrapper.replies[0].Subscribe)
			require.Equal(t, tt.expectedDelta, rwWrapper.replies[0].Subscribe.Delta)
		})
	}
}

func TestServerTagsFilter_SubRefreshReplacesFilter(t *testing.T) {
	client := newTagsFilterRefreshClient(t, SubscribeOptions{
		ServerTagsFilter: filter.Eq("env", "prod"),
	})
	require.Equal(t, filter.Eq("env", "prod"), hubServerTagsFilter(t, client, "test"))

	refreshTagsFilterSub(t, client, filter.Eq("env", "staging"))

	require.Equal(t, filter.Eq("env", "staging"), hubServerTagsFilter(t, client, "test"))
}

func TestServerTagsFilter_SubRefreshNilKeepsFilter(t *testing.T) {
	client := newTagsFilterRefreshClient(t, SubscribeOptions{
		ServerTagsFilter: filter.Eq("env", "prod"),
	})

	refreshTagsFilterSub(t, client, nil)

	require.Equal(t, filter.Eq("env", "prod"), hubServerTagsFilter(t, client, "test"))
	client.mu.RLock()
	chCtx := client.channels["test"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagServerTagsFilter))
}
