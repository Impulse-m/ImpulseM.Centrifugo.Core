package centrifuge

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// History is refused to a subscriber narrowed by a server tags filter. The
// refusal is decided twice: on entry, and again in the history callback, since
// the application decides in between and the subscription may change meanwhile.

// A subscribe which commits while the application decides on history must not
// get the history served: the check on entry saw only the reservation, but the
// filtered subscription is in place when the callback runs.
func TestClientHistoryRefusedWhenSubscribeCommitsDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	releaseSub := make(chan struct{})
	subDone := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			go func() {
				select {
				case <-releaseSub:
				case <-time.After(historyGuardWaitLimit):
				}
				cb(SubscribeReply{Options: filteredSubscribeOptions()}, nil)
				close(subDone)
			}()
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			close(releaseSub)
			waitHistoryGuard(t, subDone, "subscribe callback")
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	subRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "g"}, &protocol.Command{Id: 1}, time.Now(), subRW.rw))

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)

	client.mu.RLock()
	chCtx := client.channels["g"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagSubscribed))
	require.True(t, channelHasFlag(chCtx.flags, flagServerTagsFilter))
}

// A filtered subscription which is removed while the application decides on
// history must not get the history served: at the callback the channel has no
// subscription, which is not what the application saw on entry.
func TestClientHistoryRefusedWhenUnsubscribedDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	releaseSub := make(chan struct{})
	subDone := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			go func() {
				select {
				case <-releaseSub:
				case <-time.After(historyGuardWaitLimit):
				}
				cb(SubscribeReply{Options: filteredSubscribeOptions()}, nil)
				close(subDone)
			}()
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			close(releaseSub)
			waitHistoryGuard(t, subDone, "subscribe callback")
			client.Unsubscribe(e.Channel)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	subRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "g"}, &protocol.Command{Id: 1}, time.Now(), subRW.rw))

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)

	client.mu.RLock()
	_, subscribed := client.channels["g"]
	client.mu.RUnlock()
	require.False(t, subscribed)
}

// A filtered subscription which comes and goes entirely inside the window, for
// a channel without any subscription on entry: nothing was there on entry, but
// the subscription the application may have relied on is gone at the callback.
func TestClientHistoryRefusedWhenFilteredSubscriptionComesAndGoesDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: filteredSubscribeOptions()}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			subRW := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: e.Channel}, &protocol.Command{Id: 3}, time.Now(), subRW.rw))
			require.Len(t, subRW.replies, 1)
			require.Nil(t, subRW.replies[0].Error)
			client.Unsubscribe(e.Channel)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

// A subscribe in flight at the callback (a reservation, without a committed
// subscription) must not get the history served. The counter row also refuses
// this case, since the subscribe takes a new generation, so the test pins the
// result and not the individual row.
func TestClientHistoryRefusedWhenReservationAtCallback(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var parkedSub SubscribeCallback
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			parkedSub = cb
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			subRW := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: e.Channel}, &protocol.Command{Id: 3}, time.Now(), subRW.rw))
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	// Deferred after the Shutdown defer, so it runs first and a failed
	// assertion does not leave the subscribe parked for the shutdown to wait on.
	defer func() {
		if parkedSub != nil {
			parkedSub(SubscribeReply{}, nil)
		}
	}()

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

// Nothing subscribed on entry or at the callback, and no subscription was
// created in between: the history is served as before, for a client which the
// application lets read history without a subscription.
func TestClientHistoryServedWhenAbsentAtBothChecks(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	require.Len(t, histRW.replies, 1)
	require.Nil(t, histRW.replies[0].Error)
	require.Len(t, histRW.replies[0].History.Publications, 2)
}

// A subscribe to another channel which commits while the application decides
// on history of a channel without subscription refuses the history: a
// subscription of this client changed during the call.
func TestClientHistoryRefusedWhenOtherSubscriptionCommitsDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			subRW := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "other"}, &protocol.Command{Id: 3}, time.Now(), subRW.rw))
			require.Len(t, subRW.replies, 1)
			require.Nil(t, subRW.replies[0].Error)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

// A subscription committed without a server tags filter is served history at the
// callback, as before the check.
func TestClientHistoryServedWhenUnfilteredSubscriptionAtCallback(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	subRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "g"}, &protocol.Command{Id: 1}, time.Now(), subRW.rw))

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	require.Len(t, histRW.replies, 1)
	require.Nil(t, histRW.replies[0].Error)
	require.Len(t, histRW.replies[0].History.Publications, 2)
}

// A committed filtered subscription is refused on entry, without calling the
// application at all.
func TestClientHistoryRefusedOnEntryWithoutCallingHandler(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var handlerCalls atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: filteredSubscribeOptions()}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			handlerCalls.Add(1)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	subRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "g"}, &protocol.Command{Id: 1}, time.Now(), subRW.rw))

	histRW := testReplyWriterWrapper()
	require.Equal(t, ErrorPermissionDenied, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	require.Empty(t, histRW.replies)
	require.Equal(t, int32(0), handlerCalls.Load())
}

// A sub refresh which installs a server tags filter while the application
// decides on history makes the callback refuse it.
func TestClientHistoryRefusedWhenRefreshAddsFilterDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{ClientSideRefresh: true, Credentials: &Credentials{UserID: "user1"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 60},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt:         time.Now().Unix() + 60,
				ServerTagsFilter: &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			refreshRW := testReplyWriterWrapper()
			require.NoError(t, client.handleSubRefresh(&protocol.SubRefreshRequest{
				Channel: e.Channel, Token: "new_token",
			}, &protocol.Command{Id: 3}, time.Now(), refreshRW.rw))
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "ch")

	subRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), subRW.rw))
	require.Len(t, subRW.replies, 1)
	require.Nil(t, subRW.replies[0].Error)

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

// A map subscription which starts loading while the application decides on
// history (an entry in mapSubscribing at the callback only) makes the callback
// refuse it. The counter row also refuses this case, since seedMapSubscribing
// takes a new generation, so the test pins the result and not the individual row.
func TestClientHistoryRefusedWhenMapSubscribeStartsDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			seedMapSubscribing(client, e.Channel)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
	removeMapSubscribing(client, "g")
}

// A map subscription loading on entry which is gone at the callback is refused:
// the application decided while it was loading.
func TestClientHistoryRefusedWhenMapSubscribeEndsDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			removeMapSubscribing(client, e.Channel)
			cb(HistoryReply{}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")
	seedMapSubscribing(client, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

// The application may set the history result itself. The refusal applies to
// that result too: the publications it carries are not handed out when a
// filtered subscription is committed during the call.
func TestClientHistoryRefusedWhenApplicationResultDuringCall(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: filteredSubscribeOptions()}, nil)
		})
		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			subRW := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: e.Channel}, &protocol.Command{Id: 3}, time.Now(), subRW.rw))
			require.Len(t, subRW.replies, 1)
			require.Nil(t, subRW.replies[0].Error)
			cb(HistoryReply{Result: &HistoryResult{
				Publications: []*Publication{{Data: []byte(`{}`)}},
			}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	publishHistoryForTest(t, node, "g")

	histRW := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: "g", Limit: 100}, &protocol.Command{Id: 2}, time.Now(), histRW.rw))
	requireHistoryRefusedReply(t, histRW)
}

func filteredSubscribeOptions() SubscribeOptions {
	return SubscribeOptions{
		ServerTagsFilter: &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
	}
}

func publishHistoryForTest(t *testing.T, node *Node, channel string) {
	t.Helper()
	for i := 0; i < 2; i++ {
		_, err := node.Publish(channel, []byte(`{}`), WithHistory(100, time.Minute), WithTags(map[string]string{"team": "sales"}))
		require.NoError(t, err)
	}
}

func requireHistoryRefusedReply(t *testing.T, rw *sliceReplyWriter) {
	t.Helper()
	require.Len(t, rw.replies, 1)
	require.NotNil(t, rw.replies[0].Error)
	require.Equal(t, ErrorPermissionDenied.Code, rw.replies[0].Error.Code)
	require.Nil(t, rw.replies[0].History)
}

// historyGuardWaitLimit bounds the waits of the history guard tests, so a test
// which misses its ordering fails instead of hanging the package.
const historyGuardWaitLimit = 10 * time.Second

func waitHistoryGuard(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(historyGuardWaitLimit):
		t.Fatalf("%s did not run within %s", what, historyGuardWaitLimit)
	}
}

func seedMapSubscribing(client *Client, channel string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.mapSubscribing == nil {
		client.mapSubscribing = make(map[string]*mapSubscribeState)
	}
	client.mapSubscribing[channel] = &mapSubscribeState{
		subscribingCh: make(chan struct{}),
		subGen:        client.subGenCounter.Add(1),
	}
}

func removeMapSubscribing(client *Client, channel string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	delete(client.mapSubscribing, channel)
}
