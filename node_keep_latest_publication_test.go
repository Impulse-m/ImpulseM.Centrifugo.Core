package centrifuge

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/fdelta"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newKeepLatestPublicationTestNode returns a running node that keeps the latest
// publication of each channel and allows fossil delta and client tags filters.
func newKeepLatestPublicationTestNode(t *testing.T) *Node {
	t.Helper()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
		GetChannelMediumOptions: func(channel string) ChannelMediumOptions {
			return ChannelMediumOptions{KeepLatestPublication: true}
		},
		Metrics: MetricsConfig{
			RegistererGatherer: prometheus.NewRegistry(),
		},
	})
	require.NoError(t, err)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				AllowTagsFilter:   true,
			}}, nil)
		})
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})
	return node
}

// subscribeKeepLatestPublicationClient connects a client with a transport sink
// and subscribes it with the given request. It returns the client, the sink with
// the pushes it receives, and the subscribe result.
func subscribeKeepLatestPublicationClient(
	t *testing.T,
	node *Node,
	userID string,
	req *protocol.SubscribeRequest,
) (*Client, chan []byte, *protocol.SubscribeResult) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	transport := newTestTransport(cancelFn)
	transport.sink = make(chan []byte, 100)
	transport.setProtocolType(ProtocolTypeProtobuf)
	transport.setProtocolVersion(ProtocolVersion2)
	client := newTestConnectedClientWithTransport(t, ctx, node, transport, userID)
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Len(t, rw.replies, 1)
	require.Nil(t, rw.replies[0].Error)
	return client, transport.sink, rw.replies[0].Subscribe
}

// nextKeepLatestPublicationPush returns the next publication pushed to the sink.
func nextKeepLatestPublicationPush(t *testing.T, sink chan []byte) *protocol.Publication {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			if err := reply.UnmarshalCF(data); err != nil {
				continue
			}
			if reply.Push != nil && reply.Push.Pub != nil {
				return reply.Push.Pub
			}
		case <-timeout:
			t.Fatal("timeout waiting for pushed publication")
			return nil
		}
	}
}

// TestNodeKeepLatestPublicationAtMostOnceDelta checks that a subscriber without
// positioning or recovery gets delta publications from the node-local latest
// publication: the first push after subscribe is full, the next ones are fossil
// patches against the previous publication, and a client that subscribes later
// starts with a full publication again.
func TestNodeKeepLatestPublicationAtMostOnceDelta(t *testing.T) {
	t.Parallel()
	node := newKeepLatestPublicationTestNode(t)
	const ch = "keep_latest_at_most_once_delta"
	body := strings.Repeat("shared-body-", 12)
	payloadA := []byte("AAAA-" + body)
	payloadB := []byte("BBBB-" + body)
	payloadC := []byte("CCCC-" + body)

	subReq := &protocol.SubscribeRequest{Channel: ch, Delta: string(DeltaTypeFossil)}
	_, sink1, res1 := subscribeKeepLatestPublicationClient(t, node, "1", subReq)
	require.True(t, res1.Delta, "delta must be negotiated without a tags filter")

	_, err := node.Publish(ch, payloadA, WithDelta(true))
	require.NoError(t, err)
	pubA := nextKeepLatestPublicationPush(t, sink1)
	require.False(t, pubA.Delta, "first publication after subscribe must be full")
	require.Equal(t, payloadA, []byte(pubA.Data))

	_, err = node.Publish(ch, payloadB, WithDelta(true))
	require.NoError(t, err)
	pubB := nextKeepLatestPublicationPush(t, sink1)
	require.True(t, pubB.Delta, "second publication must be a delta against the first")
	appliedB, err := fdelta.Apply(pubA.Data, pubB.Data)
	require.NoError(t, err)
	require.Equal(t, payloadB, appliedB)

	// A client subscribing now has no base: its first push is full.
	_, sink2, res2 := subscribeKeepLatestPublicationClient(t, node, "2", subReq)
	require.True(t, res2.Delta)

	_, err = node.Publish(ch, payloadC, WithDelta(true))
	require.NoError(t, err)
	pubC2 := nextKeepLatestPublicationPush(t, sink2)
	require.False(t, pubC2.Delta, "first push of a late subscriber must be full")
	require.Equal(t, payloadC, []byte(pubC2.Data))

	pubC1 := nextKeepLatestPublicationPush(t, sink1)
	require.True(t, pubC1.Delta)
	appliedC, err := fdelta.Apply(payloadB, pubC1.Data)
	require.NoError(t, err)
	require.Equal(t, payloadC, appliedC)
}

// TestNodeKeepLatestPublicationTagsFilterSubscriberGetsFull checks that a
// subscriber with a client tags filter never negotiates delta, so it gets full
// publications, while a subscriber without a filter on the same channel gets
// deltas from the same node-local latest publication.
func TestNodeKeepLatestPublicationTagsFilterSubscriberGetsFull(t *testing.T) {
	t.Parallel()
	node := newKeepLatestPublicationTestNode(t)
	const ch = "keep_latest_tags_filter"
	body := strings.Repeat("shared-body-", 12)
	payloadA := []byte("AAAA-" + body)
	payloadB := []byte("BBBB-" + body)
	tags := map[string]string{"team": "eng"}

	_, deltaSink, deltaRes := subscribeKeepLatestPublicationClient(t, node, "delta",
		&protocol.SubscribeRequest{Channel: ch, Delta: string(DeltaTypeFossil)})
	require.True(t, deltaRes.Delta)

	_, filteredSink, filteredRes := subscribeKeepLatestPublicationClient(t, node, "filtered",
		&protocol.SubscribeRequest{
			Channel: ch,
			Delta:   string(DeltaTypeFossil),
			Tf:      &protocol.FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
		})
	require.False(t, filteredRes.Delta, "delta must not be negotiated with a tags filter")

	_, err := node.Publish(ch, payloadA, WithDelta(true), WithTags(tags))
	require.NoError(t, err)
	_, err = node.Publish(ch, payloadB, WithDelta(true), WithTags(tags))
	require.NoError(t, err)

	deltaFirst := nextKeepLatestPublicationPush(t, deltaSink)
	deltaSecond := nextKeepLatestPublicationPush(t, deltaSink)
	require.False(t, deltaFirst.Delta)
	require.True(t, deltaSecond.Delta, "delta subscriber must get a delta for the second publication")
	applied, err := fdelta.Apply(deltaFirst.Data, deltaSecond.Data)
	require.NoError(t, err)
	require.Equal(t, payloadB, applied)

	filteredFirst := nextKeepLatestPublicationPush(t, filteredSink)
	filteredSecond := nextKeepLatestPublicationPush(t, filteredSink)
	require.False(t, filteredFirst.Delta, "filtered subscriber must get full data")
	require.False(t, filteredSecond.Delta, "filtered subscriber must never get delta data")
	require.Equal(t, payloadB, []byte(filteredSecond.Data))
}

// TestNodeKeepLatestPublicationMediumReleased checks that the channel medium that
// holds the latest publication is released with the last subscriber.
func TestNodeKeepLatestPublicationMediumReleased(t *testing.T) {
	t.Parallel()
	node := newKeepLatestPublicationTestNode(t)
	const ch = "keep_latest_medium_released"
	startValue := metricValue(t, node.metrics.channelMediumsGauge)

	client, _, _ := subscribeKeepLatestPublicationClient(t, node, "1",
		&protocol.SubscribeRequest{Channel: ch, Delta: string(DeltaTypeFossil)})
	require.Equal(t, startValue+1, metricValue(t, node.metrics.channelMediumsGauge))

	_, err := node.Publish(ch, []byte(`{"x":1}`), WithDelta(true))
	require.NoError(t, err)

	client.Unsubscribe(ch)
	require.Eventually(t, func() bool {
		return metricValue(t, node.metrics.channelMediumsGauge) == startValue
	}, 15*time.Second, 50*time.Millisecond)
}
