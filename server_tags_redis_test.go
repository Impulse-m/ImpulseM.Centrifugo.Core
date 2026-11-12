//go:build integration

package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/filter"
	"github.com/stretchr/testify/require"
)

// Two nodes share one Redis. Node B holds a subscriber with a server filter on a
// server key which node A publishes. The matching publication reaches the
// subscriber, the other one does not, and no frame carries the server key.
func TestRedisServerTagsFilterAcrossNodes(t *testing.T) {
	t.Parallel()
	redisConf := testSingleRedisConf(0)
	prefix := getUniquePrefix()
	const ch = "server_tags_two_nodes"

	node2, err := New(Config{})
	require.NoError(t, err)
	defer func() { _ = node2.Shutdown(context.Background()) }()
	s2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)
	b2, err := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	require.NoError(t, err)
	node2.SetBroker(b2)
	require.NoError(t, node2.Run())
	defer stopRedisBroker(b2)

	node2.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				ServerTagsFilter: filter.Eq("tenant", "t-17"),
			}}, nil)
		})
	})
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	client := newTestClientCustomTransport(t, context.Background(), node2, transport, "user")
	connectClientV2(t, client)
	subscribeClientV2(t, client, ch)

	node1, err := New(Config{})
	require.NoError(t, err)
	defer func() { _ = node1.Shutdown(context.Background()) }()
	s1, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	b1, err := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s1},
	})
	require.NoError(t, err)
	node1.SetBroker(b1)
	require.NoError(t, node1.Run())
	defer stopRedisBroker(b1)

	_, err = node1.Publish(ch, []byte(`{"n":1}`),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)
	_, err = node1.Publish(ch, []byte(`{"n":2}`),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-99"}),
	)
	require.NoError(t, err)

	frames := serverTagsWaitFrame(t, transport.sink, `"n":1`)
	frames = append(frames, serverTagsCollectFrames(transport.sink, 200*time.Millisecond)...)
	for _, frame := range frames {
		require.NotContains(t, frame, `"n":2`, "server filter must drop the publication of another tenant")
	}
	serverTagsRequireNoKeys(t, frames)
}
