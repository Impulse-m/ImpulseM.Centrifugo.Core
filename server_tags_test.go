package centrifuge

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifuge/internal/filter"
)

// serverTagsWaitFrame collects frames from sink until one contains the given text
// and returns all frames collected so far.
func serverTagsWaitFrame(t *testing.T, sink chan []byte, contains string) []string {
	t.Helper()
	var frames []string
	timeout := time.After(5 * time.Second)
	for {
		select {
		case data := <-sink:
			frames = append(frames, string(data))
			if strings.Contains(string(data), contains) {
				return frames
			}
		case <-timeout:
			require.FailNow(t, "expected frame was not delivered", contains)
			return frames
		}
	}
}

// serverTagsCollectFrames returns the frames which reach sink within window.
func serverTagsCollectFrames(sink chan []byte, window time.Duration) []string {
	var frames []string
	timeout := time.After(window)
	for {
		select {
		case data := <-sink:
			frames = append(frames, string(data))
		case <-timeout:
			return frames
		}
	}
}

// serverTagsRequireNoKeys asserts that no frame carries a server key or its value.
func serverTagsRequireNoKeys(t *testing.T, frames []string) {
	t.Helper()
	for _, frame := range frames {
		require.NotContains(t, frame, "tenant", "server key must not reach clients")
		require.NotContains(t, frame, "t-17", "server value must not reach clients")
	}
}

func TestServerTags_FilterTagsReturnsTagsWithoutCopy(t *testing.T) {
	t.Parallel()
	tags := map[string]string{"team": "eng"}
	pub := &Publication{Tags: tags}
	require.Equal(t, reflect.ValueOf(tags).Pointer(), reflect.ValueOf(serverFilterTags(pub)).Pointer())
}

func TestServerTags_FilterTagsUnionDoesNotMutateInputs(t *testing.T) {
	t.Parallel()
	tags := map[string]string{"team": "eng"}
	serverTags := map[string]string{"tenant": "t-17"}
	pub := &Publication{Tags: tags, ServerTags: serverTags}

	union := serverFilterTags(pub)
	require.Equal(t, map[string]string{"team": "eng", "tenant": "t-17"}, union)
	require.Equal(t, map[string]string{"team": "eng"}, tags)
	require.Equal(t, map[string]string{"tenant": "t-17"}, serverTags)
}

func TestServerTags_FilterErrorsDropPublication(t *testing.T) {
	t.Parallel()
	tags := map[string]string{"tenant": "t-17"}
	bad := &protocol.FilterNode{Op: "", Key: "tenant", Cmp: "bogus", Val: "x"}
	tests := []struct {
		name   string
		filter *protocol.FilterNode
	}{
		{name: "nil node", filter: nil},
		{name: "unknown comparison", filter: bad},
		{name: "and with failing child", filter: filter.And(filter.Eq("tenant", "t-17"), bad)},
		{name: "not with two children", filter: &protocol.FilterNode{Op: "not", Nodes: []*protocol.FilterNode{filter.Eq("tenant", "t-17"), filter.Eq("tenant", "t-17")}}},
		{name: "unknown op", filter: &protocol.FilterNode{Op: "xor"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			match, err := filter.Match(tt.filter, tags)
			require.Error(t, err)
			require.False(t, match, "a filter error must not match, so the subscriber drops the publication")
		})
	}
}

func TestServerTags_ProtocolFieldRoundTrip(t *testing.T) {
	t.Parallel()
	src := &protocol.Publication{
		Data:       []byte(`{}`),
		Tags:       map[string]string{"team": "eng"},
		ServerTags: map[string]string{"tenant": "t-17"},
	}
	data, err := src.MarshalCF()
	require.NoError(t, err)

	var dst protocol.Publication
	require.NoError(t, dst.UnmarshalCF(data))
	require.Equal(t, map[string]string{"team": "eng"}, dst.GetTags())
	require.Equal(t, map[string]string{"tenant": "t-17"}, dst.GetServerTags())
}

// A protocol message without field 1000 (for example from a node which predates
// it) decodes to nil ServerTags. Such a node cannot apply server filters on
// server keys: see the rolling-upgrade rule in the design.
func TestServerTags_ProtocolWithoutFieldDecodesNil(t *testing.T) {
	t.Parallel()
	src := &protocol.Publication{
		Data: []byte(`{}`),
		Tags: map[string]string{"team": "eng"},
	}
	data, err := src.MarshalCF()
	require.NoError(t, err)

	var dst protocol.Publication
	require.NoError(t, dst.UnmarshalCF(data))
	require.Nil(t, dst.ServerTags)
	require.Nil(t, dst.GetServerTags())
	require.Equal(t, map[string]string{"team": "eng"}, dst.GetTags())
	require.Nil(t, pubFromProto(&dst).ServerTags)
}

// Frames for a publication with server tags are byte-identical to frames for the
// same publication without them, in JSON and protobuf.
func TestServerTags_FrameBytesUnchangedWithoutServerTags(t *testing.T) {
	t.Parallel()
	data := []byte(`{"text":"hi"}`)
	plain := &Publication{
		Offset: 7,
		Data:   data,
		Tags:   map[string]string{"kind": "msg"},
	}
	withServer := &Publication{
		Offset:     7,
		Data:       data,
		Tags:       map[string]string{"kind": "msg"},
		ServerTags: map[string]string{"tenant": "t-17", "member": "u-42"},
	}
	// The frame built by hand from public fields only, as it was before server tags.
	expected := &protocol.Publication{
		Offset: 7,
		Data:   data,
		Tags:   map[string]string{"kind": "msg"},
	}

	push := func(p *protocol.Publication) *protocol.Push {
		return &protocol.Push{Channel: "ch", Pub: p}
	}

	expectedJSON, err := protocol.DefaultJsonPushEncoder.Encode(push(expected))
	require.NoError(t, err)
	plainJSON, err := protocol.DefaultJsonPushEncoder.Encode(push(pubToProto(plain)))
	require.NoError(t, err)
	serverJSON, err := protocol.DefaultJsonPushEncoder.Encode(push(pubToProto(withServer)))
	require.NoError(t, err)
	require.Equal(t, expectedJSON, plainJSON)
	require.Equal(t, plainJSON, serverJSON)

	expectedPB, err := protocol.DefaultProtobufPushEncoder.Encode(push(expected))
	require.NoError(t, err)
	plainPB, err := protocol.DefaultProtobufPushEncoder.Encode(push(pubToProto(plain)))
	require.NoError(t, err)
	serverPB, err := protocol.DefaultProtobufPushEncoder.Encode(push(pubToProto(withServer)))
	require.NoError(t, err)
	require.Equal(t, expectedPB, plainPB)
	require.Equal(t, plainPB, serverPB)
}

func TestServerTags_LiveServerFilterEqAndNex(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		var options SubscribeOptions
		switch client.UserID() {
		case "eq":
			options.ServerTagsFilter = filter.Eq("tenant", "t-17")
		case "nex":
			options.ServerTagsFilter = filter.NotExists("tenant")
		}
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: options}, nil)
		})
	})

	eqTransport := newTestTransport(func() {})
	eqTransport.sink = make(chan []byte, 100)
	eqClient := newTestClientCustomTransport(t, context.Background(), node, eqTransport, "eq")
	connectClientV2(t, eqClient)
	subscribeClientV2(t, eqClient, "test")

	nexTransport := newTestTransport(func() {})
	nexTransport.sink = make(chan []byte, 100)
	nexClient := newTestClientCustomTransport(t, context.Background(), node, nexTransport, "nex")
	connectClientV2(t, nexClient)
	subscribeClientV2(t, nexClient, "test")

	_, err := node.Publish("test", []byte(`{"text":"first"}`),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text":"second"}`))
	require.NoError(t, err)

	// Publications arrive in order per subscriber. After the expected frame, a
	// window collects anything that would follow it.
	eqFrames := serverTagsWaitFrame(t, eqTransport.sink, "first")
	eqFrames = append(eqFrames, serverTagsCollectFrames(eqTransport.sink, 200*time.Millisecond)...)
	nexFrames := serverTagsWaitFrame(t, nexTransport.sink, "second")
	nexFrames = append(nexFrames, serverTagsCollectFrames(nexTransport.sink, 200*time.Millisecond)...)

	for _, frame := range eqFrames {
		require.NotContains(t, frame, "second", "eq filter must drop the publication without the server key")
	}
	for _, frame := range nexFrames {
		require.NotContains(t, frame, "first", "nex filter must drop the publication carrying the server key")
	}
	serverTagsRequireNoKeys(t, eqFrames)
	serverTagsRequireNoKeys(t, nexFrames)

	// Memory broker keeps the server tags after delivery and stripping.
	_, err = node.Publish("test_history", []byte(`{"text":"stored"}`),
		WithHistory(10, time.Minute),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)
	stored, err := node.History("test_history", WithLimit(NoLimit))
	require.NoError(t, err)
	require.Len(t, stored.Publications, 1)
	require.Equal(t, map[string]string{"tenant": "t-17"}, stored.Publications[0].ServerTags)
	require.Equal(t, map[string]string{"team": "eng"}, stored.Publications[0].Tags)
}

func TestServerTags_ClientTagsFilterNeverMatchesServerKeys(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{AllowTagsFilter: true}}, nil)
		})
	})

	publicTransport := newTestTransport(func() {})
	publicTransport.sink = make(chan []byte, 100)
	publicClient := newTestClientCustomTransport(t, context.Background(), node, publicTransport, "public")
	connectClientV2(t, publicClient)
	subscribeClientWithTagsFilter(t, publicClient, "test", filter.Eq("team", "eng"))

	serverOnlyTransport := newTestTransport(func() {})
	serverOnlyTransport.sink = make(chan []byte, 100)
	serverOnlyClient := newTestClientCustomTransport(t, context.Background(), node, serverOnlyTransport, "server_only")
	connectClientV2(t, serverOnlyClient)
	subscribeClientWithTagsFilter(t, serverOnlyClient, "test", filter.Eq("tenant", "t-17"))

	_, err := node.Publish("test", []byte(`{"text":"first"}`),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text":"second"}`), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)

	// The public filter on a public key still delivers.
	publicFrames := serverTagsWaitFrame(t, publicTransport.sink, "second")
	require.Contains(t, strings.Join(publicFrames, ""), "first")

	// The client filter on a server-only key matches nothing: the client filter
	// sees public tags only. The sink also holds the connect and subscribe
	// replies, so only the publications are checked.
	serverOnlyFrames := serverTagsCollectFrames(serverOnlyTransport.sink, 200*time.Millisecond)
	for _, frame := range serverOnlyFrames {
		require.NotContains(t, frame, "first")
		require.NotContains(t, frame, "second")
	}
}

// subscribeClientWithTagsFilter subscribes the client with a client tags filter.
func subscribeClientWithTagsFilter(t *testing.T, client *Client, ch string, tf *protocol.FilterNode) {
	t.Helper()
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
		Tf:      tf,
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Nil(t, rw.replies[0].Error)
}

// Stream recovery applies the server filter on server keys. Filtered entries are
// offset-only markers and passing entries carry no server keys.
func TestServerTags_StreamRecoveryAppliesServerFilter(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:   true,
				RecoveryMode:     RecoveryModeStream,
				ServerTagsFilter: filter.Eq("tenant", "t-17"),
			}}, nil)
		})
	})

	const ch = "stream_recovery_server_tags"
	publish := func(data string, serverTags map[string]string) {
		_, err := node.Publish(ch, []byte(data),
			WithHistory(10, time.Minute),
			WithTags(map[string]string{"team": "eng"}),
			WithServerTags(serverTags),
		)
		require.NoError(t, err)
	}
	publish(`{"n":1}`, map[string]string{"tenant": "t-17"})
	publish(`{"n":2}`, map[string]string{"tenant": "t-99"})
	publish(`{"n":3}`, map[string]string{"member": "u-42"})

	client := newTestClientV2(t, node, "user")
	connectClientV2(t, client)
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
		Recover: true,
		Offset:  0,
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Nil(t, rw.replies[0].Error)
	res := extractSubscribeResult(rw.replies)
	require.True(t, res.Recovered)
	// The publications the server filter excludes (offsets 2 and 3) become
	// offset-only markers in isStreamRecovered, which MergePublications strips,
	// so only the matching one reaches the client.
	require.Len(t, res.Publications, 1)
	require.Equal(t, uint64(1), res.Publications[0].Offset)
	require.Equal(t, protocol.Raw(`{"n":1}`), res.Publications[0].Data)
	require.Equal(t, "eng", res.Publications[0].Tags["team"])
	require.Nil(t, res.Publications[0].GetServerTags())
}

// The subscribe reply of a subscriber with a server filter carries no server keys
// in any field, neither in the publications nor in its encoded JSON.
func TestServerTags_SubscribeReplyHasNoServerKeys(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:   true,
				RecoveryMode:     RecoveryModeStream,
				ServerTagsFilter: filter.Eq("tenant", "t-17"),
			}}, nil)
		})
	})

	const ch = "subscribe_reply_server_tags"
	_, err := node.Publish(ch, []byte(`{"n":1}`),
		WithHistory(10, time.Minute),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)

	client := newTestClientV2(t, node, "user")
	connectClientV2(t, client)
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
		Recover: true,
		Offset:  0,
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Nil(t, rw.replies[0].Error)

	res := extractSubscribeResult(rw.replies)
	require.Len(t, res.Publications, 1)
	require.Equal(t, protocol.Raw(`{"n":1}`), res.Publications[0].Data)
	require.Nil(t, res.Publications[0].GetServerTags())

	encoded, err := protocol.DefaultJsonReplyEncoder.Encode(rw.replies[0])
	require.NoError(t, err)
	serverTagsRequireNoKeys(t, []string{string(encoded)})
	require.Contains(t, string(encoded), "team")
}

// Cache recovery applies the server filter on server keys and returns the newest
// publication which passes it, without server keys.
func TestServerTags_CacheRecoveryAppliesServerFilter(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:   true,
				RecoveryMode:     RecoveryModeCache,
				ServerTagsFilter: filter.Eq("tenant", "t-17"),
			}}, nil)
		})
	})

	const ch = "cache_recovery_server_tags"
	publish := func(data string, serverTags map[string]string) {
		_, err := node.Publish(ch, []byte(data),
			WithHistory(10, time.Minute),
			WithTags(map[string]string{"team": "eng"}),
			WithServerTags(serverTags),
		)
		require.NoError(t, err)
	}
	publish(`{"n":1}`, map[string]string{"tenant": "t-17"})
	publish(`{"n":2}`, map[string]string{"tenant": "t-99"})
	publish(`{"n":3}`, map[string]string{"member": "u-42"})

	client := newTestClientV2(t, node, "user")
	connectClientV2(t, client)
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Nil(t, rw.replies[0].Error)
	res := extractSubscribeResult(rw.replies)
	require.True(t, res.Recovered)
	require.Len(t, res.Publications, 1)
	require.Equal(t, protocol.Raw(`{"n":1}`), res.Publications[0].Data)
	require.Nil(t, res.Publications[0].GetServerTags())
}

// History of an unsubscribed client is served with public tags only, while the
// stored publication keeps its server tags.
func TestServerTags_HistoryServesNoServerKeys(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnHistory(func(_ HistoryEvent, cb HistoryCallback) {
			cb(HistoryReply{}, nil)
		})
	})

	const ch = "history_server_tags"
	_, err := node.Publish(ch, []byte(`{"n":1}`),
		WithHistory(10, time.Minute),
		WithTags(map[string]string{"team": "eng"}),
		WithServerTags(map[string]string{"tenant": "t-17"}),
	)
	require.NoError(t, err)

	client := newTestConnectedClientV2(t, node, "user")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleHistory(&protocol.HistoryRequest{Channel: ch, Limit: 10}, &protocol.Command{}, time.Now(), rw.rw))
	require.Nil(t, rw.replies[0].Error)
	require.Len(t, rw.replies[0].History.Publications, 1)
	pub := rw.replies[0].History.Publications[0]
	require.Equal(t, map[string]string{"team": "eng"}, pub.Tags)
	require.Nil(t, pub.GetServerTags())

	stored, err := node.History(ch, WithLimit(NoLimit))
	require.NoError(t, err)
	require.Len(t, stored.Publications, 1)
	require.Equal(t, map[string]string{"tenant": "t-17"}, stored.Publications[0].ServerTags)
}

// Broadcasting one request map to several channels from concurrent goroutines
// must not write to it. Run under -race.
func TestServerTags_BroadcastSharedMapNotMutated(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				ServerTagsFilter: filter.Eq("tenant", "t-17"),
			}}, nil)
		})
	})

	channels := []string{"shared_a", "shared_b"}
	for _, ch := range channels {
		transport := newTestTransport(func() {})
		transport.sink = make(chan []byte, 1000)
		newTestSubscribedClientWithTransport(t, context.Background(), node, transport, "user_"+ch, ch)
	}

	shared := map[string]string{"tenant": "t-17", "member": "u-42"}
	want := maps.Clone(shared)
	pub := &Publication{
		Data:       []byte(`{"x":1}`),
		Tags:       map[string]string{"team": "eng"},
		ServerTags: shared,
	}

	for i := 0; i < 20; i++ {
		var wg sync.WaitGroup
		for _, ch := range channels {
			wg.Add(1)
			go func(channel string) {
				defer wg.Done()
				assert.NoError(t, node.hub.BroadcastPublication(channel, pub, StreamPosition{}))
			}(ch)
		}
		wg.Wait()
	}
	require.Equal(t, want, shared)
}

// Benchmark: 200 server keys across 1000 subscribers, against the same keys in
// public tags. Reports the bytes of one frame per subscriber.
func BenchmarkServerTagsFanout(b *testing.B) {
	const numKeys = 200
	const numSubs = 1000
	const ch = "bench_server_tags"

	keys := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[fmt.Sprintf("key_%03d", i)] = fmt.Sprintf("value_%03d_padding", i)
	}

	for _, mode := range []string{"ServerTags", "Tags"} {
		b.Run(mode, func(b *testing.B) {
			node := defaultNodeNoHandlers()
			defer func() { _ = node.Shutdown(context.Background()) }()
			node.OnConnect(func(client *Client) {
				client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
					var options SubscribeOptions
					if mode == "ServerTags" {
						options.ServerTagsFilter = filter.Eq("key_000", "value_000_padding")
					}
					cb(SubscribeReply{Options: options}, nil)
				})
			})

			b.StopTimer()
			for i := 0; i < numSubs; i++ {
				transport := newTestTransport(func() {})
				client := newTestClientCustomTransport(b, context.Background(), node, transport, fmt.Sprintf("user_%d", i))
				connectClientV2(b, client)
				subscribeClientV2(b, client, ch)
			}

			pub := &Publication{Data: []byte(`{"text":"hello"}`)}
			if mode == "ServerTags" {
				pub.Tags = map[string]string{"kind": "msg"}
				pub.ServerTags = keys
			} else {
				pub.Tags = keys
			}
			frame, err := protocol.DefaultJsonPushEncoder.Encode(&protocol.Push{Channel: ch, Pub: pubToProto(pub)})
			require.NoError(b, err)
			b.ReportMetric(float64(len(frame)), "frame-bytes/sub")
			b.ReportMetric(float64(len(frame)*numSubs), "frame-bytes/pub")

			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := node.hub.BroadcastPublication(ch, pub, StreamPosition{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
