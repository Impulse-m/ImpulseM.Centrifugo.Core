package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRPCCallOptionsFromProtoCarriesEveryField(t *testing.T) {
	t.Parallel()
	req := &protocol.RPCRequest{
		Method:         "method",
		Data:           []byte("{}"),
		Headers:        map[string]string{"X-Trace": "abc"},
		MsgId:          (1 << 40) + 3,
		Ack:            []uint64{1, 2},
		Retransmit:     true,
		After:          []uint64{5, 6},
		WithoutUpdates: true,
		Cancel:         9,
	}
	expected := RPCCallOptions{
		Headers:        map[string]string{"X-Trace": "abc"},
		MsgID:          (1 << 40) + 3,
		Ack:            []uint64{1, 2},
		Retransmit:     true,
		After:          []uint64{5, 6},
		WithoutUpdates: true,
		Cancel:         9,
	}
	require.Equal(t, expected, rpcCallOptionsFromProto(req))
}

func TestClientHandleRPCCallOptionsReachHandler(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var got RPCEvent
	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			got = event
			cb(RPCReply{}, nil)
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRPC(&protocol.RPCRequest{
		Method:         "m",
		Data:           []byte("{}"),
		Headers:        map[string]string{"X-Trace": "abc"},
		MsgId:          7,
		Ack:            []uint64{3},
		Retransmit:     true,
		After:          []uint64{4},
		WithoutUpdates: true,
	}, &protocol.Command{}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Equal(t, RPCCallOptions{
		Headers:        map[string]string{"X-Trace": "abc"},
		MsgID:          7,
		Ack:            []uint64{3},
		Retransmit:     true,
		After:          []uint64{4},
		WithoutUpdates: true,
	}, got.Call)
}

func TestClientHandleRPCCancelReachesHandler(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var called bool
	var got RPCEvent
	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			called = true
			got = event
			cb(RPCReply{}, nil)
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRPC(&protocol.RPCRequest{Cancel: 7}, &protocol.Command{}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.True(t, called)
	require.Empty(t, got.Method)
	require.Empty(t, got.Data)
	require.Equal(t, uint64(7), got.Call.Cancel)
	require.Nil(t, rw.replies[0].Error)
}

func TestClientHandleRPCCachedReachesResult(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			cb(RPCReply{Data: []byte("{}"), Cached: true}, nil)
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRPC(&protocol.RPCRequest{MsgId: 1}, &protocol.Command{Id: 5}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Nil(t, rw.replies[0].Error)
	require.NotNil(t, rw.replies[0].Rpc)
	require.True(t, rw.replies[0].Rpc.Cached)
}

func TestClientCommandReplyTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := defaultTestNode()
			defer func() { _ = node.Shutdown(context.Background()) }()
			node.config.ReplyIncludeServerTime = tt.enabled
			node.OnConnect(func(client *Client) {
				client.OnRPC(func(event RPCEvent, cb RPCCallback) {
					cb(RPCReply{}, nil)
				})
			})
			client := newTestClient(t, node, "42")
			connectClientV2(t, client)

			rw := testReplyWriterWrapper()
			before := time.Now().UnixMilli()
			err := client.handleRPC(&protocol.RPCRequest{}, &protocol.Command{Id: 3}, time.Now(), rw.rw)
			after := time.Now().UnixMilli()
			require.NoError(t, err)
			require.Equal(t, uint32(3), rw.replies[0].Id)
			if tt.enabled {
				require.GreaterOrEqual(t, rw.replies[0].Time, before)
				require.LessOrEqual(t, rw.replies[0].Time, after)
			} else {
				require.Zero(t, rw.replies[0].Time)
			}
		})
	}
}

func TestPublicationPushHasNoReplyTime(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.config.ReplyIncludeServerTime = true

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.sink = make(chan []byte, 100)
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	client := newTestConnectedClientWithTransport(t, ctx, node, transport, "42")

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test_channel",
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Nil(t, rw.replies[0].Error)

	_, err = node.Publish("test_channel", []byte(`{"msg":1}`))
	require.NoError(t, err)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case data := <-transport.sink:
			reply := decodeReply(t, protocol.TypeJSON, data)
			if reply.Push == nil || reply.Push.Pub == nil {
				continue
			}
			require.Zero(t, reply.Time, "push must not carry a reply time")
			return
		case <-timeout:
			require.Fail(t, "timeout waiting for the publication push")
		}
	}
}

func TestRedactCommandRedactsRPCHeaders(t *testing.T) {
	t.Parallel()
	headers := map[string]string{"Authorization": "Bearer XXX"}
	cmd := &protocol.Command{Rpc: &protocol.RPCRequest{Method: "m", Headers: headers}}

	result := redactCommand(cmd)
	require.Equal(t, map[string]string{"Authorization": redacted}, result.Rpc.Headers)
	// The handler may still read the original map through RPCEvent.Call.
	require.Equal(t, "Bearer XXX", headers["Authorization"])
}
