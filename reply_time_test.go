package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

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
