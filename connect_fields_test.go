package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestConnectInstanceReachesConnectEvent(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var got ConnectEvent
	node.OnConnecting(func(_ context.Context, e ConnectEvent) (ConnectReply, error) {
		got = e
		return ConnectReply{Credentials: &Credentials{UserID: "42"}}, nil
	})
	client := newTestClient(t, node, "42")

	rw := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{Instance: "install-1"}, &protocol.Command{}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Equal(t, "install-1", got.Instance)
}

func TestConnectReplyFlagsMaskedByAdvertised(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		clientFlag  int64
		replyFlags  int64
		expectedFlg int64
	}{
		{
			name:        "advertised and enabled",
			clientFlag:  ConnectionFlagRPCOptions,
			replyFlags:  ConnectionFlagRPCOptions,
			expectedFlg: ConnectionFlagRPCOptions,
		},
		{
			name:        "enabled but not advertised",
			clientFlag:  0,
			replyFlags:  ConnectionFlagRPCOptions,
			expectedFlg: 0,
		},
		{
			name:        "advertised but not enabled",
			clientFlag:  ConnectionFlagRPCOptions,
			replyFlags:  0,
			expectedFlg: 0,
		},
		{
			name:        "unadvertised bit dropped",
			clientFlag:  ConnectionFlagRPCOptions,
			replyFlags:  ConnectionFlagRPCOptions | (1 << 40),
			expectedFlg: ConnectionFlagRPCOptions,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := defaultNodeNoHandlers()
			defer func() { _ = node.Shutdown(context.Background()) }()
			node.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
				return ConnectReply{Credentials: &Credentials{UserID: "42"}, Flags: tt.replyFlags}, nil
			})
			client := newTestClient(t, node, "42")

			rw := testReplyWriterWrapper()
			err := client.connectCmd(&protocol.ConnectRequest{Flag: tt.clientFlag}, &protocol.Command{}, time.Now(), rw.rw)
			require.NoError(t, err)
			require.Nil(t, rw.replies[0].Error)
			require.Equal(t, tt.expectedFlg, extractConnectReply(rw.replies).Flag)
		})
	}
}

func TestConnectReplyUserAndAuthClassInResult(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{UserID: "42"},
			User:        "user-1",
			AuthClass:   "class-a",
		}, nil
	})
	client := newTestClient(t, node, "42")

	rw := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Nil(t, rw.replies[0].Error)
	res := extractConnectReply(rw.replies)
	require.Equal(t, "user-1", res.User)
	require.Equal(t, "class-a", res.AuthClass)
}

func TestRefreshReplyUserAndAuthClassInResult(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
			Credentials:       &Credentials{UserID: "42"},
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{User: "user-2", AuthClass: "class-b"}, nil)
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{Token: "token"}, &protocol.Command{}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Nil(t, rw.replies[0].Error)
	require.NotNil(t, rw.replies[0].Refresh)
	require.Equal(t, "user-2", rw.replies[0].Refresh.User)
	require.Equal(t, "class-b", rw.replies[0].Refresh.AuthClass)
}
