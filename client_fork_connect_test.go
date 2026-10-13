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

func TestErrorAppFieldsSurviveProtoAndJSON(t *testing.T) {
	t.Parallel()
	appErr := &Error{
		Code:      400,
		Message:   "flood",
		Temporary: true,
		AppCode:   420,
		AppType:   "FLOOD_WAIT_17",
		Metadata:  map[string]string{"pending_token": "abc"},
	}
	protoErr := appErr.toProto()
	require.Equal(t, int32(420), protoErr.AppCode)
	require.Equal(t, "FLOOD_WAIT_17", protoErr.AppType)
	require.Equal(t, map[string]string{"pending_token": "abc"}, protoErr.Metadata)

	for _, pt := range []protocol.Type{protocol.TypeJSON, protocol.TypeProtobuf} {
		data, err := protocol.GetReplyEncoder(pt).Encode(&protocol.Reply{Error: protoErr})
		require.NoError(t, err)
		var decoded *protocol.Reply
		if pt == protocol.TypeJSON {
			decoded = decodeReply(t, pt, data)
		} else {
			// Protobuf replies are unframed on this path; the framed decoder is for streams.
			decoded = &protocol.Reply{}
			require.NoError(t, decoded.UnmarshalCF(data))
		}
		require.NotNil(t, decoded.Error)
		require.Equal(t, uint32(400), decoded.Error.Code)
		require.Equal(t, int32(420), decoded.Error.AppCode)
		require.Equal(t, "FLOOD_WAIT_17", decoded.Error.AppType)
		require.Equal(t, map[string]string{"pending_token": "abc"}, decoded.Error.Metadata)
	}
}

func TestErrorAppFieldsReachRPCReply(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			cb(RPCReply{}, &Error{
				Code:    400,
				Message: "flood",
				AppCode: 420,
				AppType: "FLOOD_WAIT_17",
			})
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRPC(&protocol.RPCRequest{}, &protocol.Command{Id: 2}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.NotNil(t, rw.replies[0].Error)
	require.Equal(t, int32(420), rw.replies[0].Error.AppCode)
	require.Equal(t, "FLOOD_WAIT_17", rw.replies[0].Error.AppType)
}
