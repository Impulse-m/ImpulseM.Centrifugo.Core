package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestReplyTTL(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		lead time.Duration
		ttl  int64
		want uint32
	}{
		{name: "no lead", lead: 0, ttl: 600, want: 600},
		{name: "lead keeps half of a short ttl", lead: 60 * time.Second, ttl: 100, want: 50},
		{name: "lead applies in full", lead: 60 * time.Second, ttl: 600, want: 540},
		{name: "lead is capped at half the ttl", lead: 60 * time.Second, ttl: 120, want: 60},
		{name: "small ttl", lead: 60 * time.Second, ttl: 3, want: 2},
		{name: "ttl of one stays one", lead: 60 * time.Second, ttl: 1, want: 1},
		{name: "ttl of zero stays zero", lead: 60 * time.Second, ttl: 0, want: 0},
		{name: "fraction is cut to whole seconds", lead: 1500 * time.Millisecond, ttl: 600, want: 599},
		{name: "negative lead is ignored", lead: -time.Second, ttl: 600, want: 600},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, replyTTL(tc.lead, tc.ttl))
		})
	}
}

func TestClientConnectTTLWithRefreshLead(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.config.ClientRefreshLead = 60 * time.Second

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	expireAt := time.Now().Unix() + 600
	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:   "42",
		ExpireAt: expireAt,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	res := extractConnectReply(rwWrapper.replies)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, res.Expires)
	require.GreaterOrEqual(t, res.Ttl, uint32(539))
	require.LessOrEqual(t, res.Ttl, uint32(540))

	client.mu.RLock()
	exp := client.exp
	client.mu.RUnlock()
	require.Equal(t, expireAt, exp)
}

func TestClientConnectTTLWithRefreshLeadServerSideRefresh(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.config.ClientRefreshLead = 60 * time.Second

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: false,
		}, nil
	})

	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 600,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	res := extractConnectReply(rwWrapper.replies)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.False(t, res.Expires)
	require.Equal(t, uint32(0), res.Ttl)
}

func TestClientRefreshTTLWithRefreshLead(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.config.ClientRefreshLead = 60 * time.Second

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 600

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			require.Equal(t, "test", e.Token)
			cb(RefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	res := rwWrapper.replies[0].Refresh
	require.GreaterOrEqual(t, res.Ttl, uint32(539))
	require.LessOrEqual(t, res.Ttl, uint32(540))

	client.mu.RLock()
	exp := client.exp
	client.mu.RUnlock()
	require.Equal(t, expireAt, exp)
}

func TestClientRefreshTTLWithoutRefreshLead(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 600

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	res := rwWrapper.replies[0].Refresh
	require.GreaterOrEqual(t, res.Ttl, uint32(599))
	require.LessOrEqual(t, res.Ttl, uint32(600))
}
