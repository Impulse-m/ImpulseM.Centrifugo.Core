package centrifuge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestClientConnectTokenIdentity(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	identity := TokenIdentity{
		Session:    [16]byte{1, 2, 3},
		Generation: 7,
		IssuedAt:   time.Now().Unix(),
	}
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:        "42",
		TokenIdentity: identity,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, identity, client.TokenIdentity())
}

func newTokenIdentityServerRefreshTestClient(t *testing.T, node *Node, identity TokenIdentity, refreshHandler RefreshHandler) *Client {
	t.Helper()
	node.OnConnect(func(client *Client) {
		client.OnRefresh(refreshHandler)
	})

	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:        "42",
		ExpireAt:      time.Now().Unix() + 60,
		TokenIdentity: identity,
	})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	return client
}

func TestClientRefreshTokenIdentityReplaced(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	newIdentity := TokenIdentity{Generation: 2, IssuedAt: 200}
	client := newTokenIdentityServerRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
		cb(RefreshReply{
			ExpireAt:      time.Now().Unix() + 60,
			TokenIdentity: &newIdentity,
		}, nil)
	})

	client.expire()
	require.False(t, client.status == statusClosed)
	require.Equal(t, newIdentity, client.TokenIdentity())
}

func TestClientRefreshTokenIdentityNilKeepsPrevious(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	client := newTokenIdentityServerRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
		cb(RefreshReply{
			ExpireAt: time.Now().Unix() + 60,
		}, nil)
	})

	client.expire()
	require.False(t, client.status == statusClosed)
	require.Equal(t, startIdentity, client.TokenIdentity())
}

func TestClientRefreshTokenIdentityNotChangedOnFailure(t *testing.T) {
	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	newIdentity := TokenIdentity{Generation: 2, IssuedAt: 200}

	t.Run("expired", func(t *testing.T) {
		node := defaultTestNode()
		defer func() { _ = node.Shutdown(context.Background()) }()

		client := newTokenIdentityServerRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				Expired:       true,
				TokenIdentity: &newIdentity,
			}, nil)
		})

		client.expire()
		require.True(t, client.status == statusClosed)
		require.Equal(t, startIdentity, client.TokenIdentity())
	})

	t.Run("expire at in the past", func(t *testing.T) {
		node := defaultTestNode()
		defer func() { _ = node.Shutdown(context.Background()) }()

		client := newTokenIdentityServerRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				ExpireAt:      time.Now().Unix() - 1,
				TokenIdentity: &newIdentity,
			}, nil)
		})

		client.expire()
		require.True(t, client.status == statusClosed)
		require.Equal(t, startIdentity, client.TokenIdentity())
	})

	t.Run("error", func(t *testing.T) {
		node := defaultTestNode()
		defer func() { _ = node.Shutdown(context.Background()) }()

		client := newTokenIdentityServerRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				TokenIdentity: &newIdentity,
			}, errors.New("refresh failed"))
		})

		client.expire()
		require.Equal(t, startIdentity, client.TokenIdentity())
	})
}

func newTokenIdentityClientSideRefreshTestClient(t *testing.T, node *Node, identity TokenIdentity, refreshHandler RefreshHandler) *Client {
	t.Helper()
	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnRefresh(refreshHandler)
	})

	transport := newTestTransport(func() {})
	newCtx := SetCredentials(context.Background(), &Credentials{
		UserID:        "42",
		ExpireAt:      time.Now().Unix() + 60,
		TokenIdentity: identity,
	})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	return client
}

func TestClientSideRefreshTokenIdentityReplaced(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	newIdentity := TokenIdentity{Generation: 2, IssuedAt: 200}
	client := newTokenIdentityClientSideRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
		cb(RefreshReply{
			ExpireAt:      time.Now().Unix() + 60,
			TokenIdentity: &newIdentity,
		}, nil)
	})

	rwWrapper := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.Equal(t, newIdentity, client.TokenIdentity())
}

func TestClientSideRefreshTokenIdentityWithoutExpiration(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	startIdentity := TokenIdentity{Session: [16]byte{1}, Generation: 1, IssuedAt: 100}
	newIdentity := TokenIdentity{Session: [16]byte{2}, Generation: 2, IssuedAt: 200}
	client := newTokenIdentityClientSideRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
		cb(RefreshReply{
			TokenIdentity: &newIdentity,
		}, nil)
	})

	rwWrapper := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.Equal(t, newIdentity, client.TokenIdentity())
}

func TestClientSideRefreshTokenIdentityNilKeepsPrevious(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	client := newTokenIdentityClientSideRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
		cb(RefreshReply{
			ExpireAt: time.Now().Unix() + 60,
		}, nil)
	})

	rwWrapper := testReplyWriterWrapper()
	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.Equal(t, startIdentity, client.TokenIdentity())
}

func TestClientSideRefreshTokenIdentityNotChangedOnFailure(t *testing.T) {
	startIdentity := TokenIdentity{Generation: 1, IssuedAt: 100}
	newIdentity := TokenIdentity{Generation: 2, IssuedAt: 200}

	t.Run("expired", func(t *testing.T) {
		node := defaultNodeNoHandlers()
		defer func() { _ = node.Shutdown(context.Background()) }()

		client := newTokenIdentityClientSideRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				ExpireAt:      time.Now().Unix() - 1,
				TokenIdentity: &newIdentity,
			}, nil)
		})

		rwWrapper := testReplyWriterWrapper()
		err := client.handleRefresh(&protocol.RefreshRequest{
			Token: "test",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.NotNil(t, rwWrapper.replies[0].Error)
		require.Equal(t, startIdentity, client.TokenIdentity())
	})

	t.Run("error", func(t *testing.T) {
		node := defaultNodeNoHandlers()
		defer func() { _ = node.Shutdown(context.Background()) }()

		client := newTokenIdentityClientSideRefreshTestClient(t, node, startIdentity, func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				TokenIdentity: &newIdentity,
			}, errors.New("refresh failed"))
		})

		rwWrapper := testReplyWriterWrapper()
		err := client.handleRefresh(&protocol.RefreshRequest{
			Token: "test",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.NotNil(t, rwWrapper.replies[0].Error)
		require.Equal(t, startIdentity, client.TokenIdentity())
	})
}
