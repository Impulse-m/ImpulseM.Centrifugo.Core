package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClientExtAllocatedOnlyWhenSet checks that a connection without a token identity or extension state holds no
// fork state, and that the first non-zero identity allocates it.
func TestClientExtAllocatedOnlyWhenSet(t *testing.T) {
	client := &Client{}
	client.storeUser("user-42")
	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{})
	client.mu.Unlock()
	require.Nil(t, client.ext.Load())
	require.Nil(t, client.ExtState())

	client.mu.Lock()
	client.setTokenIdentity(TokenIdentity{Generation: 1})
	client.mu.Unlock()
	require.NotNil(t, client.ext.Load())
	require.Equal(t, uint64(1), client.TokenIdentity().Generation)
}
