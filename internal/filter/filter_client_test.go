package filter

import (
	"fmt"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// notChain returns a NOT chain of the given number of NOT nodes over one leaf.
func notChain(depth int) *protocol.FilterNode {
	node := &protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}
	for range depth {
		node = &protocol.FilterNode{Op: OpNot, Nodes: []*protocol.FilterNode{node}}
	}
	return node
}

func TestValidateClientRejectsLargeFilters(t *testing.T) {
	t.Parallel()
	const maxNodes = 256
	const maxValues = 1024
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}, maxNodes, maxValues))
	require.NoError(t, ValidateClient(notChain(maxNodes-1), maxNodes, maxValues))
	require.Error(t, ValidateClient(notChain(maxNodes), maxNodes, maxValues))

	vals := make([]string, maxValues+1)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals[:maxValues]}, maxNodes, maxValues))
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, maxNodes, maxValues))
}

// TestValidateClientDefaultLimits checks that a zero limit selects the default limits, and that a negative limit
// means no limit.
func TestValidateClientDefaultLimits(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateClient(notChain(DefaultClientMaxNodes-1), 0, 0))
	require.Error(t, ValidateClient(notChain(DefaultClientMaxNodes), 0, 0))
	require.NoError(t, ValidateClient(notChain(1000), -1, -1))

	vals := make([]string, DefaultClientMaxValues+1)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, 0))
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, -1))
}

// TestValidateClientLimitsAreIndependent checks that each limit applies on its own when the other one is not set.
func TestValidateClientLimitsAreIndependent(t *testing.T) {
	t.Parallel()
	require.Error(t, ValidateClient(notChain(10), 5, 0))
	require.NoError(t, ValidateClient(notChain(10), 0, 1))

	vals := make([]string, 10)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 0, 5))
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}, 5, 0))
}

// TestValidateKeepsServerFilterLimits checks that Validate itself keeps no node limit on the filters the
// application sets on the server.
func TestValidateKeepsServerFilterLimits(t *testing.T) {
	t.Parallel()
	require.NoError(t, Validate(notChain(256)))
}
