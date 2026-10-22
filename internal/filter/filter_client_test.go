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
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}))
	require.NoError(t, ValidateClient(notChain(MaxClientNodes-1)))
	require.Error(t, ValidateClient(notChain(MaxClientNodes)))

	vals := make([]string, MaxClientValues+1)
	for i := range vals {
		vals[i] = "v" + fmt.Sprint(i)
	}
	require.NoError(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals[:MaxClientValues]}))
	require.Error(t, ValidateClient(&protocol.FilterNode{Cmp: CompareIn, Key: "env", Vals: vals}))
}

// TestValidateKeepsServerFilterLimits checks that the limits of client filters do not apply to the filters the
// application sets on the server.
func TestValidateKeepsServerFilterLimits(t *testing.T) {
	t.Parallel()
	require.NoError(t, Validate(notChain(MaxClientNodes)))
}
