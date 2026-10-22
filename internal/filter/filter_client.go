package filter

import (
	"fmt"

	"github.com/centrifugal/protocol"
)

// Default limits of the size of a client filter, used when the config sets a limit of zero.
const (
	DefaultClientMaxNodes  = 256
	DefaultClientMaxValues = 1024
)

// ValidateClient is Validate with the limits on the size of a filter that a client sends: at most maxNodes nodes
// of its tree and at most maxValues values of its comparisons. A limit of zero selects the default limit, and a
// negative limit means no limit. The filters the application sets on the server are checked by Validate only.
// The size is counted by a second walk of the tree.
func ValidateClient(f *protocol.FilterNode, maxNodes int, maxValues int) error {
	if err := Validate(f); err != nil {
		return err
	}
	maxNodes = clientLimit(maxNodes, DefaultClientMaxNodes)
	maxValues = clientLimit(maxValues, DefaultClientMaxValues)
	if maxNodes <= 0 && maxValues <= 0 {
		return nil
	}
	var nodes, values int
	countFilter(f, &nodes, &values)
	if maxNodes > 0 && nodes > maxNodes {
		return fmt.Errorf("filter has more than %d nodes", maxNodes)
	}
	if maxValues > 0 && values > maxValues {
		return fmt.Errorf("filter has more than %d values", maxValues)
	}
	return nil
}

// clientLimit returns the limit to check: the default for zero, and zero, which means no limit, for a negative limit.
func clientLimit(limit int, defaultLimit int) int {
	switch {
	case limit == 0:
		return defaultLimit
	case limit < 0:
		return 0
	default:
		return limit
	}
}

// countFilter counts the nodes of the tree and the values of its comparisons. The tree is valid, see Validate.
func countFilter(f *protocol.FilterNode, nodes *int, values *int) {
	*nodes++
	*values += len(f.Vals)
	for _, child := range f.Nodes {
		countFilter(child, nodes, values)
	}
}
