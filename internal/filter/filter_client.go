package filter

import (
	"fmt"

	"github.com/centrifugal/protocol"
)

// Limits of a filter that a client sends: the nodes of its tree and the values of its comparisons. The filter is
// matched for every publication of every subscriber, so a client may not send one that costs more than these.
const (
	MaxClientNodes  = 256
	MaxClientValues = 1024
)

// ValidateClient is Validate with the limits of MaxClientNodes and MaxClientValues. It checks the filters that
// clients send. The filters the application sets on the server are checked by Validate only. The count is a second
// walk of the tree, after Validate has checked it.
func ValidateClient(f *protocol.FilterNode) error {
	if err := Validate(f); err != nil {
		return err
	}
	var nodes, values int
	countFilter(f, &nodes, &values)
	if nodes > MaxClientNodes {
		return fmt.Errorf("filter has more than %d nodes", MaxClientNodes)
	}
	if values > MaxClientValues {
		return fmt.Errorf("filter has more than %d values", MaxClientValues)
	}
	return nil
}

// countFilter counts the nodes of the tree and the values of its comparisons. The tree is valid, see Validate.
func countFilter(f *protocol.FilterNode, nodes *int, values *int) {
	*nodes++
	*values += len(f.Vals)
	for _, child := range f.Nodes {
		countFilter(child, nodes, values)
	}
}
