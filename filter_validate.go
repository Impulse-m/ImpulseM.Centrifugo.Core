package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/filter"
)

// ValidateFilterNode checks a tags filter tree built by the application, for example
// from configuration, with the rules applied to SubscribeOptions.ServerTagsFilter and
// client tags filters. A nil node is an error.
func ValidateFilterNode(f *FilterNode) error {
	return filter.Validate(f)
}
