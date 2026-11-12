package centrifuge

import (
	"maps"

	"github.com/centrifugal/centrifuge/internal/filter"
)

// WithServerTags allows setting Publication.ServerTags. Server tags are visible
// only to server-side subscription filters and never reach clients.
func WithServerTags(tags map[string]string) PublishOption {
	return func(opts *PublishOptions) {
		opts.ServerTags = tags
	}
}

// serverFilterTags returns the tags a server-side subscription filter matches
// against: Publication.Tags merged with Publication.ServerTags. When the
// publication has no server tags it returns Tags itself, so the common case
// neither copies nor allocates. Otherwise it builds a new map once and the
// caller reuses it for every subscriber of the publication. Neither input is
// modified. On a key present in both maps the server tag wins, though the
// server API refuses such collisions on publish.
func serverFilterTags(pub *Publication) map[string]string {
	if len(pub.ServerTags) == 0 {
		return pub.Tags
	}
	union := make(map[string]string, len(pub.Tags)+len(pub.ServerTags))
	maps.Copy(union, pub.Tags)
	maps.Copy(union, pub.ServerTags)
	return union
}

// serverPublicationFiltered reports whether the server-side filter excludes the
// publication. It matches against serverFilterTags, so the union is built only
// for a filtered subscription. A nil filter never excludes.
func serverPublicationFiltered(pub *Publication, serverTf *tagsFilter) bool {
	if serverTf == nil {
		return false
	}
	return publicationFiltered(serverFilterTags(pub), serverTf)
}

// serverFilterTagsCache gives the server filters of one publication their view
// of its tags. The union from serverFilterTags is built on the first match and
// reused for every later subscriber of the publication, so it is built at most
// once and only when a server-filtered subscriber is met. Client filters do not
// use it: they match Publication.Tags.
type serverFilterTagsCache struct {
	pub   *Publication
	tags  map[string]string
	built bool
}

// match reports whether the server filter tf matches the publication. An error
// from the filter does not match.
func (c *serverFilterTagsCache) match(tf *tagsFilter) bool {
	if !c.built {
		c.tags = serverFilterTags(c.pub)
		c.built = true
	}
	match, _ := filter.Match(tf.filter, c.tags)
	return match
}
