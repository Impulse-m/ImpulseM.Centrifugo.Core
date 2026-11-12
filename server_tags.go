package centrifuge

import "maps"

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
