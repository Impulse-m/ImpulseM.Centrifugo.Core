package centrifuge

// emulationLogFields returns the fields logged for an emulation request: the error, the target node, the body size and
// the protocol. The body, the decoded command data and the session id are never logged. The body may carry a
// connection token or application data, and the session id addresses commands to a live connection.
func emulationLogFields(err error, node string, size int, protocolName string) map[string]any {
	fields := map[string]any{
		"error":    err.Error(),
		"size":     size,
		"protocol": protocolName,
	}
	if node != "" {
		fields["node"] = node
	}
	return fields
}
