package centrifuge

import (
	"github.com/centrifugal/protocol"
)

// ConnectionFlagRPCOptions is advertised by a client in ConnectRequest.Flag when it
// sends the per-call RPC fields: headers, msg_id, ack, retransmit, after,
// without_updates and cancel. Extension flags start at bit 32; upstream uses the low bits.
const ConnectionFlagRPCOptions int64 = 1 << 32

// applyConnectReply copies the extension fields of a connect reply into the result.
// Flags are applied only where the client advertised them.
func applyConnectReply(res *protocol.ConnectResult, reply ConnectReply, clientFlags int64) {
	res.Flag |= reply.Flags & clientFlags
	res.User = reply.User
	res.AuthClass = reply.AuthClass
}

// applyRefreshReply copies the extension fields of a refresh reply into the result.
func applyRefreshReply(res *protocol.RefreshResult, reply RefreshReply) {
	res.User = reply.User
	res.AuthClass = reply.AuthClass
}
