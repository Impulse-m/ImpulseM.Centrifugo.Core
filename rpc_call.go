package centrifuge

import (
	"github.com/centrifugal/protocol"
)

// RPCCallOptions carries per-call options the client sent with an RPC command.
type RPCCallOptions struct {
	// Headers are per-call headers sent by the client.
	Headers map[string]string
	// MsgID is the client request id. Zero means no dedup or ordering.
	MsgID uint64
	// Ack lists msg_ids whose replies the client has received.
	Ack []uint64
	// Retransmit is true when this call is a resend after transport loss.
	Retransmit bool
	// After lists msg_ids this call must run after.
	After []uint64
	// WithoutUpdates asks that updates are not delivered for this call.
	WithoutUpdates bool
	// Cancel is the msg_id of a call to cancel. Method and Data are empty for a cancel request.
	Cancel uint64
}

func rpcCallOptionsFromProto(req *protocol.RPCRequest) RPCCallOptions {
	return RPCCallOptions{
		Headers:        req.Headers,
		MsgID:          req.MsgId,
		Ack:            req.Ack,
		Retransmit:     req.Retransmit,
		After:          req.After,
		WithoutUpdates: req.WithoutUpdates,
		Cancel:         req.Cancel,
	}
}

func rpcResultFromReply(reply RPCReply) *protocol.RPCResult {
	return &protocol.RPCResult{
		Data:   reply.Data,
		Cached: reply.Cached,
	}
}

// redactRPCHeaders redacts RPC headers for logging. It replaces the map rather than
// writing into it: the handler may still read the headers through RPCEvent.Call,
// which shares this map.
func redactRPCHeaders(cmd *protocol.Command) {
	if cmd.Rpc == nil || len(cmd.Rpc.Headers) == 0 {
		return
	}
	redactedHeaders := make(map[string]string, len(cmd.Rpc.Headers))
	for k := range cmd.Rpc.Headers {
		redactedHeaders[k] = redacted
	}
	cmd.Rpc.Headers = redactedHeaders
}
