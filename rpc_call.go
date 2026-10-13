package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// ConnectionFlagRPCOptions is advertised by a client in ConnectRequest.Flag when it
// sends the per-call RPC fields: headers, msg_id, ack, retransmit, after,
// without_updates and cancel. Fork flags start at bit 32; upstream uses the low bits.
const ConnectionFlagRPCOptions int64 = 1 << 32

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

// replyTime returns server time in Unix milliseconds for command replies, or zero
// when Config.ReplyIncludeServerTime is disabled.
func (c *Client) replyTime() int64 {
	if !c.node.config.ReplyIncludeServerTime {
		return 0
	}
	return time.Now().UnixMilli()
}
