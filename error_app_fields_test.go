package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestErrorAppFieldsSurviveProtoAndJSON(t *testing.T) {
	t.Parallel()
	appErr := &Error{
		Code:      400,
		Message:   "quota exceeded",
		Temporary: true,
		AppCode:   420,
		AppType:   "QUOTA_EXCEEDED_17",
		Metadata:  map[string]string{"retry_token": "abc"},
	}
	protoErr := appErr.toProto()
	require.Equal(t, int32(420), protoErr.AppCode)
	require.Equal(t, "QUOTA_EXCEEDED_17", protoErr.AppType)
	require.Equal(t, map[string]string{"retry_token": "abc"}, protoErr.Metadata)

	for _, pt := range []protocol.Type{protocol.TypeJSON, protocol.TypeProtobuf} {
		data, err := protocol.GetReplyEncoder(pt).Encode(&protocol.Reply{Error: protoErr})
		require.NoError(t, err)
		var decoded *protocol.Reply
		if pt == protocol.TypeJSON {
			decoded = decodeReply(t, pt, data)
		} else {
			// Protobuf replies are unframed on this path; the framed decoder is for streams.
			decoded = &protocol.Reply{}
			require.NoError(t, decoded.UnmarshalCF(data))
		}
		require.NotNil(t, decoded.Error)
		require.Equal(t, uint32(400), decoded.Error.Code)
		require.Equal(t, int32(420), decoded.Error.AppCode)
		require.Equal(t, "QUOTA_EXCEEDED_17", decoded.Error.AppType)
		require.Equal(t, map[string]string{"retry_token": "abc"}, decoded.Error.Metadata)
	}
}

func TestErrorAppFieldsReachRPCReply(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			cb(RPCReply{}, &Error{
				Code:    400,
				Message: "quota exceeded",
				AppCode: 420,
				AppType: "QUOTA_EXCEEDED_17",
			})
		})
	})
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	err := client.handleRPC(&protocol.RPCRequest{}, &protocol.Command{Id: 2}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.NotNil(t, rw.replies[0].Error)
	require.Equal(t, int32(420), rw.replies[0].Error.AppCode)
	require.Equal(t, "QUOTA_EXCEEDED_17", rw.replies[0].Error.AppType)
}
