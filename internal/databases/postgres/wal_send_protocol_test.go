package postgres

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func walSendProtocolPipe(t *testing.T) (net.Conn, *pgproto3.Frontend, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	server, client := net.Pipe()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		_ = server.Close()
		walSendAwait(t, done, "protocol connection to exit")
	})
	s := walSendRegressionServer(t)
	s.pollInterval = time.Hour
	go func() { defer close(done); s.handleConnection(ctx, server) }()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	return client, pgproto3.NewFrontend(client, client), done
}

func TestWalSendRejectsMalformedFrontendMessages(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kind     byte
		body     []byte
		bodySize uint32
		copyMode bool
	}{
		{name: "unterminated_query", kind: 'Q', body: []byte("IDENTIFY_SYSTEM")},
		{name: "query_trailing_data", kind: 'Q', body: []byte("IDENTIFY_SYSTEM\x00extra\x00")},
		{name: "copy_done_payload", kind: 'c', body: []byte{1}, copyMode: true},
		{name: "oversized_query", kind: 'Q', bodySize: walSendMaxMessageLen + 1},
		{name: "oversized_copy_data", kind: 'd', bodySize: walSendMaxMessageLen + 1, copyMode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, frontend, done := walSendProtocolPipe(t)
			walSendPipeStartup(t, frontend)
			if tc.copyMode {
				walSendPipeStartCopy(t, frontend)
			}
			size := tc.bodySize
			if size == 0 {
				size = uint32(len(tc.body))
			}
			// Build invalid packets directly to exercise decoding and size limits.
			packet := binary.BigEndian.AppendUint32([]byte{tc.kind}, size+4)
			packet = append(packet, tc.body...)
			_, err := client.Write(packet)
			require.NoError(t, err)
			_, err = frontend.Receive()
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "malformed messages must close the connection without a response")
			walSendAwait(t, done, "malformed connection to exit")
		})
	}
}

func TestWalSendStartupNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request pgproto3.FrontendMessage
	}{
		{name: "plain"},
		{name: "ssl", request: &pgproto3.SSLRequest{}},
		{name: "gss", request: &pgproto3.GSSEncRequest{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, frontend, done := walSendProtocolPipe(t)
			if tc.request != nil {
				frontend.Send(tc.request)
				require.NoError(t, frontend.Flush())
				var response [1]byte
				_, err := io.ReadFull(client, response[:])
				require.NoError(t, err)
				require.Equal(t, byte('N'), response[0])
			}
			walSendPipeStartup(t, frontend)
			frontend.Send(&pgproto3.Terminate{})
			require.NoError(t, frontend.Flush())
			walSendAwait(t, done, "terminated session")
		})
	}
}

func TestWalSendCancelRequestClosesWithoutReply(t *testing.T) {
	client, frontend, done := walSendProtocolPipe(t)
	frontend.Send(&pgproto3.CancelRequest{ProcessID: 123, SecretKey: []byte{1, 2, 3, 4}})
	require.NoError(t, frontend.Flush())
	var response [1]byte
	_, err := client.Read(response[:])
	require.ErrorIs(t, err, io.EOF)
	walSendAwait(t, done, "cancel request connection")
}

func TestWalSendRejectsUnsupportedProtocolVersion(t *testing.T) {
	_, frontend, done := walSendProtocolPipe(t)
	frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: 12345,
		Parameters: map[string]string{"user": "walg_test", "replication": "true"}})
	require.NoError(t, frontend.Flush())
	_, err := frontend.Receive()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	walSendAwait(t, done, "unsupported startup version")
}

func TestWalSendPipelinedCopyDoneAndQuery(t *testing.T) {
	_, frontend, _ := walSendProtocolPipe(t)
	walSendPipeStartup(t, frontend)
	walSendPipeStartCopy(t, frontend)
	// Receive can buffer the following query while reading CopyDone. The COPY
	// reader and the command loop must hand off the same Backend instance.
	frontend.Send(&pgproto3.CopyDone{})
	frontend.Send(&pgproto3.Query{String: "IDENTIFY_SYSTEM"})
	require.NoError(t, frontend.Flush())
	msg, err := frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.CopyDone{}, msg)
	for _, tag := range []string{"START_STREAMING", "START_REPLICATION"} {
		msg, err = frontend.Receive()
		require.NoError(t, err)
		require.Equal(t, &pgproto3.CommandComplete{CommandTag: []byte(tag)}, msg)
	}
	msg, err = frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.ReadyForQuery{}, msg)
	msg, err = frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.RowDescription{}, msg)
	msg, err = frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.DataRow{}, msg)
	require.Len(t, msg.(*pgproto3.DataRow).Values, 4)
	require.Nil(t, msg.(*pgproto3.DataRow).Values[3])
	msg, err = frontend.Receive()
	require.NoError(t, err)
	require.Equal(t, &pgproto3.CommandComplete{CommandTag: []byte("IDENTIFY_SYSTEM")}, msg)
	msg, err = frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.ReadyForQuery{}, msg)
}
