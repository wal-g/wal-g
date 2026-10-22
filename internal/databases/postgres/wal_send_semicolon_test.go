package postgres_test

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

// TestWalSendHandlesSemicolonCommands checks that replication commands
// terminated with a semicolon, as psql sends them, are accepted
func TestWalSendHandlesSemicolonCommands(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// read the post-startup messages up to ReadyForQuery
	require.NoError(t, writeStartupMessage(conn))

	// IDENTIFY_SYSTEM; with a semicolon
	require.NoError(t, writeSimpleQuery(conn, "IDENTIFY_SYSTEM;"))

	msgType, payload, err := readMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, byte('T'), msgType, "expect RowDescription for IDENTIFY_SYSTEM;")

	// skip until ReadyForQuery
	deadline := time.Now().Add(5 * time.Second)
	for {
		msgType, _, err = readMessage(conn)
		require.NoError(t, err)
		if msgType == 'Z' {
			break
		}
		require.True(t, time.Now().Before(deadline), "no ReadyForQuery received")
	}

	// TIMELINE_HISTORY 4; with a semicolon must not fail with "invalid timeline"
	require.NoError(t, writeSimpleQuery(conn, "TIMELINE_HISTORY 4;"))
	msgType, payload, err = readMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, byte('E'), msgType, "history file is absent, an error response must come")
	assert.Contains(t, string(payload), "could not receive the timeline history file")

	// the failed query must be completed with ReadyForQuery
	msgType, payload, err = readMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, byte('Z'), msgType, "the failed query must be finished with ReadyForQuery")

	// START_REPLICATION with a trailing ' ;' must not fail with "invalid start LSN":
	// the command parses fine and the server enters copy-both mode
	require.NoError(t, writeSimpleQuery(conn, "START_REPLICATION PHYSICAL 8666/A7000000 TIMELINE 4 ;"))
	msgType, payload, err = readMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, byte('W'), msgType, "expect CopyBothResponse for START_REPLICATION")
}

// TestWalSendStartReplicationWithoutPhysical checks that the PHYSICAL keyword
// in START_REPLICATION is optional, as in the real walsender:
// START_REPLICATION [SLOT slot] [PHYSICAL] %X/%08X [TIMELINE %u]
func TestWalSendStartReplicationWithoutPhysical(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	require.NoError(t, writeStartupMessage(conn))

	// START_REPLICATION without the PHYSICAL keyword must be accepted
	require.NoError(t, writeSimpleQuery(conn, "START_REPLICATION SLOT some_slot 8666/A7000000 TIMELINE 4"))
	msgType, _, err := readMessage(conn)
	require.NoError(t, err)
	assert.Equal(t, byte('W'), msgType, "expect CopyBothResponse for START_REPLICATION without PHYSICAL")
}

func writeStartupMessage(conn net.Conn) error {
	params := "user\x00postgres\x00replication\x00database\x00\x00"
	body := binary.BigEndian.AppendUint32(nil, 196608)
	body = append(body, params...)
	msg := binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))
	msg = append(msg, body...)
	_, err := conn.Write(msg)
	if err != nil {
		return err
	}

	// read up to ReadyForQuery
	for {
		msgType, _, err := readMessage(conn)
		if err != nil {
			return err
		}
		if msgType == 'Z' {
			return nil
		}
	}
}

func writeSimpleQuery(conn net.Conn, query string) error {
	body := append([]byte(query), 0)
	msg := []byte{'Q'}
	msg = binary.BigEndian.AppendUint32(msg, uint32(4+len(body)))
	msg = append(msg, body...)
	_, err := conn.Write(msg)
	return err
}

func readMessage(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return 0, nil, err
	}
	msgLen := binary.BigEndian.Uint32(header[1:5])
	if msgLen < 4 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	// nolint:gosec
	payload := make([]byte, msgLen-4)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}
