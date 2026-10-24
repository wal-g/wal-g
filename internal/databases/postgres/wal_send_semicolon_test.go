package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

// TestWalSendHandlesSemicolonCommands checks that replication commands
// terminated with a semicolon, as psql sends them, are accepted.
func TestWalSendHandlesSemicolonCommands(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	results, err := conn.Exec(ctx, "IDENTIFY_SYSTEM;").ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)

	// An absent file must produce an ErrorResponse and finish the query, so
	// the same connection remains usable for the following START_REPLICATION.
	_, err = conn.Exec(ctx, "TIMELINE_HISTORY 4;").ReadAll()
	require.ErrorContains(t, err, "could not receive the timeline history file")

	conn.Frontend().Send(&pgproto3.Query{String: "START_REPLICATION PHYSICAL 8666/A7000000 TIMELINE 4 ;"})
	require.NoError(t, conn.Frontend().Flush())
	msg, err := conn.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.IsType(t, &pgproto3.CopyBothResponse{}, msg)
}

// START_REPLICATION [SLOT slot] [PHYSICAL] %X/%08X [TIMELINE %u]
func TestWalSendStartReplicationWithoutPhysical(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	conn.Frontend().Send(&pgproto3.Query{String: "START_REPLICATION SLOT some_slot 8666/A7000000 TIMELINE 4"})
	require.NoError(t, conn.Frontend().Flush())
	msg, err := conn.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.IsType(t, &pgproto3.CopyBothResponse{}, msg)
}
