package mysql

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinlogProtocolHandshakeAndInitialization(t *testing.T) {
	srv := newBinlogProtocolServer()
	legacy := server.NewServer("5.7.42", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	require.Equal(t, legacy.Capability(), srv.Capability())
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, auth.AddUser("replica", "password"))
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()
	defer clientSide.Close()
	require.NoError(t, serverSide.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, clientSide.SetDeadline(time.Now().Add(5*time.Second)))
	queries := []struct {
		query, column, value string
		fieldType            uint8
	}{
		{"SELECT @master_binlog_checksum", "master_binlog_checksum", "CRC32", mysql.MYSQL_TYPE_VAR_STRING},
		{"SELECT UNIX_TIMESTAMP()", "UNIX_TIMESTAMP()", "", mysql.MYSQL_TYPE_LONGLONG},
		{"SELECT @source_binlog_checksum", "source_binlog_checksum", "CRC32", mysql.MYSQL_TYPE_VAR_STRING},
		{"SELECT @@global.server_id", "SERVER_ID", "99", mysql.MYSQL_TYPE_LONGLONG},
		{"SELECT @@global.gtid_mode", "GTID_MODE", "ON", mysql.MYSQL_TYPE_VAR_STRING},
		{"SELECT @@global.server_uuid", "SERVER_UUID", "0", mysql.MYSQL_TYPE_VAR_STRING},
		{"SELECT @@global.rpl_semi_sync_master_enabled", "@@global.rpl_semi_sync_master_enabled", "0", mysql.MYSQL_TYPE_VAR_STRING},
		{"SELECT @@global.rpl_semi_sync_source_enabled", "@@global.rpl_semi_sync_source_enabled", "0", mysql.MYSQL_TYPE_VAR_STRING},
	}
	done := make(chan error, 1)
	go func() {
		handler := &Handler{dumpCommandProcessor: &BinlogDumpProcessor{serverID: 99}}
		conn, err := srv.NewCustomizedConn(serverSide, auth, handler)
		for i := 0; i < len(queries) && err == nil; i++ {
			err = conn.HandleCommand()
		}
		done <- err
	}()
	conn, err := client.ConnectWithDialer(context.Background(), "tcp", "unused:3306", "replica", "password", "",
		func(context.Context, string, string) (net.Conn, error) { return clientSide, nil })
	require.NoError(t, err)
	// MySQL suppresses tagged GTIDs when the source advertises an older version.
	version, err := conn.CompareServerVersion("8.3.0")
	require.NoError(t, err)
	require.GreaterOrEqual(t, version, 0)
	// Alternate string and numeric columns, closing each response as a replica
	// does. Reused go-mysql resultsets must not leak column metadata across queries.
	for _, query := range queries {
		before := time.Now().Unix()
		result, err := conn.Execute(query.query)
		require.NoError(t, err)
		require.NotNil(t, result.Resultset, query.query)
		require.Len(t, result.Fields, 1, query.query)
		require.Equal(t, query.column, string(result.Fields[0].Name), query.query)
		require.Equal(t, query.fieldType, result.Fields[0].Type, query.query)
		value, err := result.GetString(0, 0)
		result.Close()
		require.NoError(t, err)
		if query.query == "SELECT UNIX_TIMESTAMP()" {
			// The replica uses the source clock to calculate Seconds_Behind_Source.
			timestamp, err := strconv.ParseInt(value, 10, 64)
			require.NoError(t, err)
			require.GreaterOrEqual(t, timestamp, before)
			require.LessOrEqual(t, timestamp, time.Now().Unix())
		} else {
			require.Equal(t, query.value, value, query.query)
		}
	}
	require.NoError(t, <-done)
}

func TestDecideSkipForGTID(t *testing.T) {
	sidA := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	sidB := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	required, _ := mysql.ParseMysqlGTIDSet(sidA.String() + ":1-10," + sidB.String() + ":5")
	newProcessor := func() *BinlogDumpProcessor {
		empty, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
		return &BinlogDumpProcessor{
			sentGTIDs:     empty,
			requiredGTIDs: required.(*mysql.MysqlGTIDSet),
		}
	}

	t.Run("GTID already applied is skipped, not recorded", func(t *testing.T) {
		p := newProcessor()
		skip, err := p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 5))
		require.NoError(t, err)
		assert.True(t, skip)
		assert.True(t, p.skipCurrentTxn)
		assert.True(t, p.sentGTIDs.IsEmpty())
	})

	t.Run("new GTID is forwarded and recorded", func(t *testing.T) {
		p := newProcessor()
		skip, err := p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 11))
		require.NoError(t, err)
		assert.False(t, skip)
		assert.False(t, p.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":11", p.sentGTIDs.String())
	})

	t.Run("nil requiredGTIDs forwards everything", func(t *testing.T) {
		p := newProcessor()
		p.requiredGTIDs = nil
		skip, err := p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 5))
		require.NoError(t, err)
		assert.False(t, skip)
		assert.False(t, p.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":5", p.sentGTIDs.String())
	})

	t.Run("skip state is cleared on forwarded GTID", func(t *testing.T) {
		p := newProcessor()
		p.skipCurrentTxn = true
		skip, err := p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 11))
		require.NoError(t, err)
		assert.False(t, skip)
		assert.False(t, p.skipCurrentTxn)
	})
}
