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
	done := make(chan error, 1)
	go func() {
		conn, err := srv.NewCustomizedConn(serverSide, auth, &Handler{})
		for i := 0; i < 3 && err == nil; i++ {
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
	for _, query := range []string{"SELECT @master_binlog_checksum", "SELECT @source_binlog_checksum"} {
		result, err := conn.Execute(query)
		require.NoError(t, err)
		value, err := result.GetString(0, 0)
		result.Close()
		require.NoError(t, err)
		require.Equal(t, "CRC32", value, query)
	}
	// The replica uses the source clock to calculate Seconds_Behind_Source.
	before := time.Now().Unix()
	result, err := conn.Execute("SELECT UNIX_TIMESTAMP()")
	require.NoError(t, err)
	require.NotNil(t, result.Resultset, "the clock query must return a row, not just an OK packet")
	value, err := result.GetString(0, 0)
	result.Close()
	require.NoError(t, err)
	timestamp, err := strconv.ParseInt(value, 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, timestamp, before)
	require.LessOrEqual(t, timestamp, time.Now().Unix())
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
