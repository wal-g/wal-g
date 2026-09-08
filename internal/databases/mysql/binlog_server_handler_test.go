package mysql

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gtidRequestHandler struct {
	server.EmptyHandler
	received chan *mysql.MysqlGTIDSet
}

var _ server.ReplicationHandler = (*gtidRequestHandler)(nil)

func (h *gtidRequestHandler) HandleQuery(query string) (*mysql.Result, error) {
	return (&Handler{}).HandleQuery(query)
}

func (h *gtidRequestHandler) HandleRegisterSlave([]byte) error {
	return nil
}

func (h *gtidRequestHandler) HandleBinlogDump(mysql.Position) (*replication.BinlogStreamer, error) {
	return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "expected a GTID request")
}

func (h *gtidRequestHandler) HandleBinlogDumpGTID(set *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	h.received <- set
	return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "end of test stream")
}

func TestBinlogProtocolHandshakeAndGTIDRequests(t *testing.T) {
	for _, tc := range []struct {
		name  string
		gtids string
	}{
		{"untagged (5.7 and 8.0)", uuid1.String() + ":1-5"},
		{"tagged (8.4 and 9.7)", uuid1.String() + ":review:1-5"},
		{"large tagged GNO", uuid1.String() + ":review:36028797018963968-36028797018963970"},
		{"mixed", uuid1.String() + ":1-5:review:1-5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newBinlogProtocolServer()
			// The version change must not change authentication/capabilities for
			// older replicas (in particular, do not require TLS or caching_sha2).
			legacy := server.NewServer("5.7.42", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
			require.Equal(t, legacy.Capability(), srv.Capability())
			auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
			require.NoError(t, auth.AddUser("replica", "password"))
			handler := &gtidRequestHandler{received: make(chan *mysql.MysqlGTIDSet, 1)}
			serverSide, clientSide := net.Pipe()
			defer serverSide.Close()
			defer clientSide.Close()
			require.NoError(t, serverSide.SetDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, clientSide.SetDeadline(time.Now().Add(5*time.Second)))
			done := make(chan error, 1)
			go func() {
				conn, err := srv.NewCustomizedConn(serverSide, auth, handler)
				// Two checksum queries followed by COM_BINLOG_DUMP_GTID.
				for i := 0; i < 3 && err == nil; i++ {
					err = conn.HandleCommand()
				}
				done <- err
			}()
			conn, err := client.ConnectWithDialer(context.Background(), "tcp", "unused:3306", "replica", "password", "",
				func(context.Context, string, string) (net.Conn, error) { return clientSide, nil })
			require.NoError(t, err)
			// MYSQL_TAGGED_GTIDS_VERSION_SUPPORT in MySQL's include/mysql.h.
			version, err := conn.CompareServerVersion("8.3.0")
			require.NoError(t, err)
			require.GreaterOrEqual(t, version, 0, "older advertised versions suppress tagged GTIDs")

			for _, query := range []string{"SELECT @master_binlog_checksum", "SELECT @source_binlog_checksum"} {
				result, err := conn.Execute(query)
				require.NoError(t, err)
				value, err := result.GetString(0, 0)
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "CRC32", value, query)
			}

			want := requireGTIDSet(t, tc.gtids)
			encoded := want.Encode()
			data := append(make([]byte, 4), mysql.COM_BINLOG_DUMP_GTID)
			data = binary.LittleEndian.AppendUint16(data, 0)   // flags
			data = binary.LittleEndian.AppendUint32(data, 123) // replica server ID
			data = binary.LittleEndian.AppendUint32(data, 0)   // empty binlog name
			data = binary.LittleEndian.AppendUint64(data, 4)   // position
			data = binary.LittleEndian.AppendUint32(data, uint32(len(encoded)))
			data = append(data, encoded...)
			conn.ResetSequence()
			require.NoError(t, conn.WritePacket(data))
			_, err = conn.ReadPacket() // consume the intentional end-of-test error
			require.NoError(t, err)
			require.NoError(t, <-done)
			select {
			case got := <-handler.received:
				require.True(t, want.Equal(got), "received %s, expected %s", got, want)
			default:
				t.Fatal("GTID request was not dispatched")
			}
		})
	}
}

func TestHandleQuerySupportsSourceAndReplicaTerminology(t *testing.T) {
	testCases := []struct {
		name          string
		query         string
		expectedValue string
	}{
		{name: "legacy checksum", query: "SELECT @master_binlog_checksum", expectedValue: "CRC32"},
		{name: "source checksum", query: "SELECT @source_binlog_checksum", expectedValue: "CRC32"},
		{name: "legacy semi-sync", query: "SELECT @@global.rpl_semi_sync_master_enabled", expectedValue: "0"},
		{name: "source semi-sync", query: "SELECT @@global.rpl_semi_sync_source_enabled", expectedValue: "0"},
	}

	handler := &Handler{}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := handler.HandleQuery(testCase.query)
			require.NoError(t, err)
			require.NotNil(t, result)

			require.Len(t, result.RowDatas, 1)
			values, err := result.RowDatas[0].Parse(result.Fields, false, nil)
			require.NoError(t, err)
			require.Len(t, values, 1)
			assert.Equal(t, testCase.expectedValue, string(values[0].AsString()))
		})
	}
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
