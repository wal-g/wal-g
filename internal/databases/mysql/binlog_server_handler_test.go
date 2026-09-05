package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleQuerySupportsSourceAndReplicaTerminology(t *testing.T) {
	testCases := []struct {
		name          string
		query         string
		expectedValue string
	}{
		{name: "legacy checksum", query: "SELECT @master_binlog_checksum", expectedValue: "CRC32"},
		{name: "source checksum", query: "SELECT @source_binlog_checksum", expectedValue: "1"},
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
		assert.True(t, p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 5)))
		assert.True(t, p.skipCurrentTxn)
		assert.True(t, p.sentGTIDs.IsEmpty())
	})

	t.Run("new GTID is forwarded and recorded", func(t *testing.T) {
		p := newProcessor()
		assert.False(t, p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 11)))
		assert.False(t, p.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":11", p.sentGTIDs.String())
	})

	t.Run("nil requiredGTIDs forwards everything", func(t *testing.T) {
		p := newProcessor()
		p.requiredGTIDs = nil
		assert.False(t, p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 5)))
		assert.False(t, p.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":5", p.sentGTIDs.String())
	})

	t.Run("skip state is cleared on forwarded GTID", func(t *testing.T) {
		p := newProcessor()
		p.skipCurrentTxn = true
		assert.False(t, p.decideSkipForGTID(gtidEvent("2026-01-01 00:00:01", sidA, 11)))
		assert.False(t, p.skipCurrentTxn)
	})
}
