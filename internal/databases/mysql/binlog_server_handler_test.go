package mysql

import (
	"encoding/binary"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// gtidEvent builds a BinlogEvent whose RawData decodes to the given (sid, gno).
func gtidEvent(t *testing.T, sid uuid.UUID, gno int64) *replication.BinlogEvent {
	t.Helper()
	body := make([]byte, 25) // CommitFlag(1) + SID(16) + GNO(8)
	copy(body[1:17], sid[:])
	binary.LittleEndian.PutUint64(body[17:25], uint64(gno))
	raw := append(make([]byte, replication.EventHeaderSize), body...)
	return &replication.BinlogEvent{
		Header:  &replication.EventHeader{EventType: replication.GTID_EVENT},
		RawData: raw,
	}
}

func TestDecideSkipForGTID(t *testing.T) {
	sidA := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	sidB := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	required, _ := mysql.ParseMysqlGTIDSet(sidA.String() + ":1-10," + sidB.String() + ":5")
	newHandler := func() *Handler {
		empty, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
		return &Handler{
			sentGTIDs:     empty,
			requiredGTIDs: required.(*mysql.MysqlGTIDSet),
		}
	}

	t.Run("GTID already applied is skipped, not recorded", func(t *testing.T) {
		h := newHandler()
		assert.True(t, h.decideSkipForGTID(gtidEvent(t, sidA, 5)))
		assert.True(t, h.skipCurrentTxn)
		assert.True(t, h.sentGTIDs.IsEmpty())
	})

	t.Run("new GTID is forwarded and recorded", func(t *testing.T) {
		h := newHandler()
		assert.False(t, h.decideSkipForGTID(gtidEvent(t, sidA, 11)))
		assert.False(t, h.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":11", h.sentGTIDs.String())
	})

	t.Run("nil requiredGTIDs forwards everything", func(t *testing.T) {
		h := newHandler()
		h.requiredGTIDs = nil
		assert.False(t, h.decideSkipForGTID(gtidEvent(t, sidA, 5)))
		assert.False(t, h.skipCurrentTxn)
		assert.Equal(t, sidA.String()+":5", h.sentGTIDs.String())
	})

	t.Run("skip state is cleared on forwarded GTID", func(t *testing.T) {
		h := newHandler()
		h.skipCurrentTxn = true
		assert.False(t, h.decideSkipForGTID(gtidEvent(t, sidA, 11)))
		assert.False(t, h.skipCurrentTxn)
	})
}
