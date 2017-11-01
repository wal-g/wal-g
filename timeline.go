package walg

import (
	"fmt"
	"strings"
	"strconv"
	"errors"
	"github.com/jackc/pgx"
)

func readTimeline(conn *pgx.Conn) (timeline uint32, err error) {
	var bytes_per_wal_segment uint32
	err = conn.QueryRow("select timeline_id, bytes_per_wal_segment from pg_control_checkpoint(), pg_control_init()").Scan(&timeline, &bytes_per_wal_segment)
	if err == nil && uint64(bytes_per_wal_segment) != walSegmentSize {
		return 0, errors.New("bytes_per_wal_segment of the server does not match expected value")
	}
	return
}

func ParseLsn(lsnStr string) (lsn uint64, err error) {
	lsnArray := strings.SplitN(lsnStr, "/", 2)

	//Postgres format it's LSNs as two hex numbers separated by "/"
	const (
		sizeofInt32     = 4
		sizeofInt32bits = sizeofInt32 * 8
	)
	highLsn, err := strconv.ParseUint(lsnArray[0], 0x10, sizeofInt32bits)
	lowLsn, err2 := strconv.ParseUint(lsnArray[1], 0x10, sizeofInt32bits)
	if err != nil || err2 != nil {
		err = errors.New("Unable to parse LSN " + lsnStr)
	}

	lsn = highLsn<<sizeofInt32bits + lowLsn
	return
}

const (
	walSegmentSize = uint64(16 * 1024 * 1024) // xlog.c line 113
	walFileFormat  = "%08X%08X%08X"           // xlog_internal.h line 155
)

func WALFileName(lsn uint64, conn *pgx.Conn) (string, uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return "", 0, err
	}

	XLogSegmentsPerXLogId := 0x100000000 / walSegmentSize // xlog_internal.h line 101
	logSegNo := (lsn - uint64(1)) / walSegmentSize        // xlog_internal.h line 121

	return fmt.Sprintf(walFileFormat, timeline, logSegNo/XLogSegmentsPerXLogId, logSegNo%XLogSegmentsPerXLogId), timeline, nil
}
