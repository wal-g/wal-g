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

const (
	sizeofInt32bits = sizeofInt32 * 8
)

func ParseLsn(lsnStr string) (lsn uint64, err error) {
	lsnArray := strings.SplitN(lsnStr, "/", 2)

	//Postgres format it's LSNs as two hex numbers separated by "/"
	highLsn, err := strconv.ParseUint(lsnArray[0], 0x10, sizeofInt32bits)
	lowLsn, err2 := strconv.ParseUint(lsnArray[1], 0x10, sizeofInt32bits)
	if err != nil || err2 != nil {
		err = errors.New("Unable to parse LSN " + lsnStr)
	}

	lsn = highLsn<<sizeofInt32bits + lowLsn
	return
}

const (
	walSegmentSize        = uint64(16 * 1024 * 1024)     // xlog.c line 113
	walFileFormat         = "%08X%08X%08X"               // xlog_internal.h line 155
	xLogSegmentsPerXLogId = 0x100000000 / walSegmentSize // xlog_internal.h line 101
)

func WALFileName(lsn uint64, conn *pgx.Conn) (string, uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return "", 0, err
	}

	logSegNo := (lsn - uint64(1)) / walSegmentSize // xlog_internal.h line 121

	return formatWALFileName(timeline, logSegNo), timeline, nil
}
func formatWALFileName(timeline uint32, logSegNo uint64) string {
	return fmt.Sprintf(walFileFormat, timeline, logSegNo/xLogSegmentsPerXLogId, logSegNo%xLogSegmentsPerXLogId)
}

func NextWALFileName(name string) (nextname string, err error) {
	if len(name) != 24 {
		err = errors.New("Not a WAL file name: " + name)
		return
	}
	timelineId, err := strconv.ParseUint(name[0:8], 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	logSegNoHi, err := strconv.ParseUint(name[8:16], 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	logSegNoLo, err := strconv.ParseUint(name[16:24], 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	if logSegNoLo >= xLogSegmentsPerXLogId {
		err = errors.New("Incrorrect logSegNoLo in WAL file name: " + name)
		return
	}

	logSegNo := logSegNoHi*xLogSegmentsPerXLogId + logSegNoLo
	logSegNo++
	return formatWALFileName(uint32(timelineId), logSegNo), nil
}
