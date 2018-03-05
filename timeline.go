package walg

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"strconv"
	"strings"
)

func readTimeline(conn *pgx.Conn) (timeline uint32, err error) {
	var bytes_per_wal_segment uint32

	// TODO: Check if this logic can be moved to queryRunner or abstracted away somehow
	err = conn.QueryRow("select timeline_id, bytes_per_wal_segment from pg_control_checkpoint(), pg_control_init()").Scan(&timeline, &bytes_per_wal_segment)
	if err == nil && uint64(bytes_per_wal_segment) != WalSegmentSize {
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
	WalSegmentSize        = uint64(16 * 1024 * 1024)     // xlog.c line 113
	walFileFormat         = "%08X%08X%08X"               // xlog_internal.h line 155
	xLogSegmentsPerXLogId = 0x100000000 / WalSegmentSize // xlog_internal.h line 101
)

func WALFileName(lsn uint64, conn *pgx.Conn) (string, uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return "", 0, err
	}

	logSegNo := (lsn - uint64(1)) / WalSegmentSize // xlog_internal.h line 121

	return formatWALFileName(timeline, logSegNo), timeline, nil
}
func formatWALFileName(timeline uint32, logSegNo uint64) string {
	return fmt.Sprintf(walFileFormat, timeline, logSegNo/xLogSegmentsPerXLogId, logSegNo%xLogSegmentsPerXLogId)
}

func ParseWALFileName(name string) (timelineId uint32, logSegNo uint64, err error) {
	if len(name) != 24 {
		err = errors.New("Not a WAL file name: " + name)
		return
	}
	timelineId64, err0 := strconv.ParseUint(name[0:8], 0x10, sizeofInt32bits)
	timelineId = uint32(timelineId64)
	if err0 != nil {
		err = err0
		return
	}
	logSegNoHi, err0 := strconv.ParseUint(name[8:16], 0x10, sizeofInt32bits)
	if err0 != nil {
		err = err0
		return
	}
	logSegNoLo, err0 := strconv.ParseUint(name[16:24], 0x10, sizeofInt32bits)
	if err0 != nil {
		err = err0
		return
	}
	if logSegNoLo >= xLogSegmentsPerXLogId {
		err = errors.New("Incorrect logSegNoLo in WAL file name: " + name)
		return
	}

	logSegNo = logSegNoHi*xLogSegmentsPerXLogId + logSegNoLo
	return
}

func NextWALFileName(name string) (nextname string, err error) {
	timelineId, logSegNo, err0 := ParseWALFileName(name)
	if err0 != nil {
		err = err0
		return
	}
	logSegNo++
	return formatWALFileName(uint32(timelineId), logSegNo), nil
}
