package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// binlogFileStreamStats describes source events read from one binlog file.
// Artificial rotate and heartbeat events are not part of these statistics.
type binlogFileStreamStats struct {
	events         int
	gtidCount      int
	firstTimestamp uint32
	lastTimestamp  uint32
	firstGTID      string
	lastGTID       string
	eventTypes     [256]int
}

func (s *binlogFileStreamStats) addEvent(e *replication.BinlogEvent) {
	if s.events == 0 {
		s.firstTimestamp = e.Header.Timestamp
	}
	s.events++
	s.lastTimestamp = e.Header.Timestamp
	s.eventTypes[byte(e.Header.EventType)]++
}

func (s *binlogFileStreamStats) addGtid(gtid string) {
	if s.gtidCount == 0 {
		s.firstGTID = gtid
	}
	s.gtidCount++
	s.lastGTID = gtid
}

func (s *binlogFileStreamStats) String() string {
	firstTS, lastTS := "-", "-"
	if s.events > 0 {
		firstTS = time.Unix(int64(s.firstTimestamp), 0).UTC().Format(time.RFC3339)
		lastTS = time.Unix(int64(s.lastTimestamp), 0).UTC().Format(time.RFC3339)
	}

	counts := make([]string, 0)
	for eventType, count := range &s.eventTypes {
		if count > 0 {
			counts = append(counts, fmt.Sprintf("%s=%d", replication.EventType(eventType), count))
		}
	}

	return fmt.Sprintf("events=%d, gtid_count=%d, first_gtid=%q, last_gtid=%q, first_event_ts=%s, last_event_ts=%s, event_types={%s}",
		s.events, s.gtidCount, s.firstGTID, s.lastGTID, firstTS, lastTS, strings.Join(counts, ","))
}
