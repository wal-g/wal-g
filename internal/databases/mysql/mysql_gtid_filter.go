package mysql

import (
	"path"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
)

// mysqlGtidFilter handles MySQL-specific GTID filtering for binlog archiving.
// MySQL GTIDs are interval sets keyed by server UUID (e.g.
// "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"), so filtering has to reconcile
// intervals rather than compare high-water marks like mariadbGtidFilter does.
type mysqlGtidFilter struct {
	binlogsFolder string
	flavor        string
	gtidArchived  *mysql.MysqlGTIDSet
	lastGtidSeen  *mysql.MysqlGTIDSet
}

// isValid returns true if the filter is properly configured for MySQL.
// The filter is only valid if:
// 1. The receiver is non-nil
// 2. Flavor is set
// 3. Flavor is exactly MySQL (not MariaDB or other)
func (u *mysqlGtidFilter) isValid() bool {
	if u == nil {
		return false
	}
	if u.flavor == "" {
		return false
	}
	if u.flavor != mysql.MySQLFlavor {
		// MariaDB GTID Sets consists of: DomainID + ServerID + Sequence Number (64-bit unsigned integer)
		// It is not clear how it handles gaps in SequenceNumbers, so for safety reasons skip this check
		return false
	}
	return true
}

// shouldUpload determines if a binlog should be uploaded based on GTID interval-set comparison.
func (u *mysqlGtidFilter) shouldUpload(binlog, nextBinlog string) bool {
	if nextBinlog == "" {
		// it is better to skip this binlog rather than have gap in binlog sentinel GTID-set
		tracelog.DebugLogger.Printf("Cannot extract PREVIOUS_GTIDS event - no 'next' binlog found. Skip it for now. (gtid check)\n")
		return false
	}
	// nextPreviousGTIDs is 'GTIDs_executed at the end of current binary log file'
	_nextPreviousGTIDs, err := GetBinlogPreviousGTIDs(path.Join(u.binlogsFolder, nextBinlog), u.flavor)
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Cannot extract PREVIOUS_GTIDS event from current binlog %s, next %s (caused by %v). Upload it. (gtid check)\n",
			binlog, nextBinlog, err)
		return true
	}
	nextPreviousGTIDs := _nextPreviousGTIDs.(*mysql.MysqlGTIDSet)

	if u.gtidArchived == nil || u.gtidArchived.String() == "" {
		tracelog.DebugLogger.Printf("Cannot extract set of uploaded binlogs from cache\n")
		// continue uploading even when we cannot read uploadedGTIDs
		u.gtidArchived = nextPreviousGTIDs
		u.lastGtidSeen = nextPreviousGTIDs
		return true
	}

	if u.lastGtidSeen == nil {
		gtidSetBeforeCurrentBinlog, err := GetBinlogPreviousGTIDs(path.Join(u.binlogsFolder, binlog), u.flavor)
		if err != nil {
			tracelog.InfoLogger.Printf(
				"Cannot extract PREVIOUS_GTIDS event from current binlog %s, next %s (caused by %v). Upload it. (gtid check)\n",
				binlog, nextBinlog, err)
			u.lastGtidSeen = nextPreviousGTIDs
			return true
		}
		tracelog.DebugLogger.Printf("Binlog %s is the first binlog that we seen by GTID-checker in this run. (gtid check)\n", binlog)
		u.lastGtidSeen = gtidSetBeforeCurrentBinlog.(*mysql.MysqlGTIDSet)
	}

	currentBinlogGTIDSet := nextPreviousGTIDs.Clone().(*mysql.MysqlGTIDSet)
	gtidSetMinus(currentBinlogGTIDSet, u.lastGtidSeen)

	// when we know that _next_ binlog's PreviousGTID already uploaded we can safely skip _current_ binlog
	if u.gtidArchived.Contain(currentBinlogGTIDSet) {
		tracelog.InfoLogger.Printf("Binlog %v with GTID Set %s already archived (gtid check)\n", binlog, currentBinlogGTIDSet.String())
		u.lastGtidSeen = nextPreviousGTIDs
		return false
	}

	err = u.gtidArchived.Update(currentBinlogGTIDSet.String())
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot merge GTIDs: %v (gtid check)\n", err)
		return true // math is broken. upload binlog
	}
	tracelog.InfoLogger.Printf("Should upload binlog %s with GTID set: %s (gtid check)\n", binlog, currentBinlogGTIDSet.String())
	u.lastGtidSeen = nextPreviousGTIDs
	return true
}

// getArchivedGTIDString returns the string representation of the archived GTID set for MySQL.
func (u *mysqlGtidFilter) getArchivedGTIDString() string {
	if u.gtidArchived == nil {
		return ""
	}
	return u.gtidArchived.String()
}

// gtidSetMinus subtracts sub from s in place (set difference s\sub).
func gtidSetMinus(s, sub *mysql.MysqlGTIDSet) {
	for sid, subTags := range *sub {
		sTags, ok := (*s)[sid]
		if !ok {
			continue
		}
		for tag, subIntervals := range subTags {
			intervals, ok := sTags[tag]
			if !ok {
				continue
			}
			if diff := subtractIntervals(intervals, subIntervals); len(diff) > 0 {
				sTags[tag] = diff
			} else {
				delete(sTags, tag)
			}
		}
		if len(sTags) == 0 {
			delete(*s, sid)
		}
	}
}

// subtractIntervals returns a\b for half-open [Start, Stop) GTID interval slices
func subtractIntervals(a, b mysql.IntervalSlice) mysql.IntervalSlice {
	a = a.Normalize()
	b = b.Normalize()
	var result mysql.IntervalSlice
	for _, cur := range a {
		for _, sub := range b {
			if sub.Stop <= cur.Start || sub.Start >= cur.Stop {
				continue // disjoint
			}
			if sub.Start > cur.Start {
				result = append(result, mysql.Interval{Start: cur.Start, Stop: sub.Start})
			}
			cur.Start = sub.Stop
			if cur.Start >= cur.Stop {
				break // fully consumed by b
			}
		}
		if cur.Start < cur.Stop {
			result = append(result, cur)
		}
	}
	return result
}
