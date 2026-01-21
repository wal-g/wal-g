package mysql

import (
	"path"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
)

// mariadbGtidFilter handles MariaDB-specific GTID filtering for binlog archiving.
// MariaDB uses a different GTID format than MySQL: domain-server-sequence (e.g., "0-1-1011")
// instead of MySQL's uuid:interval format (e.g., "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5").
//
// This filter tracks which GTIDs have been archived and determines whether a binlog
// should be uploaded based on its GTID content.
type mariadbGtidFilter struct {
	BinlogsFolder string
	Flavor        string
	gtidArchived  *mysql.MariadbGTIDSet // GTIDs that have been successfully archived
	lastGtidSeen  *mysql.MariadbGTIDSet // GTIDs seen in the last processed binlog
}

// isValid returns true if the filter is properly configured for MariaDB.
// The filter is only valid if:
// 1. Flavor is set
// 2. Flavor is exactly MariaDB (not MySQL or other)
func (f *mariadbGtidFilter) isValid() bool {
	if f.Flavor == "" {
		return false
	}
	// Only valid for MariaDB flavor
	if f.Flavor != mysql.MariaDBFlavor {
		return false
	}
	return true
}

// shouldUpload determines if a binlog should be uploaded based on GTID comparison.
//
// Algorithm:
// 1. Extract GTIDs from the NEXT binlog (which represent the end state of the CURRENT binlog)
// 2. Compare these GTIDs with what we've already archived
// 3. Return true if the current binlog contains new transactions
//
// Why do we check the NEXT binlog?
//   - Each binlog file starts with a MARIADB_GTID_LIST_EVENT that contains all GTIDs
//     executed BEFORE this binlog started
//   - So the "next" binlog's GTID list represents the end state of the "current" binlog
//
// MariaDB GTID format: domain-server-sequence
// Example: "0-1-1011" means:
//   - Domain 0 (default replication domain)
//   - Server ID 1
//   - Sequence number 1011 (monotonically increasing)
//
// Important considerations for MariaDB:
// - GTIDs can have gaps in sequence numbers (unlike MySQL)
// - Multiple domains can exist in parallel replication scenarios
// - We need to track sequences per domain-server pair
func (f *mariadbGtidFilter) shouldUpload(binlog, nextBinlog string) bool {
	if nextBinlog == "" {
		// It is better to skip this binlog rather than have a gap in binlog sentinel GTID-set
		// This typically happens for the last (active) binlog that hasn't been rotated yet
		tracelog.DebugLogger.Printf("Cannot extract MARIADB_GTID_LIST_EVENT - no 'next' binlog found. Skip it for now. (mariadb gtid check)\n")
		return false
	}

	// nextPreviousGTIDs is 'GTIDs_executed at the end of current binary log file'
	// For MariaDB, this is read from the MARIADB_GTID_LIST_EVENT in the next binlog
	_nextPreviousGTIDs, err := GetBinlogPreviousGTIDs(path.Join(f.BinlogsFolder, nextBinlog), f.Flavor)
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Cannot extract MARIADB_GTID_LIST_EVENT from current binlog %s, next %s (caused by %v). Upload it. (mariadb gtid check)\n",
			binlog, nextBinlog, err)
		return true
	}
	nextPreviousGTIDs := _nextPreviousGTIDs.(*mysql.MariadbGTIDSet)

	// First run - no archived GTIDs yet
	if f.gtidArchived == nil || f.gtidArchived.String() == "" {
		tracelog.DebugLogger.Printf("Cannot extract set of uploaded binlogs from cache. First binlog for MariaDB. (mariadb gtid check)\n")
		// Continue uploading even when we cannot read archived GTIDs
		f.gtidArchived = nextPreviousGTIDs
		f.lastGtidSeen = nextPreviousGTIDs
		return true
	}

	// Initialize lastGtidSeen if this is the first binlog we're checking in this run
	if f.lastGtidSeen == nil {
		gtidSetBeforeCurrentBinlog, err := GetBinlogPreviousGTIDs(path.Join(f.BinlogsFolder, binlog), f.Flavor)
		if err != nil {
			tracelog.InfoLogger.Printf(
				"Cannot extract MARIADB_GTID_LIST_EVENT from current binlog %s, next %s (caused by %v). Upload it. (mariadb gtid check)\n",
				binlog, nextBinlog, err)
			f.lastGtidSeen = nextPreviousGTIDs
			return true
		}
		tracelog.DebugLogger.Printf("Binlog %s is the first binlog that we seen by GTID-checker in this run. (mariadb gtid check)\n", binlog)
		f.lastGtidSeen = gtidSetBeforeCurrentBinlog.(*mysql.MariadbGTIDSet)
	}

	// Calculate GTIDs that are in the current binlog
	// This is: (GTIDs at end of current binlog) - (GTIDs at start of current binlog)
	// For MariaDB: we need to manually calculate the difference per domain
	currentBinlogGTIDSet := subtractMariadbGTIDSets(nextPreviousGTIDs, f.lastGtidSeen)

	// Check if we've already archived these GTIDs
	// When we know that the _next_ binlog's PreviousGTID is already uploaded,
	// we can safely skip the _current_ binlog
	if f.gtidArchived.Contain(currentBinlogGTIDSet) {
		tracelog.InfoLogger.Printf("Binlog %v with GTID Set %s already archived (mariadb gtid check)\n", binlog, currentBinlogGTIDSet.String())
		f.lastGtidSeen = nextPreviousGTIDs
		return false
	}

	// New GTIDs found - merge them into our archived set
	// Use Update() to add the new GTID set string
	err = f.gtidArchived.Update(currentBinlogGTIDSet.String())
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot merge MariaDB GTIDs: %v (mariadb gtid check)\n", err)
		return true // Math is broken, upload binlog to be safe
	}

	tracelog.InfoLogger.Printf("Should upload binlog %s with GTID set: %s (mariadb gtid check)\n", binlog, currentBinlogGTIDSet.String())
	f.lastGtidSeen = nextPreviousGTIDs
	return true
}

// getArchivedGTIDString returns the string representation of archived GTIDs for MariaDB
func (f *mariadbGtidFilter) getArchivedGTIDString() string {
	if f.gtidArchived == nil {
		return ""
	}
	return f.gtidArchived.String()
}

// subtractMariadbGTIDSets calculates the difference between two MariaDB GTID sets.
// It returns a new set containing GTIDs in 'minuend' that are not in 'subtrahend'.
//
// For MariaDB GTIDs, we work per domain:
//   - If a domain exists in minuend but not in subtrahend, include it entirely
//   - If a domain exists in both, we can't simply subtract (MariaDB doesn't track intervals)
//     So we check if the sequence numbers are different
//
// Note: This is a simplified subtraction for the binlog-push use case.
// A more sophisticated implementation would track actual transaction intervals.
func subtractMariadbGTIDSets(minuend, subtrahend *mysql.MariadbGTIDSet) *mysql.MariadbGTIDSet {
	result := &mysql.MariadbGTIDSet{
		Sets: make(map[uint32]*mysql.MariadbGTID),
	}

	// Handle nil cases
	if minuend == nil {
		return result
	}
	if subtrahend == nil {
		// Return a clone of minuend if subtrahend is nil
		return minuend.Clone().(*mysql.MariadbGTIDSet)
	}

	// For each domain in minuend
	for domainID, gtid := range minuend.Sets {
		subGtid, existsInSub := subtrahend.Sets[domainID]

		if !existsInSub {
			// Domain only exists in minuend, include it
			result.Sets[domainID] = gtid.Clone()
		} else if gtid.SequenceNumber > subGtid.SequenceNumber {
			// Domain exists in both, but minuend has a higher sequence
			// Include the difference (we can't represent ranges, so we use the higher value)
			result.Sets[domainID] = gtid.Clone()
		}
		// If sequences are equal or minuend is lower, don't include (already subtracted)
	}

	return result
}
