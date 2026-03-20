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
// MariaDB GTIDs behave as monotonic high-water marks per (domain, server), not as
// interval sets like MySQL. The sequence number for each domain only increases.
//
// This filter uses GTID checkpoints (end-state markers) to determine whether a binlog
// has already been archived, avoiding unnecessary uploads.
type mariadbGtidFilter struct {
	BinlogsFolder string
	Flavor        string
	gtidArchived  *mysql.MariadbGTIDSet // GTID checkpoint of archived binlogs
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

// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
//
// Algorithm:
// 1. Extract GTIDs from the NEXT binlog (MARIADB_GTID_LIST_EVENT)
// 2. This represents the end-state checkpoint of the CURRENT binlog
// 3. If archived checkpoint already contains this end-state → skip (already uploaded)
// 4. Otherwise → upload and update archived checkpoint
//
// Why check the NEXT binlog?
//   - Each binlog starts with MARIADB_GTID_LIST_EVENT containing all GTIDs
//     executed BEFORE this binlog started
//   - The "next" binlog's GTID list = end state of "current" binlog
//
// MariaDB GTID semantics:
//   - GTID = (domain, server, sequence) is a monotonic high-water mark
//   - Sequence numbers are continuous per domain (no representable gaps)
//   - Multiple domains can exist in parallel replication
//   - Comparing checkpoints (Contain) is sufficient for deduplication
func (f *mariadbGtidFilter) shouldUpload(binlog, nextBinlog string) bool {
	if nextBinlog == "" {
		// No next binlog means we can't determine the end-state of current binlog
		// Skip to avoid incomplete checkpoint (typically the last active binlog)
		tracelog.DebugLogger.Printf("No next binlog to extract end-state checkpoint. Skip %s for now. (mariadb gtid check)\n", binlog)
		return false
	}

	// Get end-state checkpoint: GTIDs at the end of current binlog
	// Read from MARIADB_GTID_LIST_EVENT in the next binlog
	endStateCheckpoint, err := GetBinlogPreviousGTIDs(path.Join(f.BinlogsFolder, nextBinlog), f.Flavor)
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Cannot extract end-state checkpoint from next binlog %s (caused by %v). Upload %s to be safe. (mariadb gtid check)\n",
			nextBinlog, err, binlog)
		return true
	}
	nextPreviousGTIDs := endStateCheckpoint.(*mysql.MariadbGTIDSet)

	// First run - no archived checkpoint yet
	if f.gtidArchived == nil || f.gtidArchived.String() == "" {
		tracelog.DebugLogger.Printf("First binlog - initializing archived checkpoint. (mariadb gtid check)\n")
		f.gtidArchived = nextPreviousGTIDs.Clone().(*mysql.MariadbGTIDSet)
		return true
	}

	// Check if archived checkpoint already covers this binlog's end-state
	// If gtidArchived.Contain(endState) → we already uploaded a binlog that reached this state
	if f.gtidArchived.Contain(nextPreviousGTIDs) {
		tracelog.InfoLogger.Printf("Binlog %s end-state %s already covered by archived checkpoint %s. Skip. (mariadb gtid check)\n",
			binlog, nextPreviousGTIDs.String(), f.gtidArchived.String())
		return false
	}

	// New checkpoint - update archived state
	// Merge the new checkpoint into our archived set
	err = f.gtidArchived.Update(nextPreviousGTIDs.String())
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot update archived checkpoint with %s: %v. Upload %s to be safe. (mariadb gtid check)\n",
			nextPreviousGTIDs.String(), err, binlog)
		return true
	}

	tracelog.InfoLogger.Printf("Should upload binlog %s with end-state checkpoint: %s (mariadb gtid check)\n",
		binlog, nextPreviousGTIDs.String())
	return true
}

// getArchivedGTIDString returns the string representation of the archived GTID checkpoint for MariaDB
func (f *mariadbGtidFilter) getArchivedGTIDString() string {
	if f.gtidArchived == nil {
		return ""
	}
	return f.gtidArchived.String()
}
