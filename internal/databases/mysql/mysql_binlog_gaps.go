package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
)

// Only gaps with a known location count toward this limit.
const maxBinlogGapEntries = 10

// updateSentinelGaps detects gaps in the archive.
//
// Before uploading a binlog file, we check that the archive contains all transactions that
// were executed before that file started. If transactions are missing, there is a gap.
//
// Returns true when something changed in the sentinel.
func updateSentinelGaps(sentinel *BinlogSentinelDto, previousGTIDs, gtidPurged mysql.GTIDSet, maxGapArraySize int) (bool, error) {
	replayStopBinlog := sentinel.LastUploadedBinlog
	changed := false
	// On the first check we do not know where existing gaps occurred.
	if sentinel.GapTracking == nil {
		sentinel.GapTracking = &GapTrackingState{}
		replayStopBinlog = UnknownGapLocation
		changed = true
		tracelog.InfoLogger.Printf("Initialized MySQL binlog gap tracking")
	}

	gap, err := calculateGap(sentinel, previousGTIDs, gtidPurged)
	if err != nil {
		return false, err
	}

	if gap.String() != "" {
		if err := appendGap(sentinel.GapTracking, gap.String(), replayStopBinlog); err != nil {
			return false, err
		}
		changed = true
		tracelog.WarningLogger.Printf("Detected MySQL binlog gap after %q; missing GTIDs: %s", replayStopBinlog, gap.String())
	}

	if len(sentinel.GapTracking.Gaps) > maxGapArraySize {
		if err := compactGaps(sentinel.GapTracking); err != nil {
			return false, err
		}
		changed = true
		tracelog.WarningLogger.Printf("Binlog gap limit exceeded; merged the oldest gap into GapWithoutLocation")
	}
	return changed, nil
}

// calculateGap returns missing transactions that have not already been recorded as gaps.
func calculateGap(sentinel *BinlogSentinelDto, previousGTIDs, gtidPurged mysql.GTIDSet) (mysql.GTIDSet, error) {
	if previousGTIDs == nil {
		return nil, fmt.Errorf("cannot calculate binlog gaps without the first binlog's previous GTIDs")
	}
	if gtidPurged == nil {
		return nil, fmt.Errorf("cannot calculate binlog gaps without purged GTIDs")
	}

	// Read the actual archived GTIDs.
	archivedGTIDs, err := mysql.ParseMysqlGTIDSet(sentinel.GTIDArchived)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate binlog gaps: %w", err)
	}

	allGaps, err := allGapsSet(sentinel.GapTracking)
	if err != nil {
		return nil, err
	}

	// Calculate the expected archived GTIDs.
	result, err := mergeGTIDSets(previousGTIDs.String(), gtidPurged.String())
	if err != nil {
		return nil, err
	}

	// Calculate missing GTIDs.
	gtidSetMinus(result, archivedGTIDs.(*mysql.MysqlGTIDSet))
	// Exclude gaps that are already recorded.
	gtidSetMinus(result, allGaps.(*mysql.MysqlGTIDSet))

	return result, nil
}

// allGapsSet combines recorded gap GTIDs into one set.
func allGapsSet(gapTracking *GapTrackingState) (mysql.GTIDSet, error) {
	if gapTracking == nil {
		return mysql.ParseMysqlGTIDSet("")
	}
	existingGaps, err := mysql.ParseMysqlGTIDSet(gapTracking.GapWithoutLocation)
	if err != nil {
		return nil, err
	}
	for _, gap := range gapTracking.Gaps {
		if err := existingGaps.Update(gap.GTIDSet); err != nil {
			return nil, fmt.Errorf("failed to parse recorded binlog gap: %w", err)
		}
	}
	return existingGaps, nil
}

// appendGap merges GTIDs into the gap with the same binlog name, or adds a new gap.
func appendGap(gapTracking *GapTrackingState, gtidSet, replayStopBinlog string) error {
	if replayStopBinlog == UnknownGapLocation {
		mergedGTIDs, err := mergeGTIDSets(gapTracking.GapWithoutLocation, gtidSet)
		if err != nil {
			return err
		}
		gapTracking.GapWithoutLocation = mergedGTIDs.String()
		return nil
	}
	for i := range gapTracking.Gaps {
		existing := &gapTracking.Gaps[i]
		if existing.ReplayStopBinlog != replayStopBinlog {
			continue
		}
		mergedGTIDs, err := mergeGTIDSets(existing.GTIDSet, gtidSet)
		if err != nil {
			return err
		}
		existing.GTIDSet = mergedGTIDs.String()
		return nil
	}
	gapTracking.Gaps = append(gapTracking.Gaps, BinlogGap{
		GTIDSet:          gtidSet,
		ReplayStopBinlog: replayStopBinlog,
	})
	return nil
}

// compactGaps merges the oldest located gap into GapWithoutLocation.
func compactGaps(gapTracking *GapTrackingState) error {
	if len(gapTracking.Gaps) == 0 {
		return nil
	}

	mergedGTIDs, err := mergeGTIDSets(gapTracking.GapWithoutLocation, gapTracking.Gaps[0].GTIDSet)
	if err != nil {
		return err
	}
	gapTracking.GapWithoutLocation = mergedGTIDs.String()
	gapTracking.Gaps = gapTracking.Gaps[1:]
	return nil
}

// mergeGTIDSets returns the union of two MySQL GTID sets.
func mergeGTIDSets(first, second string) (*mysql.MysqlGTIDSet, error) {
	merged, err := mysql.ParseMysqlGTIDSet(first)
	if err != nil {
		return nil, fmt.Errorf("failed to merge GTID sets: %w", err)
	}
	if err := merged.Update(second); err != nil {
		return nil, fmt.Errorf("failed to merge GTID sets: %w", err)
	}
	return merged.(*mysql.MysqlGTIDSet), nil
}
