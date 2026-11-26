package postgres

import (
	"slices"

	"github.com/wal-g/wal-g/utility"
)

type ScannedSegmentStatus int

const (
	// Surely lost missing segment
	Lost ScannedSegmentStatus = iota + 1
	// Missing but probably still uploading segment
	ProbablyUploading
	// Missing but probably delayed segment
	ProbablyDelayed
	// Segment exists in storage
	Found
)

func (status ScannedSegmentStatus) String() string {
	return [...]string{"", "MISSING_LOST", "MISSING_UPLOADING", "MISSING_DELAYED", "FOUND"}[status]
}

// MarshalText marshals the ScannedSegmentStatus enum as a string
func (status ScannedSegmentStatus) MarshalText() ([]byte, error) {
	return utility.MarshalEnumToString(status)
}

// ScannedSegmentRun is a continuous run of scanned segments
// with the same timeline and status
type ScannedSegmentRun struct {
	Timeline uint32
	Status   ScannedSegmentStatus
	MinNo    WalSegmentNo
	MaxNo    WalSegmentNo
	Count    int
}

// WalSegmentScanner is used to scan the WAL segments storage.
// It aggregates scanned segments into runs, so its memory usage depends
// on the number of status/timeline changes, not on the number of segments.
type WalSegmentScanner struct {
	// runs are stored in scan order, i.e. in descending segment number order
	runs             []ScannedSegmentRun
	walSegmentRunner *WalSegmentRunner
}

// SegmentScanConfig is used to configure the single Scan() call of the WalSegmentScanner
type SegmentScanConfig struct {
	UnlimitedScan bool
	// ScanSegmentsLimit is used in case of UnlimitedScan is set to false
	ScanSegmentsLimit       int
	StopOnFirstFoundSegment bool
	// MissingSegmentStatus is set to all missing segments encountered during scan
	MissingSegmentStatus ScannedSegmentStatus
}

func NewWalSegmentScanner(walSegmentRunner *WalSegmentRunner) *WalSegmentScanner {
	return &WalSegmentScanner{
		walSegmentRunner: walSegmentRunner,
	}
}

// Scan traverse the WAL storage with WalSegmentRunner.
// Scan starts from the WalSegmentRunner's current position,
// so in case of subsequent Scan() call it will continue from the position
// where it stopped previously.
//
// Scan always stops if:
// - Stop segment is reached OR
// - Unknown error encountered
// Also, it may be configured to stop after:
// - Scanning the ScanSegmentsLimit of segments
// - Finding the first segment which exists in WAL storage
func (sc *WalSegmentScanner) Scan(config SegmentScanConfig) error {
	// scan may have a limited number of iterations, or may be unlimited
	for i := 0; config.UnlimitedScan || i < config.ScanSegmentsLimit; i++ {
		currentSegment, err := sc.walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				sc.walSegmentRunner.ForceMoveNext()
				sc.AddScannedSegment(sc.walSegmentRunner.Current(), config.MissingSegmentStatus)
				continue
			case ReachedStopSegmentError:
				return nil
			default:
				return err
			}
		}
		sc.AddScannedSegment(currentSegment, Found)
		if config.StopOnFirstFoundSegment {
			return nil
		}
	}
	return nil
}

// GetMissingSegmentsDescriptions returns a slice containing WalSegmentDescription of each missing segment
func (sc *WalSegmentScanner) GetMissingSegmentsDescriptions() []WalSegmentDescription {
	result := make([]WalSegmentDescription, 0)
	for _, run := range sc.runs {
		if run.Status == Found {
			continue
		}
		for no := run.MaxNo; ; no = no.previous() {
			result = append(result, WalSegmentDescription{Timeline: run.Timeline, Number: no})
			if no == run.MinNo {
				break
			}
		}
	}
	return result
}

// Runs returns the scanned segment runs ordered by ascending segment number
func (sc *WalSegmentScanner) Runs() []ScannedSegmentRun {
	result := slices.Clone(sc.runs)
	slices.Reverse(result)
	return result
}

// AddScannedSegment records a scanned segment. The WalSegmentRunner always moves
// to the previous segment number, so a segment extends the last run when its
// timeline and status match; otherwise it starts a new run.
func (sc *WalSegmentScanner) AddScannedSegment(description WalSegmentDescription, status ScannedSegmentStatus) {
	if n := len(sc.runs); n > 0 {
		last := &sc.runs[n-1]
		if last.Timeline == description.Timeline && last.Status == status &&
			last.MinNo > description.Number && last.MinNo.previous() == description.Number {
			last.MinNo = description.Number
			last.Count++
			return
		}
	}
	sc.runs = append(sc.runs, ScannedSegmentRun{
		Timeline: description.Timeline,
		Status:   status,
		MinNo:    description.Number,
		MaxNo:    description.Number,
		Count:    1,
	})
}
