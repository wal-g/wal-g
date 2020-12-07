package internal

import "github.com/wal-g/wal-g/utility"

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

type ScannedSegmentDescription struct {
	WalSegmentDescription
	status ScannedSegmentStatus
}

func newScannedSegmentDescription(description WalSegmentDescription, status ScannedSegmentStatus) ScannedSegmentDescription {
	return ScannedSegmentDescription{description, status}
}

func (status ScannedSegmentStatus) String() string {
	return [...]string{"", "MISSING_LOST", "MISSING_UPLOADING", "MISSING_DELAYED", "FOUND"}[status]
}

// MarshalText marshals the ScannedSegmentStatus enum as a string
func (status ScannedSegmentStatus) MarshalText() ([]byte, error) {
	return utility.MarshalEnumToString(status)
}

// WalSegmentScanner is used to scan the WAL segments storage
type WalSegmentScanner struct {
	ScannedSegments  []ScannedSegmentDescription
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
		ScannedSegments:  make([]ScannedSegmentDescription, 0),
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
func (scanner *WalSegmentScanner) Scan(config SegmentScanConfig) error {
	// scan may have a limited number of iterations, or may be unlimited
	for i := 0; config.UnlimitedScan || i < config.ScanSegmentsLimit; i++ {
		currentSegment, err := scanner.walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				scanner.walSegmentRunner.ForceMoveNext()
				scanner.AddScannedSegment(scanner.walSegmentRunner.Current(), config.MissingSegmentStatus)
				continue
			case ReachedStopSegmentError:
				return nil
			default:
				return err
			}
		}
		scanner.AddScannedSegment(currentSegment, Found)
		if config.StopOnFirstFoundSegment {
			return nil
		}
	}
	return nil
}

// GetMissingSegmentsDescriptions returns a slice containing WalSegmentDescription of each missing segment
func (scanner *WalSegmentScanner) GetMissingSegmentsDescriptions() []WalSegmentDescription {
	result := make([]WalSegmentDescription, 0)
	for _, segment := range scanner.ScannedSegments {
		if segment.status != Found {
			result = append(result, segment.WalSegmentDescription)
		}
	}
	return result
}

func (scanner *WalSegmentScanner) AddScannedSegment(description WalSegmentDescription, status ScannedSegmentStatus) {
	scanner.ScannedSegments = append(scanner.ScannedSegments, newScannedSegmentDescription(description, status))
}
