package postgres

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type referenceScannedSegment struct {
	WalSegmentDescription
	status ScannedSegmentStatus
}

// referenceIntegrityScan is the previous wal-verify implementation: it records every
// scanned segment and then sorts and groups them. It is kept as a test oracle for
// the run-based scanner and the compact WAL folder index.
func referenceIntegrityScan(runner *WalSegmentRunner, uploading, delayed int) []*IntegrityScanSegmentSequence {
	var scanned []referenceScannedSegment
	scan := func(config SegmentScanConfig) {
		for i := 0; config.UnlimitedScan || i < config.ScanSegmentsLimit; i++ {
			segment, err := runner.Next()
			switch err.(type) {
			case nil:
				scanned = append(scanned, referenceScannedSegment{segment, Found})
				if config.StopOnFirstFoundSegment {
					return
				}
			case WalSegmentNotFoundError:
				runner.ForceMoveNext()
				scanned = append(scanned, referenceScannedSegment{runner.Current(), config.MissingSegmentStatus})
			default:
				return
			}
		}
	}
	scan(SegmentScanConfig{ScanSegmentsLimit: delayed, StopOnFirstFoundSegment: true, MissingSegmentStatus: ProbablyDelayed})
	scan(SegmentScanConfig{ScanSegmentsLimit: uploading, MissingSegmentStatus: ProbablyUploading})
	scan(SegmentScanConfig{UnlimitedScan: true, MissingSegmentStatus: Lost})

	if len(scanned) == 0 {
		return nil
	}
	slices.SortFunc(scanned, func(a, b referenceScannedSegment) int { return cmp.Compare(a.Number, b.Number) })
	var result []*IntegrityScanSegmentSequence
	current := NewSegmentsSequence(scanned[0].Timeline, scanned[0].Number)
	status := scanned[0].status
	flush := func() {
		result = append(result, &IntegrityScanSegmentSequence{
			TimelineID:    current.TimelineID,
			StartSegment:  current.MinSegmentNo.GetFilename(current.TimelineID),
			EndSegment:    current.MaxSegmentNo.GetFilename(current.TimelineID),
			SegmentsCount: len(current.WalSegmentNumbers),
			Status:        status,
		})
	}
	for _, segment := range scanned[1:] {
		if segment.status != status || current.TimelineID != segment.Timeline {
			flush()
			current = NewSegmentsSequence(segment.Timeline, segment.Number)
			status = segment.status
		} else {
			current.AddWalSegmentNo(segment.Number)
		}
	}
	flush()
	return result
}

// TestWalVerifyIntegrityMatchesReference compares the compact index and run-based
// scan with the previous per-segment implementation on random WAL folders with
// gaps, timeline switches, foreign timeline segments, duplicated extensions and
// unordered listings.
func TestWalVerifyIntegrityMatchesReference(t *testing.T) {
	extensions := []string{"", ".br", ".lz4", ".zst"}
	for seed := int64(1); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			stopNo := WalSegmentNo(1 + rng.Intn(20))
			startNo := stopNo + WalSegmentNo(1+rng.Intn(400))
			currentTimeline := uint32(1 + rng.Intn(3))

			// timeline switch points, descending, one per previous timeline
			switchMap := make(map[WalSegmentNo]*TimelineHistoryRecord)
			timelineStart := map[uint32]WalSegmentNo{1: 0}
			nextSwitch := startNo
			for tl := currentTimeline; tl > 1; tl-- {
				nextSwitch = stopNo + WalSegmentNo(rng.Int63n(int64(nextSwitch-stopNo)+1))
				switchMap[nextSwitch] = NewTimelineHistoryRecord(tl-1, nextSwitch.firstLsn(), "")
				timelineStart[tl] = nextSwitch
			}

			var filenames []string
			addSegment := func(tl uint32, no WalSegmentNo) {
				name := no.GetFilename(tl)
				filenames = append(filenames, name+extensions[rng.Intn(len(extensions))])
				if rng.Intn(20) == 0 {
					// the same segment with another compression
					filenames = append(filenames, name+extensions[rng.Intn(len(extensions))])
				}
			}
			gapRate := []int{0, 2, 10, 50}[rng.Intn(4)]
			for no := WalSegmentNo(1); no <= startNo+3; no++ {
				tl := uint32(1)
				for candidate := currentTimeline; candidate > 1; candidate-- {
					if no >= timelineStart[candidate] {
						tl = candidate
						break
					}
				}
				if gapRate > 0 && rng.Intn(gapRate) == 0 {
					continue
				}
				addSegment(tl, no)
				if rng.Intn(40) == 0 {
					// a segment of an abandoned or older timeline
					addSegment(uint32(1+rng.Intn(int(currentTimeline)+1)), no)
				}
			}
			filenames = append(filenames,
				fmt.Sprintf("%08X.history.br", currentTimeline),
				"000000010000000000000002.00000028.backup",
				"000000010000000000000003.partial",
				"garbage",
				"00000007000000000000000K")
			rng.Shuffle(len(filenames), func(i, j int) { filenames[i], filenames[j] = filenames[j], filenames[i] })

			builder := newWalFolderIndexBuilder()
			for _, name := range filenames {
				builder.add(name)
			}
			index := builder.build()

			uploading, delayed := 1+rng.Intn(5), rng.Intn(5)
			start := WalSegmentDescription{Timeline: currentTimeline, Number: startNo}

			expected := referenceIntegrityScan(
				NewWalSegmentRunner(start, getSegmentsFromFiles(filenames), stopNo, switchMap), uploading, delayed)

			scanner := NewWalSegmentScanner(newWalSegmentRunnerWithSet(start, index, stopNo, switchMap))
			require.NoError(t, runWalIntegrityScan(scanner, uploading, delayed))
			actual := collapseSegmentsByStatusAndTimeline(scanner.Runs())

			assert.Equal(t, expected, actual)
		})
	}
}

func TestWalFolderIndex(t *testing.T) {
	builder := newWalFolderIndexBuilder()
	for _, name := range []string{
		"000000020000000000000005.br",
		"000000010000000000000003.lz4",
		"000000010000000000000001.br",
		"000000010000000000000003.br",
		"000000010000000000000002",
		"000000010000000000000002.00000028.backup",
		"00000003.history.br",
		"garbage",
	} {
		builder.add(name)
	}
	index := builder.build()

	assert.Equal(t, map[uint32][]WalSegmentNo{1: {1, 2, 3}, 2: {5}}, index.segments)
	assert.Equal(t, uint32(3), index.highestTimeline)
	assert.True(t, index.Contains(WalSegmentDescription{Timeline: 1, Number: 2}))
	assert.False(t, index.Contains(WalSegmentDescription{Timeline: 2, Number: 2}))
	assert.False(t, index.Contains(WalSegmentDescription{Timeline: 1, Number: 4}))
	assert.False(t, index.Contains(WalSegmentDescription{Timeline: 9, Number: 1}))
}

// nestedListingFolder lists objects with nested names, as a recursive walk does
type nestedListingFolder struct {
	storage.Folder
	names []string
}

func (f nestedListingFolder) WalkObjects(_ context.Context, visit func(storage.Object) error) error {
	for _, name := range f.names {
		if err := visit(storage.NewLocalObject(name, time.Time{}, 0)); err != nil {
			return err
		}
	}
	return nil
}

// TestBuildWalFolderIndexSkipsNestedObjects keeps the non-recursive ListFolder
// semantics: objects in nested folders are not WAL segments of this folder.
func TestBuildWalFolderIndexSkipsNestedObjects(t *testing.T) {
	folder := nestedListingFolder{names: []string{
		"000000010000000000000001.br",
		"/000000010000000000000002.br",
		"sub/000000010000000000000003.br",
		"sub/00000009.history.br",
	}}
	index, err := buildWalFolderIndex(t.Context(), folder)
	require.NoError(t, err)
	assert.Equal(t, map[uint32][]WalSegmentNo{1: {1, 2}}, index.segments)
	assert.Equal(t, uint32(1), index.highestTimeline)
}
