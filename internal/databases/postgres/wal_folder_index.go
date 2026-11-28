package postgres

import (
	"context"
	"slices"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// walFolderIndex is a compact view of the WAL segments present in storage.
// It keeps 8 bytes per segment instead of the object listing, the filenames
// and a hash set, so wal-verify can handle WAL folders with millions of files.
type walFolderIndex struct {
	// segments holds sorted and deduplicated segment numbers per timeline
	segments map[uint32][]WalSegmentNo
	// highestTimeline is the highest timeline found in WAL and .history filenames
	highestTimeline uint32
}

// buildWalFolderIndex lists the WAL folder once, streaming objects page by page
// when the storage supports it, and keeps only parsed segment numbers.
// Like ListFolder, it ignores objects in nested folders.
func buildWalFolderIndex(ctx context.Context, walFolder storage.Folder) (*walFolderIndex, error) {
	builder := newWalFolderIndexBuilder()
	err := storage.WalkObjects(ctx, walFolder, func(object storage.Object) error {
		name := strings.TrimLeft(object.GetName(), "/")
		if strings.Contains(name, "/") {
			return nil
		}
		builder.add(name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return builder.build(), nil
}

type walFolderIndexBuilder struct {
	index *walFolderIndex
	// sorted is false for timelines whose segments were not listed in order
	sorted map[uint32]bool
}

func newWalFolderIndexBuilder() *walFolderIndexBuilder {
	return &walFolderIndexBuilder{
		index:  &walFolderIndex{segments: make(map[uint32][]WalSegmentNo)},
		sorted: make(map[uint32]bool),
	}
}

func (b *walFolderIndexBuilder) add(filename string) {
	if timeline, ok := tryParseTimelineID(filename); ok {
		b.index.highestTimeline = max(b.index.highestTimeline, timeline)
	} else {
		tracelog.WarningLogger.Printf("Could not parse the timeline Id from %s. Skipping...", filename)
	}

	segment, err := NewWalSegmentDescription(utility.TrimFileExtension(filename))
	if err != nil {
		// non-wal segment file, skip it
		return
	}
	numbers, exists := b.index.segments[segment.Timeline]
	if !exists {
		b.sorted[segment.Timeline] = true
	}
	if len(numbers) > 0 {
		last := numbers[len(numbers)-1]
		if last == segment.Number {
			// the same segment stored with another extension
			return
		}
		if last > segment.Number {
			b.sorted[segment.Timeline] = false
		}
	}
	b.index.segments[segment.Timeline] = append(numbers, segment.Number)
}

func (b *walFolderIndexBuilder) build() *walFolderIndex {
	for timeline, numbers := range b.index.segments {
		if !b.sorted[timeline] {
			slices.Sort(numbers)
			numbers = slices.Compact(numbers)
		}
		b.index.segments[timeline] = numbers
	}
	return b.index
}

// Contains reports whether the segment exists in storage
func (idx *walFolderIndex) Contains(segment WalSegmentDescription) bool {
	_, found := slices.BinarySearch(idx.segments[segment.Timeline], segment.Number)
	return found
}
