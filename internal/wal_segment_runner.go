package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type WalSegmentNotFoundError struct {
	error
}

func newWalSegmentNotFoundError(segmentFileName string) WalSegmentNotFoundError {
	return WalSegmentNotFoundError{
		errors.Errorf("Segment file '%s' does not exist in storage.\n", segmentFileName)}
}

func (err WalSegmentNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type ReachedStopSegmentError struct {
	error
}

func newReachedStopSegmentError() ReachedStopSegmentError {
	return ReachedStopSegmentError{errors.Errorf("Reached stop segment.\n")}
}

func (err ReachedStopSegmentError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalSegmentDescription struct {
	number   WalSegmentNo
	timeline uint32
}

func (desc *WalSegmentDescription) GetFileName() string {
	return desc.number.getFilename(desc.timeline)
}

// WalSegmentRunner is used for sequential iteration over WAL segments in the storage
type WalSegmentRunner struct {
	currentWalSegment WalSegmentDescription
	walFolderSegments map[WalSegmentDescription]bool
	stopSegmentNo     WalSegmentNo
}

func NewWalSegmentRunner(
	startWalSegment WalSegmentDescription,
	segments map[WalSegmentDescription]bool,
	stopSegmentNo WalSegmentNo,
) *WalSegmentRunner {
	return &WalSegmentRunner{startWalSegment,
		segments, stopSegmentNo}
}

// MoveNext tries to get the next segment from storage
func (r *WalSegmentRunner) MoveNext() (WalSegmentDescription, error) {
	if r.currentWalSegment.number <= r.stopSegmentNo {
		return WalSegmentDescription{}, newReachedStopSegmentError()
	}
	nextSegment := r.getNextSegment()
	var fileExists bool
	if _, fileExists = r.walFolderSegments[nextSegment]; !fileExists {
		return WalSegmentDescription{}, newWalSegmentNotFoundError(nextSegment.GetFileName())
	}
	r.currentWalSegment = nextSegment
	return r.currentWalSegment, nil
}

// ForceMoveNext do a force-switch to the next segment without accessing storage
func (r *WalSegmentRunner) ForceMoveNext() {
	nextSegment := r.getNextSegment()
	r.currentWalSegment = nextSegment
}

// getNextSegment calculates the next segment
func (r *WalSegmentRunner) getNextSegment() WalSegmentDescription {
	nextTimeline := r.currentWalSegment.timeline
	nextSegmentNo := r.currentWalSegment.number.previous()
	return WalSegmentDescription{timeline: nextTimeline, number: nextSegmentNo}
}

// getFolderFilenames returns a set of filenames in provided storage folder
func getFolderFilenames(folder storage.Folder) ([]string, error) {
	objects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	filenames := make([]string, 0, len(objects))
	for _, object := range objects {
		filenames = append(filenames, object.GetName())
	}
	return filenames, nil
}

func getSegmentsFromFiles(filenames []string) map[WalSegmentDescription]bool {
	walSegments := make(map[WalSegmentDescription]bool)
	for _, filename := range filenames {
		baseName := utility.TrimFileExtension(filename)
		timeline, segmentNo, err := ParseWALFilename(baseName)
		if _, ok := err.(NotWalFilenameError); ok {
			// non-wal segment file, skip it
			continue
		}
		segment := WalSegmentDescription{timeline: timeline, number: WalSegmentNo(segmentNo)}
		walSegments[segment] = true
	}
	return walSegments
}
