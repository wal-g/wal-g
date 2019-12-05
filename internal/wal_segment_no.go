package internal

import (
	"fmt"
)

type WalSegmentNo uint64

func newWalSegmentNo(lsn uint64) WalSegmentNo {
	return WalSegmentNo(lsn / WalSegmentSize)
}

func newWalSegmentNoFromFilename(filename string) (WalSegmentNo, error) {
	_, no, err := ParseWALFilename(filename)
	return WalSegmentNo(no), err
}

func newWalSegmentNoFromFilenameNoError(filename string) WalSegmentNo {
	_, no, _ := ParseWALFilename(filename)
	return WalSegmentNo(no)
}

func (walSegmentNo WalSegmentNo) next() WalSegmentNo {
	return walSegmentNo.add(1)
}

func (walSegmentNo WalSegmentNo) previous() WalSegmentNo {
	return walSegmentNo.sub(1)
}

func (walSegmentNo WalSegmentNo) add(n uint64) WalSegmentNo {
	return WalSegmentNo(uint64(walSegmentNo) + n)
}

func (walSegmentNo WalSegmentNo) sub(n uint64) WalSegmentNo {
	return WalSegmentNo(uint64(walSegmentNo) - n)
}

func (walSegmentNo WalSegmentNo) firstLsn() uint64 {
	return uint64(walSegmentNo) * WalSegmentSize
}

func (walSegmentNo WalSegmentNo) getFilename(timeline uint32) string {
	return fmt.Sprintf(walFileFormat, timeline, uint64(walSegmentNo)/xLogSegmentsPerXLogId, uint64(walSegmentNo)%xLogSegmentsPerXLogId)
}
