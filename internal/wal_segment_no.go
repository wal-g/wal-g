package internal

import (
	"fmt"
)

type WalSegmentNo uint64

func newWalSegmentNo(lsn uint64) WalSegmentNo {
	return WalSegmentNo(lsn / WalSegmentSize)
}

func NewWalSegmentNoFromFilename(filename string) (WalSegmentNo, error) {
	_, no, err := ParseWALFilename(filename)
	return WalSegmentNo(no), err
}

func NewWalSegmentNoFromFilenameNoError(filename string) WalSegmentNo {
	_, no, _ := ParseWALFilename(filename)
	return WalSegmentNo(no)
}

func (walSegmentNo WalSegmentNo) Next() WalSegmentNo {
	return walSegmentNo.Add(1)
}

func (walSegmentNo WalSegmentNo) Previous() WalSegmentNo {
	return walSegmentNo.Sub(1)
}

func (walSegmentNo WalSegmentNo) Add(n uint64) WalSegmentNo {
	return WalSegmentNo(uint64(walSegmentNo) + n)
}

func (walSegmentNo WalSegmentNo) Sub(n uint64) WalSegmentNo {
	return WalSegmentNo(uint64(walSegmentNo) - n)
}

func (walSegmentNo WalSegmentNo) FirstLsn() uint64 {
	return uint64(walSegmentNo) * WalSegmentSize
}

func (walSegmentNo WalSegmentNo) GetFilename(timeline uint32) string {
	return fmt.Sprintf(walFileFormat, timeline, uint64(walSegmentNo)/xLogSegmentsPerXLogId, uint64(walSegmentNo)%xLogSegmentsPerXLogId)
}
