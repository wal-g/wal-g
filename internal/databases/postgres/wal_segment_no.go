package postgres

import (
	"fmt"
)

type WalSegmentNo uint64

func NewWalSegmentNo(lsn LSN) WalSegmentNo {
	return WalSegmentNo(GetSegmentNoFromLsn(lsn))
}

func GetSegmentNoFromLsn(lsn LSN) uint64 {
	return uint64(lsn) / WalSegmentSize
}

func (walSegmentNo WalSegmentNo) Next() WalSegmentNo {
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

func (walSegmentNo WalSegmentNo) GetFilename(timeline uint32) string {
	return fmt.Sprintf(walFileFormat,
		timeline, uint64(walSegmentNo)/xLogSegmentsPerXLogID,
		uint64(walSegmentNo)%xLogSegmentsPerXLogID)
}
