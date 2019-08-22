package internal

import "strings"

type DeltaNo uint64

func NewDeltaNoFromWalSegmentNo(walSegmentNo WalSegmentNo) DeltaNo {
	no := uint64(walSegmentNo) - (uint64(walSegmentNo) % WalFileInDelta)
	return DeltaNo(no)
}

func NewDeltaNoFromLsn(lsn uint64) DeltaNo {
	return NewDeltaNoFromWalSegmentNo(NewWalSegmentNo(lsn))
}

func NewDeltaNoFromFilename(filename string) (DeltaNo, error) {
	filename = strings.TrimSuffix(filename, DeltaFilenameSuffix)
	_, no, err := ParseWALFilename(filename)
	return DeltaNo(no), err
}

func NewDeltaNoFromFilenameNoError(filename string) DeltaNo {
	filename = strings.TrimSuffix(filename, DeltaFilenameSuffix)
	_, no, _ := ParseWALFilename(filename)
	return DeltaNo(no)
}

func (deltaNo DeltaNo) Next() DeltaNo {
	return deltaNo.Add(1)
}

func (deltaNo DeltaNo) Previous() DeltaNo {
	return deltaNo.Sub(1)
}

func (deltaNo DeltaNo) Add(n uint64) DeltaNo {
	deltaNo = DeltaNo(uint64(deltaNo) + n*WalFileInDelta)
	return deltaNo
}

func (deltaNo DeltaNo) Sub(n uint64) DeltaNo {
	return DeltaNo(uint64(deltaNo) - n*WalFileInDelta)
}

func (deltaNo DeltaNo) FirstWalSegmentNo() WalSegmentNo {
	return WalSegmentNo(deltaNo)
}

func (deltaNo DeltaNo) FirstLsn() uint64 {
	return deltaNo.FirstWalSegmentNo().FirstLsn()
}

func (deltaNo DeltaNo) GetFilename(timeline uint32) string {
	return deltaNo.FirstWalSegmentNo().GetFilename(timeline) + DeltaFilenameSuffix
}
