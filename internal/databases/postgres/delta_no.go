package postgres

type DeltaNo uint64

func newDeltaNoFromWalSegmentNo(walSegmentNo WalSegmentNo) DeltaNo {
	no := uint64(walSegmentNo) - (uint64(walSegmentNo) % WalFileInDelta)
	return DeltaNo(no)
}

func newDeltaNoFromLsn(lsn LSN) DeltaNo {
	return newDeltaNoFromWalSegmentNo(NewWalSegmentNo(lsn))
}

func (deltaNo DeltaNo) next() DeltaNo {
	return deltaNo.add(1)
}

func (deltaNo DeltaNo) previous() DeltaNo {
	return deltaNo.sub(1)
}

func (deltaNo DeltaNo) add(n uint64) DeltaNo {
	deltaNo = DeltaNo(uint64(deltaNo) + n*WalFileInDelta)
	return deltaNo
}

func (deltaNo DeltaNo) sub(n uint64) DeltaNo {
	return DeltaNo(uint64(deltaNo) - n*WalFileInDelta)
}

func (deltaNo DeltaNo) firstWalSegmentNo() WalSegmentNo {
	return WalSegmentNo(deltaNo)
}

func (deltaNo DeltaNo) getFilename(timeline uint32) string {
	return deltaNo.firstWalSegmentNo().GetFilename(timeline) + DeltaFilenameSuffix
}
