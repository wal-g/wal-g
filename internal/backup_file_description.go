package internal

import (
	"sort"
	"time"
)

const MaxCorruptBlocksInFileDesc int = 10

type BackupFileDescription struct {
	IsIncremented bool // should never be both incremented and Skipped
	IsSkipped     bool
	MTime         time.Time
	CorruptBlocks *CorruptBlocksInfo `json:",omitempty"`
}

func NewBackupFileDescription(isIncremented, isSkipped bool, modTime time.Time) *BackupFileDescription {
	return &BackupFileDescription{isIncremented, isSkipped, modTime, nil}
}

type CorruptBlocksInfo struct {
	CorruptBlocksCount int
	SomeCorruptBlocks  []uint32
}

func (desc *BackupFileDescription) SetCorruptBlocks(corruptBlockNumbers []uint32, storeAllBlocks bool) {
	if len(corruptBlockNumbers) == 0 {
		return
	}
	sort.Slice(corruptBlockNumbers, func(i, j int) bool {
		return corruptBlockNumbers[i] < corruptBlockNumbers[j]
	})

	corruptBlocksCount := len(corruptBlockNumbers)
	// write no more than MaxCorruptBlocksInFileDesc
	someCorruptBlocks := make([]uint32, 0)
	for idx, blockNo := range corruptBlockNumbers {
		if !storeAllBlocks && idx >= MaxCorruptBlocksInFileDesc {
			break
		}
		someCorruptBlocks = append(someCorruptBlocks, blockNo)
	}
	desc.CorruptBlocks = &CorruptBlocksInfo{
		CorruptBlocksCount: corruptBlocksCount,
		SomeCorruptBlocks:  someCorruptBlocks,
	}
}

type BackupFileList map[string]BackupFileDescription
