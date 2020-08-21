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
	CorruptBlocks []uint32 `json:",omitempty"`
}

func NewBackupFileDescription(isIncremented, isSkipped bool, modTime time.Time) *BackupFileDescription {
	return &BackupFileDescription{isIncremented, isSkipped, modTime, nil}
}

func (desc *BackupFileDescription) SetCorruptBlocks(corruptBlockNumbers []uint32) {
	sort.Slice(corruptBlockNumbers, func(i, j int) bool {
		return corruptBlockNumbers[i] < corruptBlockNumbers[j]
	})

	// write no more than MaxCorruptBlocksInFileDesc
	desc.CorruptBlocks = make([]uint32, 0)
	for idx, blockNo := range corruptBlockNumbers {
		if idx >= MaxCorruptBlocksInFileDesc {
			break
		}
		desc.CorruptBlocks = append(desc.CorruptBlocks, blockNo)
	}
}

type BackupFileList map[string]BackupFileDescription
