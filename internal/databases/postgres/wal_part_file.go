package postgres

import (
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/walparser"
)

type WalPartFile struct {
	WalTails        [][]byte
	PreviousWalHead []byte
	WalHeads        [][]byte
}

func NewWalPartFile() *WalPartFile {
	return &WalPartFile{
		make([][]byte, WalFileInDelta),
		nil,
		make([][]byte, WalFileInDelta),
	}
}

func (partFile *WalPartFile) IsComplete() bool {
	for _, walTail := range partFile.WalTails {
		if walTail == nil {
			return false
		}
	}
	if partFile.PreviousWalHead == nil {
		return false
	}
	for _, walHead := range partFile.WalHeads {
		if walHead == nil {
			return false
		}
	}
	return true
}

func (partFile *WalPartFile) Save(writer io.Writer) error {
	walParts := make([]WalPart, 0)
	for id, data := range partFile.WalTails {
		if data != nil {
			walParts = append(walParts, *NewWalPart(WalTailType, uint8(id), data))
		}
	}
	if partFile.PreviousWalHead != nil {
		walParts = append(walParts, *NewWalPart(PreviousWalHeadType, 0, partFile.PreviousWalHead))
	}
	for id, data := range partFile.WalHeads {
		if data != nil {
			walParts = append(walParts, *NewWalPart(WalHeadType, uint8(id), data))
		}
	}
	return saveWalParts(walParts, writer)
}

func (partFile *WalPartFile) getCurrentDeltaFileRecordHeads() [][]byte {
	recordHeads := make([][]byte, WalFileInDelta)
	recordHeads[0] = partFile.PreviousWalHead
	for id := 1; id < int(WalFileInDelta); id++ {
		recordHeads[id] = partFile.WalHeads[id-1]
	}
	return recordHeads
}

func (partFile *WalPartFile) CombineRecords() ([]walparser.XLogRecord, error) {
	recordHeads := partFile.getCurrentDeltaFileRecordHeads()
	records := make([]walparser.XLogRecord, 0)
	for id := 0; id < int(WalFileInDelta); id++ {
		recordData := make([]byte, 0)
		recordData = append(recordData, recordHeads[id]...)
		recordData = append(recordData, partFile.WalTails[id]...)
		if len(recordData) == 0 {
			continue
		}
		record, err := walparser.ParseXLogRecordFromBytes(recordData)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	return records, nil
}

func (partFile *WalPartFile) setPart(part WalPart) {
	switch part.dataType {
	case PreviousWalHeadType:
		partFile.PreviousWalHead = part.data
	case WalTailType:
		partFile.WalTails[part.id] = part.data
	case WalHeadType:
		partFile.WalHeads[part.id] = part.data
	}
}

// IsPartiallyFilledPartFile checks if the current wal part file is the partially filled wal part file.
// It returns three values:
//   - bool: true if it is the partially filled wal part file, false otherwise.
//   - int: if it is the partially filled wal part file, this is the index of the first non-nil wal head/tail.
//     if it is not the partially filled wal part file, this value is -1.
//   - error: if there is an inconsistency between wal heads and wal tails, an error is returned.
//     nil otherwise.
func (partFile *WalPartFile) IsPartiallyFilledPartFile() (bool, int, error) {
	// PreviousWalHead is only set for the next wal part file when processing the last wal file in
	// the current wal part file. Therefore, if PreviousWalHead is not nil, it means the current
	// wal part file is not the partially filled wal part file. See WalPartRecorder.SaveNextWalHead()
	// for more details.
	if partFile.PreviousWalHead != nil {
		return false, -1, nil
	}

	for i := 0; i < len(partFile.WalHeads); i++ {
		walHeadNotNil := partFile.WalHeads[i] != nil
		walTailNotNil := partFile.WalTails[i] != nil

		// If both WalHead and WalTail are not nil, it means we found the first non-nil index
		if walHeadNotNil && walTailNotNil {
			return true, i, nil
		}

		// If the states of WalHead and WalTail are inconsistent (one is nil, the other is not),
		// return false and an error indicating the inconsistency
		if walHeadNotNil != walTailNotNil {
			return false, -1, errors.New("inconsistent state between wal heads and wal tails")
		}
	}

	// If no non-nil index is found after iterating through the entire loop, return false and -1
	return false, -1, nil
}

// CompletePartFile completes the partially filled part file by setting
// PreviousWalHead to an empty slice and WalTails and WalHeads up to the given
// index to empty slices. This is used when the current part file is determined
// to be the partially filled part file.
func (partFile *WalPartFile) CompletePartFile(index int) {
	partFile.PreviousWalHead = make([]byte, 0)
	for i := 0; i < index; i++ {
		partFile.WalTails[i] = make([]byte, 0)
		partFile.WalHeads[i] = make([]byte, 0)
	}
}

func LoadPartFile(reader io.Reader) (*WalPartFile, error) {
	partFile := NewWalPartFile()
	for {
		walPart, err := LoadWalPart(reader)
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return partFile, nil
			}
			return nil, err
		}
		partFile.setPart(*walPart)
	}
}
