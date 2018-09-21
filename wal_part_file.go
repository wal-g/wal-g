package walg

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"io"
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
