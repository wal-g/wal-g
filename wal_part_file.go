package walg

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"io"
)

type WalPartFile struct {
	walTails        [][]byte
	previousWalHead []byte
	walHeads        [][]byte
}

func NewWalPartFile() *WalPartFile {
	return &WalPartFile{
		make([][]byte, WalFileInDelta),
		nil,
		make([][]byte, WalFileInDelta),
	}
}

// TODO : unit tests
func (partFile *WalPartFile) isComplete() bool {
	for _, walTail := range partFile.walTails {
		if walTail == nil {
			return false
		}
	}
	if partFile.previousWalHead == nil {
		return false
	}
	for _, walHead := range partFile.walHeads {
		if walHead == nil {
			return false
		}
	}
	return true
}

//TODO : unit tests
func (partFile *WalPartFile) save(writer io.Writer) error {
	walParts := make([]WalPart, 0)
	for id, data := range partFile.walTails {
		if data != nil {
			walParts = append(walParts, *NewWalPart(WalTailType, uint8(id), data))
		}
	}
	if partFile.previousWalHead != nil {
		walParts = append(walParts, *NewWalPart(PreviousWalHeadType, 0, partFile.previousWalHead))
	}
	for id, data := range partFile.walHeads {
		walParts = append(walParts, *NewWalPart(WalHeadType, uint8(id), data))
	}
	return saveWalParts(walParts, writer)
}

func (partFile *WalPartFile) getCurrentDeltaFileRecordHeads() [][]byte {
	recordHeads := make([][]byte, WalFileInDelta)
	recordHeads[0] = partFile.previousWalHead
	for id := 1; id < int(WalFileInDelta); id++ {
		recordHeads[id] = partFile.walHeads[id-1]
	}
	return recordHeads
}

// TODO : unit tests
func (partFile *WalPartFile) combineRecords() ([]walparser.XLogRecord, error) {
	recordHeads := partFile.getCurrentDeltaFileRecordHeads()
	records := make([]walparser.XLogRecord, WalFileInDelta)
	for id := range records {
		recordData := make([]byte, 0)
		recordData = append(recordData, recordHeads[id]...)
		recordData = append(recordData, partFile.walTails[id]...)
		record, err := walparser.ParseXLogRecordFromBytes(recordData)
		if err != nil {
			return nil, err
		}
		records[id] = *record
	}
	return records, nil
}

// TODO : unit tests
func loadPartFile(reader io.Reader) (*WalPartFile, error) {
	partFile := NewWalPartFile()
	for {
		walPart, err := readWalPart(reader)
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return partFile, nil
			}
			return nil, err
		}
		switch walPart.dataType {
		case PreviousWalHeadType:
			partFile.previousWalHead = walPart.data
		case WalTailType:
			partFile.walTails[walPart.id] = walPart.data
		case WalHeadType:
			partFile.walHeads[walPart.id] = walPart.data
		}
	}
}
