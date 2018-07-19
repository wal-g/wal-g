package walg

import (
	"encoding/binary"
	"github.com/wal-g/wal-g/_vendor-20180626150014/github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"os"
	"path"
)

var DeltaFileExistanceError = errors.New("delta file doesn't exist")

type WalDeltaRecorder struct {
	deltaFile *os.File
}

func (recorder *WalDeltaRecorder) Close() error {
	return recorder.deltaFile.Close()
}

func NewWalDeltaRecorder() (*WalDeltaRecorder, error) {
	deltaFile, err := os.OpenFile(path.Join(PathToDataFolder, DeltaFilename), os.O_APPEND|os.O_WRONLY, 0600) // TODO : it may not exist and it is not an error
	if err != nil {
		if os.IsNotExist(err) {
			return nil, DeltaFileExistanceError
		}
		return nil, err
	}
	return &WalDeltaRecorder{deltaFile}, nil
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) error {
	for _, record := range records {
		for _, block := range record.Blocks {
			location := block.Header.BlockLocation
			numbersToWrite := []uint32{
				uint32(location.RelationFileNode.DBNode),
				uint32(location.RelationFileNode.RelNode),
				uint32(location.RelationFileNode.SpcNode),
				location.BlockNo,
			}
			for _, number := range numbersToWrite {
				err := binary.Write(recorder.deltaFile, binary.LittleEndian, number)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
