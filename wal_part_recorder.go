package walg

import (
	"bytes"
	"fmt"
	"github.com/wal-g/wal-g/walparser"
	"log"
)

type NotWalFilenameError struct {
	filename string
}

func (err NotWalFilenameError) Error() string {
	return fmt.Sprintf("expected to get wal filename, but found: '%s'", err.filename)
}

type WalPartRecorder struct {
	manager     *DeltaFileManager
	walFilename string
}

func NewWalPartRecorder(walFilename string, manager *DeltaFileManager) (*WalPartRecorder, error) {
	if !isWalFilename(walFilename) {
		return nil, NotWalFilenameError{walFilename}
	}
	return &WalPartRecorder{manager, walFilename}, nil
}

// TODO : unit tests
func (recorder *WalPartRecorder) savePreviousWalTail(tailData []byte) error {
	deltaFilename, err := GetDeltaFilenameFor(recorder.walFilename)
	if err != nil {
		return err
	}
	partFile, err := recorder.manager.getPartFile(deltaFilename)
	if err != nil {
		return err
	}
	partFile.walTails[getPositionInDelta(recorder.walFilename)] = tailData
	return nil
}

// TODO : unit tests
func (recorder *WalPartRecorder) saveNextWalHead(parser *walparser.WalParser) error {
	var parserData bytes.Buffer
	err := parser.SaveParser(&parserData)
	if err != nil {
		return err
	}
	deltaFilename, _ := GetDeltaFilenameFor(recorder.walFilename)
	partFile, err := recorder.manager.getPartFile(deltaFilename)
	if err != nil {
		return err
	}
	positionInDelta := getPositionInDelta(recorder.walFilename)
	partFile.walHeads[positionInDelta] = parserData.Bytes()
	if positionInDelta == int(WalFileInDelta)-1 {
		nextWalFilename, _ := GetNextWalFilename(recorder.walFilename)
		nextDeltaFilename, _ := GetDeltaFilenameFor(nextWalFilename)
		nextPartFile, err := recorder.manager.getPartFile(nextDeltaFilename)
		if err != nil {
			return err
		}
		nextPartFile.previousWalHead = parserData.Bytes()
	}
	return nil
}

func (recorder *WalPartRecorder) cancelRecordingWithErr(err error) {
	log.Printf("stopped wal file: '%s' recording because of error: '%v'", recorder.walFilename, err)
	recorder.manager.cancelRecording(recorder.walFilename)
}
