package walg

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/tracelog"
)

type NotWalFilenameError struct {
	error
}

func NewNotWalFilenameError(filename string) NotWalFilenameError {
	return NotWalFilenameError{errors.Errorf("expected to get wal filename, but found: '%s'", filename)}
}

func (err NotWalFilenameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalPartRecorder struct {
	manager     *DeltaFileManager
	walFilename string
}

func NewWalPartRecorder(walFilename string, manager *DeltaFileManager) (*WalPartRecorder, error) {
	if !isWalFilename(walFilename) {
		return nil, NewNotWalFilenameError(walFilename)
	}
	return &WalPartRecorder{manager, walFilename}, nil
}

func (recorder *WalPartRecorder) SavePreviousWalTail(tailData []byte) error {
	if tailData == nil {
		tailData = make([]byte, 0)
	}
	deltaFilename, err := GetDeltaFilenameFor(recorder.walFilename)
	if err != nil {
		return err
	}
	partFile, err := recorder.manager.GetPartFile(deltaFilename)
	if err != nil {
		return err
	}
	partFile.WalTails[GetPositionInDelta(recorder.walFilename)] = tailData
	return nil
}

func (recorder *WalPartRecorder) SaveNextWalHead(head []byte) error {
	if head == nil {
		head = make([]byte, 0)
	}
	deltaFilename, _ := GetDeltaFilenameFor(recorder.walFilename)
	partFile, err := recorder.manager.GetPartFile(deltaFilename)
	if err != nil {
		return err
	}
	positionInDelta := GetPositionInDelta(recorder.walFilename)
	partFile.WalHeads[positionInDelta] = head
	if positionInDelta == int(WalFileInDelta)-1 {
		nextWalFilename, _ := GetNextWalFilename(recorder.walFilename)
		nextDeltaFilename, _ := GetDeltaFilenameFor(nextWalFilename)
		nextPartFile, err := recorder.manager.GetPartFile(nextDeltaFilename)
		if err != nil {
			return err
		}
		nextPartFile.PreviousWalHead = head
	}
	return nil
}

func (recorder *WalPartRecorder) cancelRecordingWithErr(err error) {
	tracelog.WarningLogger.Printf("Stopped wal file: '%s' recording because of error: '%v'\n", recorder.walFilename, err)
	recorder.manager.CancelRecording(recorder.walFilename)
}
