package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"log"
	"os"
	"path"
	"io"
)

const (
	WalFileInDelta      uint64 = 16
	DeltaFilenameSuffix        = "_delta"
)

type WalDeltaRecorder struct {
	DeltaFile            *os.File
	RecordingWalFilename string
	Uploader             *Uploader
}

func (recorder *WalDeltaRecorder) Close() error {
	nextWalFilename, _ := GetNextWALFileName(recorder.RecordingWalFilename)
	nextDeltaFilename, _ := GetDeltaFilenameFor(nextWalFilename)
	if toDeltaFilename(nextWalFilename) == nextDeltaFilename {
		// this is the last record in delta file, unique it, and send to S3
		_, err := recorder.DeltaFile.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		locations, err := ReadLocationsFrom(recorder.DeltaFile)
		if err != nil {
			return err
		}
		recorder.DeltaFile.Close()
		os.Remove(recorder.DeltaFile.Name())
		locations = uniqueLocations(locations)
		return recorder.SendDeltaToS3(locations)
	}
	recorder.DeltaFile.Close()
	return nil
}

func NewWalDeltaRecorder(dataFolderPath, walFilename string, uploader *Uploader) (*WalDeltaRecorder, error) {
	deltaFile, err := OpenDeltaFileFor(dataFolderPath, walFilename)
	if err != nil {
		return nil, err
	}
	return &WalDeltaRecorder{deltaFile, walFilename, uploader}, nil
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) error {
	return WriteLocationsTo(recorder.DeltaFile, extractBlockLocations(records))
}

func (recorder *WalDeltaRecorder) stopRecording(err error) {
	recorder.closeAfterErr()
	log.Printf("Can't write delta file because of error: %v", err)
}

func (recorder *WalDeltaRecorder) closeAfterErr() {
	recorder.DeltaFile.Close()
	os.Remove(recorder.DeltaFile.Name())
}

func (recorder *WalDeltaRecorder) SendDeltaToS3(locations []walparser.BlockLocation) error {
	var buffer bytes.Buffer
	WriteLocationsTo(&buffer, locations)
	deltaFilename := path.Base(recorder.DeltaFile.Name())
	_, err := recorder.Uploader.UploadWalFile(&NamedReaderImpl{&buffer, deltaFilename}, false)
	return err
}

func toDeltaFilename(walFilename string) string {
	return walFilename + DeltaFilenameSuffix
}

func GetDeltaFilenameFor(walFilename string) (string, error) {
	timeline, logSegNo, err := ParseWALFileName(walFilename)
	if err != nil {
		return "", err
	}
	deltaSegNo := logSegNo - (logSegNo % WalFileInDelta)
	return toDeltaFilename(formatWALFileName(timeline, deltaSegNo)), nil
}

func OpenDeltaFileFor(dataFolderPath, walFilename string) (*os.File, error) {
	deltaFilename, err := GetDeltaFilenameFor(walFilename)
	if err != nil {
		return nil, err
	}
	deltaFilePath := path.Join(dataFolderPath, deltaFilename)
	if deltaFilename == toDeltaFilename(walFilename) {
		// this is the first wal file in delta, so new delta file should be created
		return os.Create(deltaFilePath)
	}
	deltaFile, err := os.OpenFile(deltaFilePath, os.O_RDWR | os.O_APPEND, 0666)
	return deltaFile, err
}
