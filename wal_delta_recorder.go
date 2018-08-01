package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"log"
	"os"
	"path"
)

const (
	WalFileInDelta      uint64 = 16
	DeltaFilenameSuffix        = "_delta"
)

type WalDeltaRecorder struct {
	deltaFile            *os.File
	recordingWalFilename string
	uploader             *Uploader
}

func (recorder *WalDeltaRecorder) Close() error {
	defer recorder.deltaFile.Close()
	nextWalFilename, _ := GetNextWALFileName(recorder.recordingWalFilename)
	nextDeltaFilename, _ := getDeltaFileNameFor(nextWalFilename)
	if nextWalFilename == nextDeltaFilename {
		// this is the last record in delta file, unique it, and send to S3
		locationReader := BlockLocationReader{recorder.deltaFile}
		locations, err := locationReader.readAllLocations()
		if err != nil {
			return err
		}
		locations = uniqueLocations(locations)
		return recorder.sendDeltaToS3(locations)
	}
	return nil
}

func NewWalDeltaRecorder(walFilename string, uploader *Uploader) (*WalDeltaRecorder, error) {
	deltaFile, err := openDeltaFileFor(walFilename)
	if err != nil {
		return nil, err
	}
	return &WalDeltaRecorder{deltaFile, walFilename, uploader}, nil
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) error {
	return writeLocationsTo(recorder.deltaFile, extractBlockLocations(records))
}

func (recorder *WalDeltaRecorder) stopRecording(err error) {
	recorder.closeAfterErr()
	log.Printf("Can't write delta file because of error: %v", err)
}

func (recorder *WalDeltaRecorder) closeAfterErr() {
	recorder.deltaFile.Close()
	os.Remove(recorder.deltaFile.Name())
}

// TODO : implementation is bad, don't use UploadWal here
func (recorder *WalDeltaRecorder) sendDeltaToS3(locations []walparser.BlockLocation) error {
	var buffer bytes.Buffer
	writeLocationsTo(&buffer, locations)
	_, err := recorder.uploader.UploadWal(&NamedReaderImpl{&buffer, recorder.deltaFile.Name()}, false)
	return err
}

func getDeltaFileNameFor(walFilename string) (string, error) {
	timeline, logSegNo, err := parseWALFileName(walFilename)
	if err != nil {
		return "", err
	}
	deltaSegNo := logSegNo - (logSegNo % WalFileInDelta)
	return formatWALFileName(timeline, deltaSegNo) + DeltaFilenameSuffix, nil
}

func openDeltaFileFor(walFilename string) (*os.File, error) {
	deltaFileName, err := getDeltaFileNameFor(walFilename)
	if err != nil {
		return nil, err
	}
	deltaFilePath := path.Join(PathToDataFolder, deltaFileName)
	if deltaFileName == walFilename {
		// this is the first wal file in delta, so new delta file should be created
		return os.Create(deltaFilePath)
	}
	deltaFile, err := os.Open(deltaFilePath)
	return deltaFile, err
}
