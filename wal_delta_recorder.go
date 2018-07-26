package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"log"
	"os"
	"path"
)

const WalFileInDelta uint64 = 16

type WalDeltaRecorder struct {
	deltaFile            *os.File
	recordingWalFilename string
	s3Prefix             *S3Folder
	uploader             *Uploader
}

func (recorder *WalDeltaRecorder) Close() error {
	nextWalFilename, _ := GetNextWALFileName(recorder.recordingWalFilename)
	nextDeltaFilename, _ := getDeltaFileNameFor(nextWalFilename)
	recorder.deltaFile.Close()
	if nextWalFilename == nextDeltaFilename {
		// this is the last record in delta file, unique it, and send to S3
		locations, err := readAllLocationsFromFile(recorder.deltaFile.Name())
		if err != nil {
			return err
		}
		locations = uniqueLocations(locations)
		return recorder.sendDeltaToS3(locations)
	}
	return nil
}

func NewWalDeltaRecorder(walFilename string, s3Prefix *S3Folder, uploader *Uploader) (*WalDeltaRecorder, error) {
	deltaFile, err := openDeltaFileFor(walFilename)
	if err != nil {
		return nil, err
	}
	return &WalDeltaRecorder{deltaFile, walFilename, s3Prefix, uploader}, nil
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) error {
	return WriteLocationsTo(recorder.deltaFile, extractBlockLocations(records))
}

// TODO : refactor
func (recorder *WalDeltaRecorder) StopRecording(err error) {
	recorder.CloseAfterErr()
	log.Printf("Can't write delta file because of error: %v", err)
}

func (recorder *WalDeltaRecorder) CloseAfterErr() {
	recorder.deltaFile.Close()
	os.Remove(recorder.deltaFile.Name())
}

func (recorder *WalDeltaRecorder) sendDeltaToS3(locations []walparser.BlockLocation) error {
	var buffer bytes.Buffer
	WriteLocationsTo(&buffer, locations)
	_, err := recorder.uploader.UploadWal(&NamedReaderImpl{&buffer, recorder.deltaFile.Name()}, recorder.s3Prefix, false)
	return err
}

func getDeltaFileNameFor(walFilename string) (string, error) {
	timeline, logSegNo, err := ParseWALFileName(walFilename)
	if err != nil {
		return "", err
	}
	deltaSegNo := logSegNo - (logSegNo % WalFileInDelta)
	return formatWALFileName(timeline, deltaSegNo), nil
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
