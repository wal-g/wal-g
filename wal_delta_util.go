package walg

import (
	"github.com/wal-g/wal-g/walparser"
	"os"
	"path"
	"strings"
)

const (
	WalFileInDelta      uint64 = 16
	DeltaFilenameSuffix        = "_delta"
	PartFilenameSuffix         = "_part"
)

func toPartFilename(deltaFilename string) string {
	return deltaFilename + PartFilenameSuffix
}

func toDeltaFilename(walFilename string) string {
	return walFilename + DeltaFilenameSuffix
}

func partFilenameToDelta(partFilename string) string {
	return strings.TrimSuffix(partFilename, "_part")
}

func GetDeltaFilenameFor(walFilename string) (string, error) {
	timeline, logSegNo, err := ParseWALFilename(walFilename)
	if err != nil {
		return "", err
	}
	deltaSegNo := logSegNo - (logSegNo % WalFileInDelta)
	return toDeltaFilename(formatWALFileName(timeline, deltaSegNo)), nil
}

// TODO : unit tests
func getPositionInDelta(walFilename string) int {
	_, logSegNo, _ := ParseWALFilename(walFilename)
	return int(logSegNo % uint64(WalFileInDelta))
}

func LoadWalParser(dataFolderPath string) (*walparser.WalParser, error) {
	pathToParser := path.Join(dataFolderPath, RecordPartFilename)
	parserFile, err := os.Open(pathToParser)
	if err != nil {
		if os.IsNotExist(err) {
			return walparser.NewWalParser(), nil
		}
		return nil, err
	}
	parser, err := walparser.LoadParser(parserFile)
	if err != nil {
		return nil, err
	}
	return parser, nil
}
