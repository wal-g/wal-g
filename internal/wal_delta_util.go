package internal

import (
	"strings"
)

const (
	WalFileInDelta      uint64 = 16
	DeltaFilenameSuffix string = "_delta"
	PartFilenameSuffix  string = "_part"
)

func ToPartFilename(deltaFilename string) string {
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

func GetPositionInDelta(walFilename string) int {
	_, logSegNo, _ := ParseWALFilename(walFilename)
	return int(logSegNo % WalFileInDelta)
}
