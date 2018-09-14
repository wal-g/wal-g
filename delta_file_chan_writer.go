package walg

import "github.com/wal-g/wal-g/walparser"

type DeltaFileChanWriter struct {
	deltaFile *DeltaFile
	blockLocationConsumer chan walparser.BlockLocation
}

func NewDeltaFileChanWriter(deltaFile *DeltaFile) *DeltaFileChanWriter {
	blockLocationConsumer := make(chan walparser.BlockLocation)
	return &DeltaFileChanWriter{deltaFile, blockLocationConsumer}
}

func (writer *DeltaFileChanWriter) consume() {
	for blockLocation := range writer.blockLocationConsumer {
		writer.deltaFile.locations = append(writer.deltaFile.locations, blockLocation)
	}
}

func (writer *DeltaFileChanWriter) close() {
	close(writer.blockLocationConsumer)
}
