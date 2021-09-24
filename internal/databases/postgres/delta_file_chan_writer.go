package postgres

import (
	"sync"

	"github.com/wal-g/wal-g/internal/walparser"
)

type DeltaFileChanWriter struct {
	DeltaFile             *DeltaFile
	BlockLocationConsumer chan walparser.BlockLocation
}

func NewDeltaFileChanWriter(deltaFile *DeltaFile) *DeltaFileChanWriter {
	blockLocationConsumer := make(chan walparser.BlockLocation)
	return &DeltaFileChanWriter{deltaFile, blockLocationConsumer}
}

func (writer *DeltaFileChanWriter) Consume(waitGroup *sync.WaitGroup) {
	for blockLocation := range writer.BlockLocationConsumer {
		writer.DeltaFile.Locations = append(writer.DeltaFile.Locations, blockLocation)
	}
	waitGroup.Done()
}

func (writer *DeltaFileChanWriter) close() {
	close(writer.BlockLocationConsumer)
}
