package walg

import (
	"github.com/wal-g/wal-g/walparser"
	"sync"
)

type DeltaFileChanWriter struct {
	deltaFile             *DeltaFile
	blockLocationConsumer chan walparser.BlockLocation
}

func NewDeltaFileChanWriter(deltaFile *DeltaFile) *DeltaFileChanWriter {
	blockLocationConsumer := make(chan walparser.BlockLocation)
	return &DeltaFileChanWriter{deltaFile, blockLocationConsumer}
}

func (writer *DeltaFileChanWriter) consume(waitGroup *sync.WaitGroup) {
	for blockLocation := range writer.blockLocationConsumer {
		writer.deltaFile.locations = append(writer.deltaFile.locations, blockLocation)
	}
	waitGroup.Done()
}

func (writer *DeltaFileChanWriter) close() {
	close(writer.blockLocationConsumer)
}
