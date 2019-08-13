package internal

import (
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
)

var TerminalLocation = *walparser.NewBlockLocation(0, 0, 0, 0)

type WalDeltaRecorder struct {
	blockLocationConsumer chan walparser.BlockLocation
}

func NewWalDeltaRecorder(blockLocationConsumer *chan walparser.BlockLocation) *WalDeltaRecorder {
	tracelog.InfoLogger.Println(blockLocationConsumer)
	tracelog.InfoLogger.Println("blockLocationConsumer address")
	return &WalDeltaRecorder{*blockLocationConsumer}
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) {
	locations := ExtractBlockLocations(records)
	for _, location := range locations {
		tracelog.InfoLogger.Printf("DBNode: %d, RelNode: %d, SpcNode: %d, BlockNo: %d",
			location.RelationFileNode.DBNode, location.RelationFileNode.RelNode,
			location.RelationFileNode.SpcNode, location.BlockNo)
		recorder.blockLocationConsumer <- location
	}
}
