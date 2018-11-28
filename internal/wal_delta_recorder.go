package internal

import (
	"github.com/wal-g/wal-g/internal/walparser"
)

var TerminalLocation = *walparser.NewBlockLocation(0, 0, 0, 0)

type WalDeltaRecorder struct {
	blockLocationConsumer chan walparser.BlockLocation
}

func NewWalDeltaRecorder(blockLocationConsumer chan walparser.BlockLocation) *WalDeltaRecorder {
	return &WalDeltaRecorder{blockLocationConsumer}
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) {
	locations := ExtractBlockLocations(records)
	for _, location := range locations {
		recorder.blockLocationConsumer <- location
	}
}
