package postgres

import (
	"github.com/wal-g/wal-g/internal/walparser"
)

type WalDeltaRecorder struct {
	blockLocationConsumer chan walparser.BlockLocation
}

func NewWalDeltaRecorder(blockLocationConsumer chan walparser.BlockLocation) *WalDeltaRecorder {
	return &WalDeltaRecorder{blockLocationConsumer}
}

func (recorder *WalDeltaRecorder) recordWalDelta(records []walparser.XLogRecord) {
	locations := walparser.ExtractBlockLocations(records)
	for _, location := range locations {
		recorder.blockLocationConsumer <- location
	}
}
