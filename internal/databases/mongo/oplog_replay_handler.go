package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
)

func HandleOplogReplay(ctx context.Context,
	since,
	until models.Timestamp,
	fetcher stages.BetweenFetcher,
	applier stages.Applier) error {
	return binary.HandleOplogReplay(ctx, since, until, fetcher, applier)
}

func RunOplogReplay(ctx context.Context, mongodbURL string, replayArgs binary.ReplyOplogConfig) error {
	return binary.RunOplogReplay(ctx, mongodbURL, replayArgs)
}
