package binary

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
)

type FilePartsTarBallComposerMaker struct {
	partSize int64
}

func NewFilePartsTarBallComposerMaker(partSize int64) *FilePartsTarBallComposerMaker {
	return &FilePartsTarBallComposerMaker{partSize: partSize}
}

func (maker *FilePartsTarBallComposerMaker) Make(ctx context.Context, bundle *internal.Bundle) (internal.TarBallComposer, error) {
	if maker.partSize <= 0 {
		return nil, errors.New("MongoDB file part size must be positive")
	}
	baseMaker := NewDirDatabaseTarBallComposerMaker()
	composer, err := baseMaker.Make(ctx, bundle)
	if err != nil {
		return nil, err
	}
	return newFilePartsTarBallComposer(ctx, bundle, composer, baseMaker.tarFileSets, maker.partSize), nil
}
