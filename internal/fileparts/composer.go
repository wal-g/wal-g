package fileparts

import (
	"archive/tar"
	"context"
	"io"
	"strconv"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type ShouldSplitFunc func(*internal.ComposeFileInfo) bool

type TarBallComposerMaker struct {
	base        internal.TarBallComposerMaker
	partSize    int64
	shouldSplit ShouldSplitFunc
	fileParts   *Info
}

func NewTarBallComposerMaker(base internal.TarBallComposerMaker, partSize int64,
	shouldSplit ShouldSplitFunc,
) *TarBallComposerMaker {
	return &TarBallComposerMaker{base: base, partSize: partSize, shouldSplit: shouldSplit}
}

func (maker *TarBallComposerMaker) Enabled() bool {
	return maker.partSize > 0
}

func (maker *TarBallComposerMaker) FileParts() *Info {
	if maker.fileParts == nil || len(maker.fileParts.Files) == 0 {
		return nil
	}
	return maker.fileParts
}

func (maker *TarBallComposerMaker) Make(ctx context.Context, bundle *internal.Bundle) (internal.TarBallComposer, error) {
	if maker.base == nil {
		return nil, errors.New("base tar ball composer maker is required")
	}
	if maker.partSize < 0 {
		return nil, errors.New("file part size must not be negative")
	}
	composer, err := maker.base.Make(ctx, bundle)
	if err != nil {
		return nil, err
	}
	if !maker.Enabled() {
		maker.fileParts = nil
		return composer, nil
	}
	maker.fileParts = &Info{PartSize: maker.partSize, Files: make(map[string]int64)}
	return &TarBallComposer{
		TarBallComposer: composer,
		ctx:             ctx,
		bundle:          bundle,
		shouldSplit:     maker.shouldSplit,
		fileParts:       maker.fileParts,
	}, nil
}

type TarBallComposer struct {
	internal.TarBallComposer
	ctx         context.Context //nolint:containedctx // backup context outlives asynchronous archive uploads
	bundle      *internal.Bundle
	shouldSplit ShouldSplitFunc
	fileParts   *Info
	files       []*internal.ComposeFileInfo
}

func (composer *TarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	if info.Header.Size <= composer.fileParts.PartSize ||
		(composer.shouldSplit != nil && !composer.shouldSplit(info)) {
		composer.TarBallComposer.AddFile(info)
		return
	}
	composer.files = append(composer.files, info)
	composer.fileParts.Files[info.Header.Name] = info.Header.Size
}

func (composer *TarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	sets, err := composer.TarBallComposer.FinishComposing()
	if err != nil {
		return nil, err
	}
	concurrency, err := conf.GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	group, ctx := errgroup.WithContext(composer.ctx)
	group.SetLimit(concurrency)
	for _, info := range composer.files {
		group.Go(func() error {
			return composer.packFileParts(ctx, sets, info)
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return sets, nil
}

func (composer *TarBallComposer) packFileParts(ctx context.Context, sets internal.TarFileSets,
	info *internal.ComposeFileInfo,
) error {
	reader, err := internal.StartReadingFile(ctx, info.Header, info.FileInfo, info.Path)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(reader, "")
	composer.GetFiles().AddFile(info.Header, info.FileInfo, info.IsIncremented)

	for offset := int64(0); offset < info.Header.Size; {
		size := min(composer.fileParts.PartSize, info.Header.Size-offset)
		if err := composer.packFilePart(ctx, sets, info.Header, reader, offset, size); err != nil {
			return err
		}
		offset += size
	}
	return nil
}

func (composer *TarBallComposer) packFilePart(ctx context.Context, sets internal.TarFileSets,
	original *tar.Header, reader io.Reader, offset, size int64,
) error {
	ball, err := composer.bundle.TarBallQueue.Deque(ctx)
	if err != nil {
		return err
	}
	ball.SetUp(composer.ctx, composer.bundle.Crypter)
	header := *original
	header.Size = size
	header.Format = tar.FormatPAX
	header.PAXRecords = make(map[string]string, len(original.PAXRecords)+2)
	for key, value := range original.PAXRecords {
		header.PAXRecords[key] = value
	}
	header.PAXRecords[OffsetPAX] = strconv.FormatInt(offset, 10)
	header.PAXRecords[FileSizePAX] = strconv.FormatInt(original.Size, 10)
	sets.AddFile(ball.Name(), original.Name)
	packed, err := internal.PackFileTo(ball, &header, io.LimitReader(reader, size))
	if err != nil {
		return errors.Wrapf(err, "pack file %s part at offset %d", original.Name, offset)
	}
	if packed != size {
		return errors.Errorf("short file %s part at offset %d: got %d bytes, expected %d",
			original.Name, offset, packed, size)
	}
	return composer.bundle.TarBallQueue.FinishTarBall(ball)
}
