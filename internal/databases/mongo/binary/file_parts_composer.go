package binary

import (
	"archive/tar"
	"context"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type filePartsTarBallComposer struct {
	internal.TarBallComposer
	ctx       context.Context //nolint:containedctx // backup context outlives asynchronous archive uploads
	bundle    *internal.Bundle
	sets      internal.TarFileSets
	fileParts *models.FilePartsInfo
	files     []*internal.ComposeFileInfo
}

func newFilePartsTarBallComposer(ctx context.Context, bundle *internal.Bundle,
	composer internal.TarBallComposer, sets internal.TarFileSets, partSize int64,
) *filePartsTarBallComposer {
	return &filePartsTarBallComposer{
		TarBallComposer: composer,
		ctx:             ctx,
		bundle:          bundle,
		sets:            sets,
		fileParts:       &models.FilePartsInfo{PartSize: partSize, Files: make(map[string]int64)},
	}
}

func (c *filePartsTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	name := path.Base(info.Header.Name)
	isWiredTigerFile := strings.HasSuffix(name, ".wt") &&
		(strings.HasPrefix(name, "collection-") || strings.HasPrefix(name, "index-"))
	if !isWiredTigerFile || info.Header.Size <= c.fileParts.PartSize {
		c.TarBallComposer.AddFile(info)
		return
	}
	c.files = append(c.files, info)
	c.fileParts.Files[info.Header.Name] = info.Header.Size
}

func (c *filePartsTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	if _, err := c.TarBallComposer.FinishComposing(); err != nil {
		return nil, err
	}
	concurrency, err := conf.GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	group, ctx := errgroup.WithContext(c.ctx)
	group.SetLimit(concurrency)
	for _, info := range c.files {
		group.Go(func() error {
			return c.packFileParts(ctx, info)
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return c.sets, nil
}

func (c *filePartsTarBallComposer) packFileParts(ctx context.Context, info *internal.ComposeFileInfo) error {
	reader, err := internal.StartReadingFile(ctx, info.Header, info.FileInfo, info.Path)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(reader, "")
	c.GetFiles().AddFile(info.Header, info.FileInfo, info.IsIncremented)

	for offset := int64(0); offset < info.Header.Size; {
		size := min(c.fileParts.PartSize, info.Header.Size-offset)
		if err := c.packFilePart(ctx, info.Header, reader, offset, size); err != nil {
			return err
		}
		offset += size
	}
	return nil
}

func (c *filePartsTarBallComposer) packFilePart(ctx context.Context, original *tar.Header,
	reader io.Reader, offset, size int64,
) error {
	ball, err := c.bundle.TarBallQueue.Deque(ctx)
	if err != nil {
		return err
	}
	ball.SetUp(c.ctx, c.bundle.Crypter)
	header := *original
	header.Size = size
	header.Format = tar.FormatPAX
	header.PAXRecords = make(map[string]string, len(original.PAXRecords)+2)
	for key, value := range original.PAXRecords {
		header.PAXRecords[key] = value
	}
	header.PAXRecords[filePartOffsetPAX] = strconv.FormatInt(offset, 10)
	header.PAXRecords[fileSizePAX] = strconv.FormatInt(original.Size, 10)
	c.sets.AddFile(ball.Name(), original.Name)
	packed, err := internal.PackFileTo(ball, &header, io.LimitReader(reader, size))
	if err != nil {
		return errors.Wrapf(err, "pack MongoDB file %s part at offset %d", original.Name, offset)
	}
	if packed != size {
		return errors.Errorf("short MongoDB file %s part at offset %d: got %d bytes, expected %d",
			original.Name, offset, packed, size)
	}
	return c.bundle.TarBallQueue.FinishTarBall(ball)
}
