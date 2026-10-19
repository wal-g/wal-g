package copy

import (
	"context"
	"fmt"
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/statistics"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/sync/errgroup"
)

type SourceTransformerFunc func(context.Context, io.Reader) (io.ReadCloser, error)

type InfoProvider struct {
	From storage.Folder

	SourceTransformer SourceTransformerFunc

	To storage.Folder

	SrcObj     storage.Object
	targetName string
}

func Infos(ctx context.Context, chs []InfoProvider) error {
	return InfosWithOptions(ctx, chs, ExecuteOptions{})
}

func InfosWithOptions(ctx context.Context, chs []InfoProvider, options ExecuteOptions) error {
	retries, err := options.copyRetries()
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(defaultRawCopyConcurrency)
	for _, ch := range chs {
		if groupCtx.Err() != nil {
			break
		}
		group.Go(func() error { return ch.copyObject(groupCtx, retries) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func (ch *InfoProvider) copyObject(ctx context.Context, retries int) error {
	statistics.WalgMetrics.UploadedFilesTotal.Inc()
	err := retryCopy(ctx, retries, copyRetryDelay, func(ctx context.Context) error {
		return streamObject(ctx, ch.From, ch.To, ch.SrcObj.GetName(), ch.targetName,
			ch.SrcObj.GetSize(), ch.SourceTransformer)
	})
	if err != nil {
		statistics.WalgMetrics.UploadedFilesFailedTotal.Inc()
		return fmt.Errorf("copy %q to %q: %w", ch.SrcObj.GetName(), ch.targetName, err)
	}

	tracelog.InfoLogger.Printf(
		"Copied '%s' from folder '%s' to '%s' in folder '%s'.",
		ch.SrcObj.GetName(), ch.From.GetPath(), ch.targetName, ch.To.GetPath())
	return nil
}

var NoopRenameFunc = func(o storage.Object) string {
	if o == nil {
		return ""
	}
	return o.GetName()
}

var NoopSourceTransformer = func(_ context.Context, r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

func BuildCopyingInfos(
	from storage.Folder,
	to storage.Folder,
	objects []storage.Object,
	filter func(storage.Object) bool,
	renameFunc func(object storage.Object) string,
	sourceTransformer SourceTransformerFunc) (infos []InfoProvider) {
	tracelog.DebugLogger.Println("processing copy infos filtering")

	for _, object := range objects {
		if filter(object) {
			infos = append(infos, InfoProvider{
				From:              from,
				To:                to,
				SrcObj:            object,
				targetName:        renameFunc(object),
				SourceTransformer: sourceTransformer,
			})
			tracelog.DebugLogger.Printf("add copy info %s-%s \n", object.GetName(), renameFunc(object))
		}
	}
	return
}
