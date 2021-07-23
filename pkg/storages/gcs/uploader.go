package gcs

import (
	"bytes"
	"context"
	"io"
	"math"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	// The maximum number of chunks cannot exceed 32.
	// So, increase the chunk size to 50 MiB to be able to upload files up to 1600 MiB.
	defaultMaxChunkSize = 50 << 20

	// defaultMaxRetries limits upload and download retries during interaction with GCS.
	defaultMaxRetries = 16
)

type Uploader struct {
	objHandle        *storage.ObjectHandle
	maxChunkSize     int64
	baseRetryDelay   time.Duration
	maxRetryDelay    time.Duration
	maxUploadRetries int
}

type UploaderOption func(*Uploader)

type chunk struct {
	name  string
	index int
	data  []byte
	size  int
}

func NewUploader(objHandle *storage.ObjectHandle, options ...UploaderOption) *Uploader {
	u := &Uploader{
		objHandle:        objHandle,
		maxChunkSize:     defaultMaxChunkSize,
		baseRetryDelay:   BaseRetryDelay,
		maxRetryDelay:    maxRetryDelay,
		maxUploadRetries: defaultMaxRetries,
	}

	for _, opt := range options {
		opt(u)
	}

	return u
}

func (u *Uploader) allocateBuffer() []byte {
	return make([]byte, u.maxChunkSize)
}

// UploadChunk uploads ab object chunk to storage.
func (u *Uploader) UploadChunk(ctx context.Context, chunk chunk) error {
	return u.retry(ctx, u.getUploadFunc(chunk))
}

func (u *Uploader) getUploadFunc(chunk chunk) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		tracelog.DebugLogger.Printf("Upload %s, chunk %d\n", chunk.name, chunk.index)

		writer := u.objHandle.NewWriter(ctx)
		reader := bytes.NewReader(chunk.data[:chunk.size])

		defer func() {
			if closeErr := writer.Close(); closeErr != nil {
				tracelog.WarningLogger.Printf("Unable to close object writer %s, part %d, err: %v", chunk.name, chunk.index, closeErr)

				if err == nil {
					err = closeErr
				}
			}

			// Since compose sources must not have an encryption key, clean up it.
			if err == nil {
				*u.objHandle = *u.objHandle.Key(nil)
			}
		}()

		_, err = io.Copy(writer, reader)
		if err == nil {
			return nil
		}

		tracelog.WarningLogger.Printf("Unable to copy an object chunk %s, part %d, err: %v", chunk.name, chunk.index, err)

		return err
	}
}

// ComposeObject composes an object from temporary chunks.
func (u *Uploader) ComposeObject(ctx context.Context, tmpChunks []*storage.ObjectHandle) error {
	return u.retry(ctx, u.getComposeFunc(tmpChunks))
}

func (u *Uploader) getComposeFunc(tmpChunks []*storage.ObjectHandle) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := u.objHandle.ComposerFrom(tmpChunks...).Run(ctx)
		// Since compose sources must not have an encryption key, clean up it.
		if err == nil {
			*u.objHandle = *u.objHandle.Key(nil)
		}

		return err
	}
}

// CleanUpChunks removes temporary chunks.
func (u *Uploader) CleanUpChunks(ctx context.Context, tmpChunks []*storage.ObjectHandle) {
	for _, tmpChunk := range tmpChunks {
		if err := u.retry(ctx, u.getCleanUpChunksFunc(tmpChunk)); err != nil {
			tracelog.WarningLogger.Printf("Unable to delete a temporary chunk %v. Err: %v", tmpChunk.BucketName(), err)
		}
	}
}

func (u *Uploader) getCleanUpChunksFunc(tmpChunk *storage.ObjectHandle) func(context.Context) error {
	return func(ctx context.Context) error {
		err := tmpChunk.Delete(ctx)

		if err == storage.ErrObjectNotExist {
			tracelog.WarningLogger.Printf("Temporary chunk %v doesn't exist", tmpChunk.BucketName())
			return nil
		}

		return err
	}
}

func (u *Uploader) retry(ctx context.Context, retryableFunc func(ctx context.Context) error) error {
	timer := time.NewTimer(u.baseRetryDelay)
	defer func() {
		timer.Stop()
	}()

	for retry := 0; retry <= u.maxUploadRetries; retry++ {
		err := retryableFunc(ctx)
		if err == nil {
			return nil
		}

		tracelog.ErrorLogger.Printf("Failed to run a retryable func. Err: %v, retrying attempt %d", err, retry)

		tempDelay := u.baseRetryDelay * time.Duration(math.Exp2(float64(retry)))
		sleepInterval := minDuration(u.maxRetryDelay, getJitterDelay(tempDelay/2))

		timer.Reset(sleepInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return errors.Errorf("retry limit has been exceeded, total attempts: %d", u.maxUploadRetries)
}
