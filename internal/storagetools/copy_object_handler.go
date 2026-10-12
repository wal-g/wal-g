package storagetools

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func Encrypt(ctx context.Context, source io.Reader, crypter crypto.Crypter) (io.ReadCloser, error) {
	if crypter == nil {
		return io.NopCloser(source), nil
	}

	cryptReader, dstWriter := io.Pipe()

	writeCloser, err := crypter.Encrypt(ctx, dstWriter)

	if err != nil {
		_ = cryptReader.Close()
		_ = dstWriter.Close()
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer dstWriter.Close()
		_, err := utility.FastCopy(writeCloser, source)

		if err != nil {
			_ = dstWriter.CloseWithError(err)
			_ = writeCloser.Close()
			return
		}

		err = writeCloser.Close()
		if err != nil {
			_ = dstWriter.CloseWithError(err)
			return
		}

		_ = dstWriter.Close()
	}()

	return &encryptionReader{PipeReader: cryptReader, done: done}, nil
}

type encryptionReader struct {
	*io.PipeReader
	done <-chan struct{}
	once sync.Once
}

func (r *encryptionReader) Close() error {
	err := r.PipeReader.Close()
	r.once.Do(func() { <-r.done })
	return err
}

func collectCopyingInfo(
	ctx context.Context,
	prefix string,
	fromConfigFile string,
	toConfigFile string,
	decryptSource bool,
	encryptTarget bool) ([]copy.InfoProvider, copy.ExecuteOptions, error) {
	tracelog.InfoLogger.Printf("Collecting files with prefix %s.", prefix)
	from, fromConfig, err := internal.StorageAndConfigFromFile(ctx, fromConfigFile)
	if err != nil {
		return nil, copy.ExecuteOptions{}, err
	}
	to, toConfig, err := internal.StorageAndConfigFromFile(ctx, toConfigFile)
	if err != nil {
		return nil, copy.ExecuteOptions{}, err
	}
	options, err := copy.OptionsFromConfigs(fromConfig, toConfig)
	if err != nil {
		return nil, copy.ExecuteOptions{}, err
	}

	objects, err := storage.ListFolderRecursively(ctx, from.RootFolder())
	if err != nil {
		return nil, copy.ExecuteOptions{}, err
	}

	var hasPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), prefix) }
	return copy.BuildCopyingInfos(
		from.RootFolder(),
		to.RootFolder(),
		objects,
		hasPrefix,
		func(object storage.Object) string {
			return object.GetName()
		},
		func(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
			if decryptSource {
				decrypted, decryptErr := internal.DecryptBytes(ctx, r)
				if decryptErr != nil {
					return nil, decryptErr
				}
				r = decrypted
			}

			if encryptTarget {
				crypter := internal.CrypterFromConfig(toConfigFile)
				return Encrypt(ctx, r, crypter)
			}

			return io.NopCloser(r), nil
		},
	), options, nil
}

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyObjects(
	ctx context.Context,
	fromConfigFile, toConfigFile, prefix string,
	decryptSource, encryptTarget bool) {
	infos, options, err := collectCopyingInfo(ctx, prefix, fromConfigFile, toConfigFile, decryptSource,
		encryptTarget)
	tracelog.ErrorLogger.FatalOnError(err)

	// TODO: truncate this log line, because it may grow really big?
	tracelog.DebugLogger.Printf("copying files %s\n", strings.Join(func() []string {
		ret := make([]string, 0)
		for _, e := range infos {
			ret = append(ret, e.SrcObj.GetName())
		}

		return ret
	}(), ","))

	tracelog.ErrorLogger.FatalOnError(copy.InfosWithOptions(ctx, infos, options))

	tracelog.InfoLogger.Printf("Successfully copied %d objects", len(infos))
}
