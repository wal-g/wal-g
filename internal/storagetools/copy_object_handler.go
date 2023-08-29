package storagetools

import (
	"io"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

func Encrypt(source io.Reader, crypter crypto.Crypter) (io.Reader, error) {
	if crypter == nil {
		return source, nil
	}

	cryptReader, dstWriter := io.Pipe()

	writeCloser, err := crypter.Encrypt(dstWriter)

	if err != nil {
		return nil, err
	}

	go func() {
		_, err := utility.FastCopy(writeCloser, source)

		if err != nil {
			_ = dstWriter.CloseWithError(err)
			return
		}

		err = writeCloser.Close()
		if err != nil {
			_ = dstWriter.CloseWithError(err)
			return
		}

		_ = dstWriter.Close()
	}()

	return cryptReader, nil
}

func collectCopyingInfo(
	prefix string,
	fromConfigFile string,
	toConfigFile string,
	decryptSource bool,
	encryptTarget bool) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Printf("Collecting files with prefix %s.", prefix)
	from, err := internal.FolderFromConfig(fromConfigFile)
	if err != nil {
		return nil, err
	}
	to, err := internal.FolderFromConfig(toConfigFile)
	if err != nil {
		return nil, err
	}

	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	var hasPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), prefix) }
	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		hasPrefix,
		func(object storage.Object) string {
			return object.GetName()
		},
		func(r io.Reader) (io.Reader, error) {
			if decryptSource {
				r, err = internal.DecryptBytes(r)
				if err != nil {
					return nil, err
				}
			}

			if encryptTarget {
				crypter := internal.CrypterFromConfig(toConfigFile)
				r, err = Encrypt(r, crypter)
				if err != nil {
					return nil, err
				}
			}

			return r, nil
		},
	), nil
}

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyObjects(
	fromConfigFile, toConfigFile, prefix string,
	decryptSource, encryptTarget bool) {
	infos, err := collectCopyingInfo(prefix, fromConfigFile, toConfigFile, decryptSource,
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

	tracelog.ErrorLogger.FatalOnError(copy.Infos(infos))

	tracelog.InfoLogger.Printf("Success copyed objects.\n")
}
