package postgres

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/spf13/viper"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type InvalidWalFileMagicError struct {
	error
}

func newInvalidWalFileMagicError() InvalidWalFileMagicError {
	return InvalidWalFileMagicError{errors.New("WAL-G: WAL file magic is invalid ")}
}

func (err InvalidWalFileMagicError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(folder storage.Folder, walFileName string, location string, triggerPrefetch bool) {
	tracelog.DebugLogger.Printf("HandleWALFetch(folder, %s, %s, %v)\n", walFileName, location, triggerPrefetch)
	folder = folder.GetSubFolder(utility.WalPath)
	location = utility.ResolveSymlink(location)
	if triggerPrefetch {
		prefetchLocation := location
		if viper.IsSet(internal.PrefetchDir) {
			prefetchLocation = viper.GetString(internal.PrefetchDir)
		}
		defer forkPrefetch(walFileName, prefetchLocation)
	}

	_, _, running, prefetched := getPrefetchLocations(path.Dir(location), walFileName)
	seenSize := int64(-1)

	sizeStallInterations := 0
	maxSizeStallTerations := 100
	for {
		if stat, err := os.Stat(prefetched); err == nil {
			if stat.Size() != int64(WalSegmentSize) {
				tracelog.ErrorLogger.Println("WAL-G: Prefetch error: wrong file size of prefetched file ", stat.Size())
				break
			}

			err = os.Rename(prefetched, location)
			tracelog.ErrorLogger.FatalOnError(err)

			err := checkWALFileMagic(location)
			if err != nil {
				tracelog.ErrorLogger.Println("Prefetched file contain errors", err)
				_ = os.Remove(location)
				break
			}

			return
		} else if !os.IsNotExist(err) {
			tracelog.ErrorLogger.FatalError(err)
		}

		// We have race condition here, if running is renamed here, but it's OK

		if runStat, err := os.Stat(running); err == nil {
			observedSize := runStat.Size() // If there is no progress in 200 ms (100 iterations for 2ms)- start downloading myself
			if observedSize <= seenSize {
				sizeStallInterations++
				if sizeStallInterations >= maxSizeStallTerations {
					defer func() {
						_ = os.Remove(running) // we try to clean up and ignore here any error
						_ = os.Remove(prefetched)
					}()
					break
				}
			} else {
				sizeStallInterations = 0
				seenSize = observedSize
			}
		} else if os.IsNotExist(err) {
			break // Normal startup path
		} else {
			break // Abnormal path. Permission denied etc. Yes, I know that previous 'else' can be eliminated.
		}
		time.Sleep(2 * time.Millisecond)
	}

	err := internal.DownloadFileTo(folder, walFileName, location)
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")
	magic := make([]byte, 4)
	_, err = file.Read(magic)
	if err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return newInvalidWalFileMagicError()
	}

	return nil
}
