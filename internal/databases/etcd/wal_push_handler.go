package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"os/user"
	"path"
	"path/filepath"
	"strings"

	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"

	"github.com/wal-g/wal-g/utility"
)

var errBadWALName = errors.New("bad wal name")

type LogsCache struct {
	LastArchivedWal string `json:"LastArchivedWal"`
}

func cacheDir(dataDir string) string { return filepath.Join(dataDir, ".walg_etcd_wals_cache") }

func getWalDir(dataDir string) string { return filepath.Join(dataDir, "member", "wal") }

func HandleWALPush(ctx context.Context, uploader internal.Uploader, dataDir string) error {
	walDir, ok := conf.GetSetting(conf.ETCDWalDirectory)
	if !ok {
		walDir = getWalDir(dataDir)
	}

	uploader.ChangeDirectory(utility.WalPath)
	files, err := ReadDir(walDir)
	walFiles := checkWalNames(files)
	if err != nil {
		return err
	}

	cache := getCache()
	fromWal := 0
	if len(walFiles) > 0 && cache.LastArchivedWal != "" {
		lastSeq, _ := parseWALName(walFiles[len(walFiles)-1])
		cachedSeq, _ := parseWALName(cache.LastArchivedWal)

		//write ensurance that reading leader member of cluster
		if lastSeq < cachedSeq {
			tracelog.WarningLogger.Printf("wal was reset (%s => %s), clearing cache",
				cache.LastArchivedWal, walFiles[0])
			cache = LogsCache{}
		} else {
			//cacheSeq is last wal that was already archived, start archiving from the next
			fromWal = int(cachedSeq) + 1
			tracelog.WarningLogger.Printf("Start to archive from wal: %s\n",
				cache.LastArchivedWal)
		}
	}

	// Archive all wals that are complete (skip the last one) and have not been archived yet
	for i := fromWal; i < len(walFiles)-1; i++ {
		wal := walFiles[i]

		tracelog.DebugLogger.Printf("Testing... %v\n", wal)

		// Upload wals:
		err = archiveWal(uploader, walDir, wal)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedWal = wal
		putCache(cache)
	}

	// Write Wal Cache (even when no data uploaded, it will create file on first run)
	putCache(cache)
	return nil
}

func archiveWal(uploader internal.Uploader, dataDir string, wal string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", wal)

	filename := path.Join(dataDir, wal)
	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(context.Background(), walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	return nil
}

func getCache() LogsCache {
	var cache LogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = cacheDir(usr.HomeDir)
		var file []byte
		file, err = os.ReadFile(cacheFilename)
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err == nil {
				return cache
			}
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("ETCD wal cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return LogsCache{}
}

func putCache(cache LogsCache) {
	var cacheFilename string
	usr, err := user.Current()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get current user homedir: %v\n", err)
	}

	cacheFilename = cacheDir(usr.HomeDir)
	marshal, err := json.Marshal(&cache)
	if err == nil {
		err = os.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to write ETCD wal cache file: %v\n", err)
		}
	}
}

func ReadDir(d string) ([]string, error) {
	dir, err := os.Open(d)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := dir.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to close directory %v", err)
		}
	}()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	return names, nil
}

func checkWalNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				tracelog.ErrorLogger.Printf("ignored file in WAL directory: %v\n", name)
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func parseWALName(wal string) (seq uint64, err error) {
	if !strings.HasSuffix(wal, ".wal") {
		return 0, errBadWALName
	}

	var index uint64
	_, err = fmt.Sscanf(wal, "%016x-%016x.wal", &seq, &index)

	if index < seq {
		tracelog.ErrorLogger.Printf("wrong naming of wal files. Sequence number can not be bigger than index\n")
	}
	return
}
