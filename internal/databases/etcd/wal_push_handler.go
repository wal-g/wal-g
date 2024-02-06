package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strings"

	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/utility"
)

var errBadWALName = errors.New("bad wal name")

type LogsCache struct {
	LastArchivedIndex uint64 `json:"LastArchivedIndex"`
}

type ServerResponse struct {
	Endpoint string `json:"Endpoint"`
	Status   Status `json:"Status"`
}

type Status struct {
	Leader    uint64 `json:"leader"`
	RaftIndex uint64 `json:"raftIndex"`
	Header    Header `json:"header"`
}

type Header struct {
	ClusterId uint64 `json:"cluster_id"`
	MemberId  uint64 `json:"member_id"`
}

func cacheDir(dataDir string) string { return filepath.Join(dataDir, ".walg_etcd_wals_cache") }

func getWalDir(dataDir string) string { return filepath.Join(dataDir, "member", "wal") }

func HandleWALPush(ctx context.Context, uploader internal.Uploader, dataDir string) error {
	raftIndex := getRaftIndex()
	walDir, ok := internal.GetSetting(internal.ETCDWalDirectory)
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
	if len(walFiles) > 0 && cache.LastArchivedIndex != 0 {
		if raftIndex < cache.LastArchivedIndex {
			tracelog.WarningLogger.Printf("wal was reset (%s => %s), clearing cache",
				cache.LastArchivedIndex, walFiles[0])
			cache = LogsCache{}
		} else {
			tracelog.WarningLogger.Printf("Start to archive from wal record: %s\n",
				cache.LastArchivedIndex)
		}
	}

	// Archive all wals that are complete (skip the last one) and have not been archived yet
	for i := 1; i < len(walFiles); i++ {
		wal := walFiles[i-1]
		tracelog.DebugLogger.Printf("Testing... %v\n", wal)

		ind, err := parseWALName(walFiles[i])
		// if index of last record in wal is not greater than last archived index, skip this wal
		if ind-1 <= cache.LastArchivedIndex {
			continue
		}
		// if index of first record in current wal is greater than raft index, exit the loop, further records can not yet be trusted
		if ind > raftIndex {
			break
		}
		//Now we are sure that all wal records from walFiles[i-1] can be trusted and some of them were not yet archived
		// Upload wals:
		err = archiveWal(uploader, walDir, wal)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedIndex = ind - 1
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

func getRaftIndex() uint64 {
	out, err := exec.Command("etcdctl", "endpoint", "status", "-w", "json").Output()
	if err != nil {
		tracelog.ErrorLogger.Println("Could not check if node is a leader ", err)
	}

	var data []ServerResponse
	err = json.Unmarshal(out, &data)
	if err != nil || len(data) == 0 {
		tracelog.ErrorLogger.Println("Could not unmarshal etcd output ", err)
	}

	response := data[0]
	if response.Status.Leader != response.Status.Header.MemberId {
		tracelog.ErrorLogger.Println("Current node is not leader, it can provide inconsistent wal records", err)
	}

	return response.Status.RaftIndex
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

func parseWALName(wal string) (index uint64, err error) {
	if !strings.HasSuffix(wal, ".wal") {
		return 0, errBadWALName
	}

	var seq uint64
	_, err = fmt.Sscanf(wal, "%016x-%016x.wal", &seq, &index)

	if index < seq {
		tracelog.ErrorLogger.Printf("wrong naming of wal files. Sequence number can not be bigger than index\n")
	}
	return
}
