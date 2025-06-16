package archive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const SlotsFileName = "slots.json"

func GetSlotsCompressedFileName(backupName string) (string, error) {
	upl, err := internal.ConfigureUploader()
	if err != nil {
		return "", err
	}

	fileName := fmt.Sprintf("%s.%s", SlotsFileName, upl.Compression().FileExtension())
	return filepath.Join(backupName, fileName), nil
}

func getFQDNToIDMap() (map[string]string, error) {
	var fqdnMap map[string]string
	fqdnMapRaw := viper.GetString(conf.RedisFQDNToIDMap)
	// {"fqdn1": "id1", ...}
	err := json.Unmarshal([]byte(fqdnMapRaw), &fqdnMap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse FQDN to id mapping as a JSON object")
	}

	return fqdnMap, nil
}

func getIntervals(line string) ([][]string, error) {
	// 56cac18e538888e2fb81b09b8491e819d2bda1e1 2a02:6b8:c18:3e81:0:1589:4138:e47b:6379@16379 master,nofailover -
	// 0 1747228909000 44 connected 2731-5460 10923-13653 [10923->3d68e5b49b010564b64c8a4ac26536a8d6a756f8]
	slotsPart := strings.Split(line, "connected")[1]
	if strings.Contains(slotsPart, "[") {
		return [][]string{}, fmt.Errorf("there are slots migrating: %s", slotsPart)
	}

	var intervals [][]string
	for _, intervalRaw := range strings.Split(slotsPart, " ") {
		if strings.TrimSpace(intervalRaw) == "" {
			continue
		}

		if strings.Contains(intervalRaw, "-") {
			ends := strings.Split(intervalRaw, "-")
			intervals = append(intervals, []string{ends[0], ends[1]})
		} else {
			intervals = append(intervals, []string{intervalRaw, intervalRaw})
		}
	}
	return intervals, nil
}

func GetSlotsMap() (map[string][][]string, error) {
	fqdnToIDMap, err := getFQDNToIDMap()
	if err != nil {
		return map[string][][]string{}, err
	}

	clusterConfPath := viper.GetString(conf.RedisClusterConfPath)
	clusterConf, err := os.Open(clusterConfPath)
	if err != nil {
		return map[string][][]string{}, err
	}
	defer clusterConf.Close()

	idToSlots := make(map[string][][]string)
	scanner := bufio.NewScanner(clusterConf)
	for scanner.Scan() {
		line := scanner.Text()
		// 56cac18e538888e2fb81b09b8491e819d2bda1e1 2a02:6b8:c18:3e81:0:1589:4138:e47b:6379@16379 master,nofailover -
		// 0 1747228909000 44 connected 2731-5460 10923-13653 [10923->3d68e5b49b010564b64c8a4ac26536a8d6a756f8]
		if !strings.Contains(line, "master") {
			continue
		}

		intervals, err := getIntervals(line)
		if err != nil {
			return map[string][][]string{}, err
		}

		ipWithPorts := strings.Split(line, " ")[1]
		parts := strings.Split(ipWithPorts, ":")
		ip := strings.Join(parts[:len(parts)-1], ":")

		var fqdns []string
		if ip == "" {
			host, err := os.Hostname()
			if err != nil {
				return map[string][][]string{}, err
			}
			fqdns = append(fqdns, host)
		} else {
			fqdns, err = net.LookupAddr(ip)
			if err != nil {
				return map[string][][]string{}, errors.Wrapf(err, "failed to find address %s", ip)
			}
		}

		found := false
		for _, fqdn := range fqdns {
			if id, ok := fqdnToIDMap[fqdn]; ok {
				found = true
				idToSlots[id] = intervals
			}
		}

		if !found {
			return map[string][][]string{}, fmt.Errorf("failed to find ID from %+v in %+v", fqdns, fqdnToIDMap)
		}
	}

	return idToSlots, nil
}

func FetchSlotsDataFromStorage(folder storage.Folder, backup *Backup) (string, error) {
	tmpDir, err := os.MkdirTemp("/tmp", "slots_data")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	compressedFileName, err := GetSlotsCompressedFileName(backup.BackupName)
	if err != nil {
		return "", err
	}

	fileName := utility.TrimFileExtension(compressedFileName)
	tarInterpreter := internal.NewFileTarInterpreter(tmpDir)
	intBackup := backup.ToInternal(folder)
	storageFolder := intBackup.Folder.GetSubFolder("")

	pathToExtract := internal.NewRegularFileStorageReaderMarker(storageFolder, compressedFileName, fileName, 0644)
	err = internal.ExtractAll(tarInterpreter, []internal.ReaderMaker{pathToExtract})
	if err != nil {
		return "", errors.Wrapf(err, "file %s in folder %s", compressedFileName, storageFolder.GetPath())
	}

	localPath := filepath.Join(tmpDir, fileName)
	f, err := os.ReadFile(localPath)
	if err != nil {
		return "", err
	}

	return string(f), nil
}

type FileUploader interface {
	UploadExactFile(ctx context.Context, file ioextensions.NamedReader) error
}

type FillSlotsForShardedArgs struct {
	BackupName string
	Sharded	bool
	Uploader FileUploader
}

func FillSlotsForSharded(ctx context.Context, args FillSlotsForShardedArgs) error {
	if !args.Sharded {
		return nil
	}

	idToSlots, err := GetSlotsMap()
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(idToSlots)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("packing %s", string(jsonData))

	fullPath, err := GetSlotsCompressedFileName(args.BackupName)
	if err != nil {
		return err
	}

	file := ioextensions.NewNamedReaderImpl(bytes.NewReader(jsonData), fullPath)
	err = args.Uploader.UploadExactFile(ctx, file)
	if err != nil {
		return err
	}

	return nil
}