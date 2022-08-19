package sqlserver

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func prepareBackupExportSpec(d map[string]string) (dbnames []string, mapping map[string]string) {
	mapping = make(map[string]string, len(d))
	for k, v := range d {
		dbnames = append(dbnames, k)
		if v == "" {
			v = k
		}
		mapping[k] = v
	}
	return
}

func HandleBackupExport(externalConfig string, exportPrefixes map[string]string) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	externalFolder, err := internal.FolderFromConfig(externalConfig)
	tracelog.ErrorLogger.FatalOnError(err)

	dbnames, exportPrefixes := prepareBackupExportSpec(exportPrefixes)
	_, err = resolveExternalStorageFiles(externalFolder, nil)
	tracelog.ErrorLogger.FatalOnError(err)

	lock, err := RunOrReuseProxy(ctx, cancel, folder)
	tracelog.ErrorLogger.FatalOnError(err)
	defer lock.Close()

	var backupName string
	var sentinel *SentinelDto
	backup, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("can't find latest backup: %v", err)
	backupName = backup.Name
	sentinel = new(SentinelDto)
	err = backup.FetchSentinel(sentinel)
	tracelog.ErrorLogger.FatalOnError(err)

	err = runParallel(func(i int) error {
		return exportSingleDatabaseBackup(ctx, folder, backupName, dbnames[i], externalFolder, exportPrefixes[dbnames[i]])
	}, len(dbnames), getDBConcurrency())
	tracelog.ErrorLogger.FatalfOnError("overall export failed: %v", err)

	sentinel.Databases = uniq(append(sentinel.Databases, dbnames...))
	uploader := internal.NewUploader(nil, folder.GetSubFolder(utility.BaseBackupPath))
	tracelog.InfoLogger.Printf("uploading sentinel: %s", sentinel)
	err = internal.UploadSentinel(uploader, sentinel, backupName)
	tracelog.ErrorLogger.FatalfOnError("failed to save sentinel: %v", err)

	tracelog.InfoLogger.Printf("export finished")
}

func exportSingleDatabaseBackup(ctx context.Context, folder storage.Folder, backupName string,
	dbname string, externalFolder storage.Folder, prefix string) error {
	baseURL := getDatabaseBackupURL(backupName, dbname)
	basePath := getDatabaseBackupPath(backupName, dbname)
	blobs, err := listBackupBlobs(folder.GetSubFolder(basePath))
	if err != nil {
		tracelog.ErrorLogger.Printf("database %v doesn't exist in backup %v", dbname, backupName)
		return err
	}
	urls := buildRestoreUrlsList(baseURL, blobs)
	tracelog.InfoLogger.Printf("starting exporting backup files for %v from %v", dbname, blobs)
	for i := 0; i < len(urls); i++ {
		backupFileName := fmt.Sprintf("%s_%03d.bak", prefix, i)
		err := exportSingleDatabaseBlob(ctx, urls[i], externalFolder, backupFileName)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to export backup %v: %v", backupFileName, err)
			return err
		}
		tracelog.InfoLogger.Printf("backup file %v exported", backupFileName)
	}
	tracelog.InfoLogger.Printf("all backup files for %v are exported successfully", dbname)
	return nil
}

func exportSingleDatabaseBlob(ctx context.Context, blobURL string, externalFolder storage.Folder, backupFile string) error {
	client := getProxyHTTPClient()
	lease, err := acquireLease(ctx, client, blobURL)
	if err != nil {
		return err
	}
	defer releaseLeasePrintError(ctx, client, blobURL, lease)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Ms-Lease-Id", lease)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected proxy GET response: %d %s", resp.StatusCode, resp.Status)
	}
	defer resp.Body.Close()
	return externalFolder.PutObject(backupFile, resp.Body)
}
