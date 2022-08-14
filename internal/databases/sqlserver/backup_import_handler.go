package sqlserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func prepareBackupImportSpec(externalStorage storage.Folder, d map[string]string) ([]string, map[string][]storage.Object, error) {
	dbnames := make([]string, 0, len(d))
	fileNames := make([]string, 0, len(d))
	fileNameMapping := make(map[string][]string, len(d))
	for dbname, v := range d {
		dbnames = append(dbnames, dbname)
		fileNameMapping[dbname] = strings.Split(v, ExternalBackupFilenameSeparator)
		fileNames = append(fileNames, fileNameMapping[dbname]...)
	}
	objects, err := resolveExternalStorageFiles(externalStorage, fileNames)
	if err != nil {
		return nil, nil, err
	}
	fileMapping := make(map[string][]storage.Object, len(d))
	for dbname, names := range fileNameMapping {
		for _, fileName := range names {
			fileMapping[dbname] = append(fileMapping[dbname], objects[fileName])
		}
	}
	return dbnames, fileMapping, nil
}

func resolveExternalStorageFiles(externalFolder storage.Folder, fileNames []string) (map[string]storage.Object, error) {
	objs, _, err := externalFolder.ListFolder()
	if err != nil {
		return nil, fmt.Errorf("failed to access external storage: %w", err)
	}
	allObjects := make(map[string]storage.Object)
	for _, obj := range objs {
		allObjects[obj.GetName()] = obj
	}
	objects := make(map[string]storage.Object, len(fileNames))
	notFound := make([]string, 0)
	for _, n := range fileNames {
		if obj, ok := allObjects[n]; ok {
			objects[n] = obj
		} else {
			notFound = append(notFound, n)
		}
	}
	if len(notFound) > 0 {
		return nil, fmt.Errorf("backup files %v not found in the external storage", notFound)
	}
	return objects, nil
}

func HandleBackupImport(externalConfig string, importDatabases map[string]string) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	externalFolder, err := internal.FolderFromConfig(externalConfig)
	tracelog.ErrorLogger.FatalOnError(err)

	dbnames, databaseFiles, err := prepareBackupImportSpec(externalFolder, importDatabases)
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
		return importSingleDatabaseBackup(ctx, backupName, dbnames[i], externalFolder, databaseFiles[dbnames[i]])
	}, len(dbnames), getDBConcurrency())
	tracelog.ErrorLogger.FatalfOnError("overall import failed: %v", err)

	sentinel.Databases = uniq(append(sentinel.Databases, dbnames...))
	uploader := internal.NewUploader(nil, folder.GetSubFolder(utility.BaseBackupPath))
	tracelog.InfoLogger.Printf("uploading sentinel: %s", sentinel)
	err = internal.UploadSentinel(uploader, sentinel, backupName)
	tracelog.ErrorLogger.FatalfOnError("failed to save sentinel: %v", err)

	tracelog.InfoLogger.Printf("import finished")
}

func importSingleDatabaseBackup(ctx context.Context, backupName string, dbname string,
	externalFolder storage.Folder, backupFiles []storage.Object) error {
	baseURL := getDatabaseBackupURL(backupName, dbname)
	urls := buildBackupUrlsList(baseURL, len(backupFiles))
	tracelog.InfoLogger.Printf("starting importing backup files for %v from %v", dbname, backupFiles)
	for i := 0; i < len(backupFiles); i++ {
		err := importSingleDatabaseBlob(ctx, externalFolder, backupFiles[i], urls[i])
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to import backup %v: %v", backupFiles[i], err)
			return err
		}
		tracelog.InfoLogger.Printf("backup file %v imported", backupFiles[i])
	}
	tracelog.InfoLogger.Printf("all backup files for %v are imported successfully", dbname)
	return nil
}

func getProxyHTTPClient() *http.Client {
	config := &tls.Config{InsecureSkipVerify: true}
	tr := &http.Transport{
		TLSClientConfig:   config,
		ForceAttemptHTTP2: false,
		TLSNextProto:      make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
	}
	client := &http.Client{Transport: tr}
	return client
}

func acquireLease(ctx context.Context, client *http.Client, blobURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL+"?comp=lease", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Ms-Lease-Action", "Acquire")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("unexpected proxy acquire lease response: %d %s", resp.StatusCode, resp.Status)
	}
	return resp.Header.Get("X-Ms-Lease-Id"), nil
}

func releaseLease(ctx context.Context, client *http.Client, blobURL string, lease string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL+"?comp=lease", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Ms-Lease-Action", "Release")
	req.Header.Set("X-Ms-Lease-Id", lease)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected proxy acquire lease response: %d %s", resp.StatusCode, resp.Status)
	}
	return nil
}

func releaseLeasePrintError(ctx context.Context, client *http.Client, blobURL string, lease string) {
	err := releaseLease(ctx, client, blobURL, lease)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to release lease %s: %v", lease, err)
	}
}

func importSingleDatabaseBlob(ctx context.Context, externalFolder storage.Folder, backupFile storage.Object, blobURL string) error {
	rc, err := externalFolder.ReadObject(backupFile.GetName())
	if err != nil {
		return err
	}
	defer rc.Close()
	client := getProxyHTTPClient()
	lease, err := acquireLease(ctx, client, blobURL)
	if err != nil {
		return err
	}
	defer releaseLeasePrintError(ctx, client, blobURL, lease)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, blobURL, rc)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", backupFile.GetSize()))
	req.Header.Set("X-Ms-Lease-Id", lease)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected proxy PUT response: %d %s", resp.StatusCode, resp.Status)
	}
	return nil
}
