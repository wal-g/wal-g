package aof

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/tracelog"
)

type BackupFilesListProvider struct {
	ReadManifestPath   string
	UploadManifestPath string
	AOFFolder          string
}

func NewBackupFilesListProvider(readFolder, uploadFolder, name string) *BackupFilesListProvider {
	return &BackupFilesListProvider{
		AOFFolder:          readFolder,
		ReadManifestPath:   filepath.Join(readFolder, name),
		UploadManifestPath: filepath.Join(uploadFolder, name),
	}
}

func (p *BackupFilesListProvider) Get() []string {
	res := []string{p.UploadManifestPath}
	lines := readManifest(p.ReadManifestPath)
	copyManifestToUpload(lines, p.UploadManifestPath)
	addon := parseManifest(lines, p.AOFFolder)
	validateAofFiles(lines, p.AOFFolder)
	res = append(res, addon...)
	return res
}

// validateAofFiles guards against backing up a temporarily broken AOF:
// during an AOF rewrite the on-disk files may be briefly missing or truncated.
// Failing fast here (instead of uploading a broken backup) lets the caller
// (e.g. the salt orchestration) retry after the rewrite finishes.
//
// An empty incremental file is a valid state (an empty database or no writes
// since the last rewrite), so only the base file is required to be non-empty.
func validateAofFiles(lines []string, folder string) {
	hasBase := false
	for _, line := range lines {
		chunks := strings.Fields(line)
		if len(chunks) != 6 {
			tracelog.ErrorLogger.Fatalf("unexpected line format in manifest file: %s", line)
		}
		path := filepath.Join(folder, chunks[1])
		info, err := os.Stat(path)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("can not stat aof file %s: %v", path, err)
		}
		if chunks[5] == "b" {
			hasBase = true
			if info.Size() == 0 {
				tracelog.ErrorLogger.Fatalf("aof base file %s is empty, refusing to back up a broken AOF (aof rewrite in progress?)", path)
			}
		}
	}
	if !hasBase {
		tracelog.ErrorLogger.Fatalf("manifest has no base aof file, refusing to back up a broken AOF (aof rewrite in progress?)")
	}
}

func copyManifestToUpload(lines []string, path string) {
	folder := filepath.Dir(path)
	err := os.MkdirAll(folder, 0644)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("error creating temp folder %s: %v", folder, err)
	}

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("error creating temp manifest %s: %v", path, err)
	}
	defer file.Close()

	for _, line := range lines {
		_, err := file.Write([]byte(line + "\n"))
		if err != nil {
			tracelog.ErrorLogger.Fatalf("error writing line %s to temp manifest %s: %v", line, path, err)
		}
	}
}

func readManifest(manifestPath string) []string {
	manifest, err := os.Open(manifestPath)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed to open manifest file %s: %v", manifestPath, err)
	}
	defer manifest.Close()

	scanner := bufio.NewScanner(manifest)
	var res []string
	for scanner.Scan() {
		res = append(res, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		tracelog.ErrorLogger.Fatalf("error scanning manifest file %s: %v", manifestPath, err)
	}
	if len(res) == 0 {
		tracelog.ErrorLogger.Fatalf("no records in manifest file %s: %v", manifestPath, err)
	}

	return res
}

func parseManifest(lines []string, folder string) []string {
	// file appendonly.aof.1.base.rdb seq 1 type b
	// file appendonly.aof.1.incr.aof seq 1 type i
	var res []string
	for _, line := range lines {
		chunks := strings.Fields(line)
		if len(chunks) != 6 {
			tracelog.ErrorLogger.Fatalf("unexpected line format in manifest file: %s", line)
		}
		res = append(res, filepath.Join(folder, chunks[1]))
	}
	return res
}
