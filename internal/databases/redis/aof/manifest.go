package aof

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/tracelog"
)

type BackupFilesListProvider struct {
	ManifestPath string
	AOFFolder    string
}

func NewBackupFilesListProvider(path string) *BackupFilesListProvider {
	return &BackupFilesListProvider{
		ManifestPath: path,
		AOFFolder:    filepath.Dir(path),
	}
}

func (p *BackupFilesListProvider) Get() []string {
	res := []string{p.ManifestPath}
	lines := readManifest(p.ManifestPath)
	addon := parseManifest(lines, p.AOFFolder)
	res = append(res, addon...)
	return res
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
