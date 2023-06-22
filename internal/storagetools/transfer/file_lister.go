package transfer

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type FileLister interface {
	ListFilesToMove(sourceStorage, targetStorage storage.Folder) (files []FilesGroup, num int, err error)
}

// FilesGroup is an ordered set of files that must be transferred atomically
type FilesGroup []FileToMove

type FileToMove struct {
	path        string
	copyAfter   []string
	deleteAfter []string
}

type RegularFileLister struct {
	Prefix    string
	Overwrite bool
	MaxFiles  int
}

func NewRegularFileLister(prefix string, overwrite bool, maxFiles int) *RegularFileLister {
	return &RegularFileLister{
		Prefix:    prefix,
		Overwrite: overwrite,
		MaxFiles:  maxFiles,
	}
}

func (l *RegularFileLister) ListFilesToMove(source, target storage.Folder) (files []FilesGroup, num int, err error) {
	missingFiles, err := listMissingFiles(source, target, l.Prefix, l.Overwrite)
	if err != nil {
		return nil, 0, err
	}
	limitedFiles := limitFiles(missingFiles, l.MaxFiles)
	return limitedFiles, len(limitedFiles), nil
}

func listMissingFiles(source, target storage.Folder, prefix string, overwrite bool) (map[string]storage.Object, error) {
	targetFiles, err := storage.ListFolderRecursivelyWithPrefix(target, prefix)
	if err != nil {
		return nil, fmt.Errorf("list files in the target storage: %w", err)
	}
	sourceFiles, err := storage.ListFolderRecursivelyWithPrefix(source, prefix)
	if err != nil {
		return nil, fmt.Errorf("list files in the source storage: %w", err)
	}
	tracelog.InfoLogger.Printf("Total files in the source storage: %d", len(sourceFiles))

	missingFiles := make(map[string]storage.Object, len(sourceFiles))
	for _, sourceFile := range sourceFiles {
		missingFiles[sourceFile.GetName()] = sourceFile
	}
	for _, targetFile := range targetFiles {
		sourceFile, presentInBothStorages := missingFiles[targetFile.GetName()]
		if !presentInBothStorages {
			continue
		}
		if overwrite {
			logSizesDifference(sourceFile, targetFile)
		} else {
			delete(missingFiles, targetFile.GetName())
		}
	}
	tracelog.InfoLogger.Printf("Files missing in the target storage: %d", len(missingFiles))
	return missingFiles, nil
}

func logSizesDifference(sourceFile, targetFile storage.Object) {
	if sourceFile.GetSize() != targetFile.GetSize() {
		tracelog.WarningLogger.Printf(
			"File present in both storages and its size is different: %q (source %d bytes VS target %d bytes)",
			targetFile.GetName(),
			sourceFile.GetSize(),
			targetFile.GetSize(),
		)
	}
}

func limitFiles(files map[string]storage.Object, max int) []FilesGroup {
	count := 0
	fileGroups := make([]FilesGroup, 0, utility.Min(max, len(files)))
	for _, file := range files {
		if count >= max {
			break
		}
		singleFileGroup := FilesGroup{
			FileToMove{path: file.GetName()},
		}
		fileGroups = append(fileGroups, singleFileGroup)
		count++
	}
	tracelog.InfoLogger.Printf("Files will be transferred: %d", len(fileGroups))
	return fileGroups
}
