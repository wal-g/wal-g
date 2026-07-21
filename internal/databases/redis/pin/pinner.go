package pin

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"
)

// FilesPinner protects upload sources from being reclaimed while they are read.
// Each source is hard-linked into PinFolder and kept open until Unpin is called.
type FilesPinner struct {
	pinFolder   string
	pinnedPaths []string
	openFiles   []*os.File
}

func NewFilesPinner(pinFolder string) *FilesPinner {
	return &FilesPinner{pinFolder: pinFolder}
}

// Pin preserves the legacy AOF flat-by-basename layout.
func (p *FilesPinner) Pin(paths []string) ([]string, error) {
	for _, sourcePath := range paths {
		pinnedPath := filepath.Join(p.pinFolder, filepath.Base(sourcePath))
		if err := p.pinFile(sourcePath, pinnedPath); err != nil {
			p.Unpin()
			return nil, err
		}
	}
	return p.pinnedPaths, nil
}

// PinTree recursively mirrors sourceDir below PinFolder, preserving relative paths.
// Transient module artifacts are intentionally excluded.
func (p *FilesPinner) PinTree(sourceDir string) ([]string, error) {
	if err := ValidateSameFilesystem(sourceDir, p.pinFolder); err != nil {
		return nil, err
	}
	if err := filepath.WalkDir(sourceDir, func(sourcePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || isTransient(entry.Name()) {
			return nil
		}
		relativePath, err := filepath.Rel(sourceDir, sourcePath)
		if err != nil {
			return err
		}
		return p.pinFile(sourcePath, filepath.Join(p.pinFolder, relativePath))
	}); err != nil {
		p.Unpin()
		return nil, err
	}
	return p.pinnedPaths, nil
}

func (p *FilesPinner) pinFile(sourcePath, pinnedPath string) error {
	if err := os.MkdirAll(filepath.Dir(pinnedPath), 0o700); err != nil {
		return fmt.Errorf("create pin directory for %s: %w", sourcePath, err)
	}
	if sourcePath != pinnedPath {
		if err := os.Link(sourcePath, pinnedPath); err != nil {
			return fmt.Errorf("pin file %s: %w", sourcePath, err)
		}
	}
	file, err := os.Open(pinnedPath)
	if err != nil {
		return fmt.Errorf("hold pinned file %s open: %w", pinnedPath, err)
	}
	p.pinnedPaths = append(p.pinnedPaths, pinnedPath)
	p.openFiles = append(p.openFiles, file)
	return nil
}

func (p *FilesPinner) Unpin() {
	for _, file := range p.openFiles {
		if err := file.Close(); err != nil {
			tracelog.ErrorLogger.Printf("failed to close pinned file %s: %v", file.Name(), err)
		}
	}
	for i := len(p.pinnedPaths) - 1; i >= 0; i-- {
		if err := os.Remove(p.pinnedPaths[i]); err != nil && !os.IsNotExist(err) {
			tracelog.ErrorLogger.Printf("failed to remove pinned file %s: %v", p.pinnedPaths[i], err)
		}
	}
	p.openFiles = nil
	p.pinnedPaths = nil
}

func ValidateSameFilesystem(sourceDir, pinFolder string) error {
	sourceInfo, err := os.Stat(sourceDir)
	if err != nil {
		return fmt.Errorf("stat ts source directory %s: %w", sourceDir, err)
	}
	pinInfo, err := os.Stat(pinFolder)
	if err != nil {
		return fmt.Errorf("stat ts pin folder %s: %w", pinFolder, err)
	}
	sourceStat, sourceOK := sourceInfo.Sys().(*syscall.Stat_t)
	pinStat, pinOK := pinInfo.Sys().(*syscall.Stat_t)
	if !sourceOK || !pinOK {
		return fmt.Errorf("cannot determine filesystem for ts source %s and pin folder %s", sourceDir, pinFolder)
	}
	if sourceStat.Dev != pinStat.Dev {
		return fmt.Errorf(
			"ts source %s and pin folder %s are on different filesystems; configure WALG_REDIS_TS_PIN_FOLDER on the source filesystem",
			sourceDir, pinFolder,
		)
	}
	return nil
}

func isTransient(name string) bool {
	return strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, ".lock") ||
		strings.HasSuffix(name, ".pid") || strings.HasSuffix(name, ".part")
}
