package copy

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteRcloneFileList writes source-relative paths for rclone --files-from-raw.
// Inline snapshots are represented by their existing source object paths.
func (p *Plan) WriteRcloneFileList(filename string) error {
	var list strings.Builder
	for _, entry := range p.Entries() {
		name := entry.SourcePath
		if entry.inline != nil {
			name = entry.TargetPath
			if _, ok := p.source[name]; !ok {
				return fmt.Errorf("generated object %q has no source object for rclone", name)
			}
		} else if entry.SourcePath != entry.TargetPath {
			return fmt.Errorf("rclone file lists cannot rename %q to %q", entry.SourcePath, entry.TargetPath)
		}
		if name == "" || strings.ContainsAny(name, "\r\n\x00") {
			return fmt.Errorf("object path %q cannot be represented in a rclone raw file list", name)
		}
		list.WriteString(name)
		list.WriteByte('\n')
	}

	file, err := os.CreateTemp(filepath.Dir(filename), ".walg-rclone-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	_, writeErr := file.WriteString(list.String())
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), filename)
}
