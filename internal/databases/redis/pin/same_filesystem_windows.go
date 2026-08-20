//go:build windows

package pin

import (
	"io/fs"
	"path/filepath"
	"strings"
)

func sameFilesystem(sourceDir, pinFolder string, _ fs.FileInfo, _ fs.FileInfo) bool {
	return strings.EqualFold(filepath.VolumeName(sourceDir), filepath.VolumeName(pinFolder))
}
