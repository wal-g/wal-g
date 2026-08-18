//go:build windows

package pin

import (
	"fmt"
	"os"
	"path/filepath"
)

func pinAttemptDiagnostics(sourcePath, pinnedPath string) string {
	return fmt.Sprintf(
		"source=%q source_stat={%s} destination=%q destination_parent_stat={%s}",
		sourcePath,
		fileDiagnostics(sourcePath),
		pinnedPath,
		fileDiagnostics(filepath.Dir(pinnedPath)),
	)
}

func fileDiagnostics(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Sprintf("stat_error=%v", err)
	}
	return fmt.Sprintf("mode=%s size=%d", info.Mode(), info.Size())
}
