package ao

import (
	"fmt"
	"os"
)

// ZeroFillFileToEOF preserves the existing file prefix and pads or truncates
// the file to the logical EOF recorded in backup metadata. Extending a file
// with Truncate reads as zeroes without allocating a buffer proportional to
// the file size.
func ZeroFillFileToEOF(filePath string, logicalEOF int64, mode os.FileMode, fsync bool) error {
	if logicalEOF < 0 {
		return fmt.Errorf("invalid AO logical EOF %d", logicalEOF)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, mode)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(logicalEOF); err != nil {
		return err
	}
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if fsync {
		return file.Sync()
	}
	return nil
}
