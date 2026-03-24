package utils

import (
	"os"
)

func setOwner(_ os.FileInfo, _ string, _ string) error {
	// windows does not have this concept so continue
	return nil
}
