package internal

import "github.com/wal-g/wal-g/internal/fsutil"

type NopASM struct {
	folder fsutil.DataFolder
}

func NewNopASM() ArchiveStatusManager {
	return NopASM{}
}

func (asm NopASM) isWalAlreadyUploaded(walFilePath string) bool {
	return false
}

func (asm NopASM) markWalUploaded(walFilePath string) error {
	return nil
}

func (asm NopASM) unmarkWalFile(walFilePath string) error {
	return nil
}
