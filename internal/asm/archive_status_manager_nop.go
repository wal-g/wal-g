package asm

import "github.com/wal-g/wal-g/internal/fsutil"

type NopASM struct {
	folder fsutil.DataFolder
}

func NewNopASM() ArchiveStatusManager {
	return NopASM{}
}

func (asm NopASM) IsWalAlreadyUploaded(walFilePath string) bool {
	return false
}

func (asm NopASM) MarkWalUploaded(walFilePath string) error {
	return nil
}

func (asm NopASM) UnmarkWalFile(walFilePath string) error {
	return nil
}
