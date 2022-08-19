package asm

import "sync"

type FakeASM struct {
	uploaded map[string]bool
	mutex    sync.Mutex
}

var _ ArchiveStatusManager = &FakeASM{}

func NewFakeASM() *FakeASM {
	return &FakeASM{
		uploaded: make(map[string]bool),
	}
}

func (asm *FakeASM) IsWalAlreadyUploaded(walFilePath string) bool {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	isUploaded, ok := asm.uploaded[walFilePath]
	return ok && isUploaded
}

// WalAlreadyUploaded is used for testing
func (asm *FakeASM) WalAlreadyUploaded(walFilePath string) bool {
	return asm.IsWalAlreadyUploaded(walFilePath)
}

func (asm *FakeASM) MarkWalUploaded(walFilePath string) error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	asm.uploaded[walFilePath] = true
	return nil
}

func (asm *FakeASM) UnmarkWalFile(walFilePath string) error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	asm.uploaded[walFilePath] = false
	return nil
}

func (asm *FakeASM) RenameReady(walFilePath string) error {
	return nil
}
