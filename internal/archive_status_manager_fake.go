package internal

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

func (asm *FakeASM) isWalAlreadyUploaded(walFilePath string) bool {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	isUploaded, ok := asm.uploaded[walFilePath]
	return ok && isUploaded
}

// IsWalAlreadyUploaded is used for testing
func (asm *FakeASM) IsWalAlreadyUploaded(walFilePath string) bool {
	return asm.isWalAlreadyUploaded(walFilePath)
}

func (asm *FakeASM) markWalUploaded(walFilePath string) error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	asm.uploaded[walFilePath] = true
	return nil
}

func (asm *FakeASM) unmarkWalFile(walFilePath string) error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	asm.uploaded[walFilePath] = false
	return nil
}
