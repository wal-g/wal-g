package asm

type NopASM struct{}

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
