package internal

type DataFolderASM struct {
	folder DataFolder
}

func NewDataFolderASM(folder DataFolder) DataFolderASM {
	return DataFolderASM{
		folder: folder,
	}
}

func (asm DataFolderASM) isWalAlreadyUploaded(walFilePath string) bool {
	walFilePath = getOnlyWalName(walFilePath)
	return asm.folder.FileExists(walFilePath)
}

func (asm DataFolderASM) markWalUploaded(walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return asm.folder.CreateFile(walFilePath)
}

func (asm DataFolderASM) unmarkWalFile(walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return asm.folder.DeleteFile(walFilePath)
}
