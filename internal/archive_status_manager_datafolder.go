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
	return asm.folder.fileExists(walFilePath)
}

func (asm DataFolderASM) markWalUploaded(walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return asm.folder.createFile(walFilePath)
}

func (asm DataFolderASM) unmarkWalFile(walFilePath string) error {
	walFilePath = getOnlyWalName(walFilePath)
	return asm.folder.deleteFile(walFilePath)
}
