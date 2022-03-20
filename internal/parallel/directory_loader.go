package parallel

type DirectoryLoader interface {
	DownloadDirectory(pathToRestore string) error
}
