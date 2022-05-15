package parallel

type DirectoryDownloader interface {
	DownloadDirectory(pathToRestore string) error
}
