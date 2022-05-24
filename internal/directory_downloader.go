package internal

type DirectoryDownloader interface {
	DownloadDirectory(pathToRestore string) error
}
