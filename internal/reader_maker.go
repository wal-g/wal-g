package internal

import "io"

type FileType string

const (
	TarFileType     FileType = "TarFileType"
	RegularFileType FileType = "RegularFileType"
)

// ReaderMaker is the generic interface used by extract. It
// allows for ease of handling different file formats.
type ReaderMaker interface {
	Reader() (io.ReadCloser, error)
	Path() string
	FileType() FileType
	Mode() int
}

func readerMakersToFilePaths(readerMakers []ReaderMaker) []string {
	paths := make([]string, 0)
	for _, readerMaker := range readerMakers {
		paths = append(paths, readerMaker.Path())
	}
	return paths
}
