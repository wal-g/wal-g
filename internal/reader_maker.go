package internal

import "io"

// ReaderMaker is the generic interface used by extract. It
// allows for ease of handling different file formats.
type ReaderMaker interface {
	Reader() (io.ReadCloser, error)
	Path() string
}

func readerMakersToFilePaths(readerMakers []ReaderMaker) []string {
	paths := make([]string, 0)
	for _, readerMaker := range readerMakers {
		paths = append(paths, readerMaker.Path())
	}
	return paths
}
