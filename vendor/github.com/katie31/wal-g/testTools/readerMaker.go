package tools

import (
	"io"
	"net/http"
	"os"
)

// HTTPReaderMaker decompresses lzop tarballs from
// the passed in url.
type HTTPReaderMaker struct {
	Client     *http.Client
	Key        string
	FileFormat string
}

func (h *HTTPReaderMaker) Format() string { return h.FileFormat }
func (h *HTTPReaderMaker) Path() string   { return h.Key }

// FileReaderMaker decompresses lzop tarballs from
// the passed in file.
type FileReaderMaker struct {
	Key        string
	FileFormat string
}

func (f *FileReaderMaker) Format() string { return f.FileFormat }
func (f *FileReaderMaker) Path() string   { return f.Key }

// Reader creates a new request to grab the data generated
// by the random bytes generator.
func (h *HTTPReaderMaker) Reader() (io.ReadCloser, error) {
	get, err := http.NewRequest("GET", h.Key, nil)
	if err != nil {
		return nil, err
	}

	data, err := h.Client.Do(get)
	if err != nil {
		return nil, err
	}

	return data.Body, nil
}

// Reader creates a new reader from the passed in file.
func (f *FileReaderMaker) Reader() (io.ReadCloser, error) {
	r, err := os.Open(f.Key)
	if err != nil {
		return nil, err
	}

	return r, nil
}
