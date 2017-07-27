package tools

import (
	"io"
	"net/http"
	"os"
)

type HttpReaderMaker struct {
	Client     *http.Client
	Path       string
	FileFormat string
}

func (h *HttpReaderMaker) Format() string { return h.FileFormat }

type FileReaderMaker struct {
	Path       string
	FileFormat string
}

func (f *FileReaderMaker) Format() string { return f.FileFormat }

func (h *HttpReaderMaker) Reader() io.ReadCloser {
	get, err := http.NewRequest("GET", h.Path, nil)
	if err != nil {
		panic(err)
	}

	data, err := h.Client.Do(get)
	if err != nil {
		panic(err)
	}

	return data.Body
}

func (f *FileReaderMaker) Reader() io.ReadCloser {
	r, err := os.Open(f.Path)
	if err != nil {
		panic(err)
	}

	return r
}
