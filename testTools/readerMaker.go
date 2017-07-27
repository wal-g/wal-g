package tools

import (
	"io"
	"net/http"
	"os"
)

type HttpReaderMaker struct {
	Client     *http.Client
	Key        string
	FileFormat string
}

func (h *HttpReaderMaker) Format() string { return h.FileFormat }
func (h *HttpReaderMaker) Path() string   { return h.Key }

type FileReaderMaker struct {
	Key        string
	FileFormat string
}

func (f *FileReaderMaker) Format() string { return f.FileFormat }
func (f *FileReaderMaker) Path() string   { return f.Key }

func (h *HttpReaderMaker) Reader() io.ReadCloser {
	get, err := http.NewRequest("GET", h.Key, nil)
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
	r, err := os.Open(f.Key)
	if err != nil {
		panic(err)
	}

	return r
}
