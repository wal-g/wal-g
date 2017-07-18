package walg

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

type ReaderMaker interface {
	Reader() io.ReadCloser
	Format() string
}

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

func ExtractAll(ti TarInterpreter, files []ReaderMaker) int {
	defer TimeTrack(time.Now(), "EXTRACT ALL")

	if len(files) < 1 {
		log.Fatalln("No data provided.")
	}

	concurrency := 40
	sem := make(chan Empty, concurrency)

	for i, val := range files {
		go func(i int, val ReaderMaker) {
			pr, pw := io.Pipe()
			go func() {
				r := val.Reader()
				defer r.Close()
				if val.Format() == "lzo" {
					DecompressLzo(pw, r)
				} else if val.Format() == "lz4" {
					DecompressLz4(pw, r)
				} else {
					fmt.Printf("Invalid file type '%s'\n", val.Format())
					os.Exit(1)
				}

				defer pw.Close()
			}()

			extractOne(ti, pr)
			sem <- Empty{}
		}(i, val)
	}

	num := runtime.NumGoroutine()
	for i := 0; i < len(files); i++ {
		<-sem
	}
	return num
}
