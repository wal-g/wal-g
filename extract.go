package walg

import (
	"archive/tar"
	"io"
	"log"
	//"time"
)

/**
 *  Extract exactly one tar bundle.
 */
func extractOne(ti TarInterpreter, s io.Reader) {
	tr := tar.NewReader(s)

	for {
		cur, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		ti.Interpret(tr, cur)
	}

}

/**
 *  Handles all files passed in. Supports `.lzo`, `.lz4, and `.tar`.
 */
func ExtractAll(ti TarInterpreter, files []ReaderMaker) error {
	//defer TimeTrack(time.Now(), "EXTRACT ALL")
	if len(files) < 1 {
		log.Fatalln("No data provided.")
	}

	var err error
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
				} else if val.Format() == "tar" {
					io.Copy(pw, r)
				} else {
					err = UnsupportedFileTypeError{val.Path(), val.Format()}
				}

				defer pw.Close()
			}()

			if err == nil {
				extractOne(ti, pr)
			}
			sem <- Empty{}

		}(i, val)
	}

	//num := runtime.NumGoroutine()

	for i := 0; i < len(files); i++ {
		<-sem
	}
	return err
}
