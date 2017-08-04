package walg

import (
	"archive/tar"
	"github.com/pkg/errors"
	"io"
	//"time"
)

/**
 *  Extract exactly one tar bundle.
 */
func extractOne(ti TarInterpreter, s io.Reader) error {
	tr := tar.NewReader(s)

	for {
		cur, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "extractOne: tar extract failed")
		}

		err = ti.Interpret(tr, cur)
		if err != nil {
			return errors.Wrap(err, "extractOne: Interpret failed")
		}
	}
	return nil

}

/**
 *  Handles all files passed in. Supports `.lzo`, `.lz4, and `.tar`.
 *  File type `nop` is used for testing purposes.
 */
func ExtractAll(ti TarInterpreter, files []ReaderMaker) error {
	//defer TimeTrack(time.Now(), "EXTRACT ALL")
	if len(files) < 1 {
		return errors.New("ExtractAll: did not provide files to extract")
	}

	var err error

	collect := make(chan error, 1)
	sem := make(chan Empty, len(files))

	for i, val := range files {
		go func(i int, val ReaderMaker) {
			var e error
			pr, pw := io.Pipe()

			go func() {
				r, err := val.Reader()
				if err != nil {
					panic(err)
				}
				defer r.Close()
				defer pw.Close()
				if val.Format() == "lzo" {
					e = DecompressLzo(pw, r)
					if e != nil {
						collect <- errors.Wrap(e, "ExtractAll: lzo decompress failed")
					}
				} else if val.Format() == "lz4" {
					e = DecompressLz4(pw, r)
					if e != nil {
						collect <- errors.Wrap(e, "ExtractAll: lz4 decompress failed")
					}
				} else if val.Format() == "tar" {
					_, e = io.Copy(pw, r)
					if e != nil {
						collect <- errors.Wrap(e, "ExtractAll: tar extract failed")
					}
				} else if val.Format() == "nop" {
				} else {
					e = UnsupportedFileTypeError{val.Path(), val.Format()}
					collect <- e
				}

			}()

			if len(collect) == 0 {
				e = extractOne(ti, pr)
				if e != nil {
					collect <- e
				}
			}

			sem <- Empty{}

		}(i, val)
	}

	for i := 0; i < len(files); i++ {
		<-sem
	}

	select {
	case err := <-collect:
		return err
	default:
		return err
	}

}
