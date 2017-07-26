package walg

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	//"time"
)

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

func ExtractAll(ti TarInterpreter, files []ReaderMaker) int {
	//defer TimeTrack(time.Now(), "EXTRACT ALL")

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
