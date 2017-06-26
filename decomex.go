package extract

import (
	"crypto/tls"
	"io"
	_ "io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

type Empty struct{}

func ExtractAll(ti TarInterpreter, files []string, remote bool) int {
	defer timeTrack(time.Now(), "Extract All")

	if len(files) < 1 {
		log.Fatalln("No data provided.")
	}

	concurrency := 40
	sem := make(chan Empty, concurrency)
	tls := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{
		Transport: tls,
	}

	for i, val := range files {
		go func(i int, val string) {
			pr, pw := io.Pipe()
			go func() {
				if remote {

					get, err := http.NewRequest("GET", val, nil)
					if err != nil {
						panic(err)
					}

					data, err := client.Do(get)
					if err != nil {
						panic(err)
					}

					r := data.Body

					defer r.Close()
					decompress(pw, r)
				} else if !remote {
					r, err := os.Open(val)
					if err != nil {
						panic(err)
					}
					decompress(pw, r)
				} else {
					log.Fatalln("Flag")
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
