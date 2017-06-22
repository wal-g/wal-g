package extract

import (
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

type Empty struct{}

func ExtractAll(ti TarInterpreter, files []string, flag string) int {
	defer timeTrack(time.Now(), "Extract All")

	if len(files) < 1 {
		log.Fatalln("No data provided.")
	}

	sem := make(chan Empty, len(files))

	for i, val := range files {
		go func(i int, val string) {
			pr, pw := io.Pipe()
			go func() {
				if flag == "-d" {
					tls := &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					}

					client := &http.Client{
						Transport: tls,
					}

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
				} else if flag == "-f" {
					r, err := os.Open(os.Getenv("Home") + "/" + val)
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
