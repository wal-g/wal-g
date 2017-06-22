package extract

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
	"crypto/tls"
)

type Empty struct{}

type TarInterpreter interface {
	Interpret(r io.Reader, hdr *tar.Header)
}

type NOPTarInterpreter struct{}

type FileTarInterpreter struct {
	Home   string
	NewDir string
}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	fmt.Println(cur.Name)
}

func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	targetPath := ti.Home + "/" + ti.NewDir + "/" + cur.Name
	switch cur.Typeflag {
	case tar.TypeReg, tar.TypeRegA:

		f, err := os.Create(targetPath)
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(f, tr)
		if err != nil {
			panic(err)
		}

		mode := os.FileMode(cur.Mode)
		if err = os.Chmod(f.Name(), mode); err != nil {
			panic(err)
		}

		if err = f.Close(); err != nil {
			panic(err)
		}
	case tar.TypeDir:
		err := os.Mkdir(targetPath, os.FileMode(cur.Mode))
		fmt.Println(cur.Mode)
		if err != nil {
			panic(err)
		}
	case tar.TypeLink:
		if err := os.Link(cur.Name, targetPath); err != nil {
			panic(err)
		}
	case tar.TypeSymlink:
		if err := os.Symlink(cur.Name, targetPath); err != nil {
			panic(err)
		}
	}
	fmt.Println(cur.Name)
}

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

func ExtractAll(ti TarInterpreter, files []string, flag string) int {
	defer timeTrack(time.Now(), "Extract With")

	if len(files) < 1 {
		log.Fatalln("No files provided.")
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

