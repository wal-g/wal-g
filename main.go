package main

import (
	"archive/tar"
	_ "bytes"
	"encoding/binary"
	"fmt"
	"github.com/rasky/go-lzo"
	"io"
	_ "io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

const MAX_BLOCK_SIZE = 64 * 1024 * 1024

var HOME string = os.Getenv("HOME")

type Empty struct{}

type TarInterpreter interface {
	Interpret(r io.Reader, hdr *tar.Header)
}

type NOPTarInterpreter struct{}

type FileTarInterpreter struct {
	home   string
	newDir string
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func makeDir(home, name string) {
	dest := home + "/" + name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, 0700); err != nil {
			panic(err)
		}
	}
}

func decompress(w io.Writer, s io.Reader) {
	var skip int = 33

	sk := make([]byte, skip)
	_, err := s.Read(sk)
	if err != nil {
		panic(err)
	}

	var fileNameLen uint8

	binary.Read(s, binary.BigEndian, &fileNameLen)
	
	fileName := make([]byte, fileNameLen)
	_, err = s.Read(fileName)
	if err != nil {
		panic(err)
	}

	fileComment := make([]byte, 4)
	_, err = s.Read(fileComment)
	if err != nil {
		panic(err)
	}

	var uncom uint32
	var com uint32
	var check uint32

	for {

		err = binary.Read(s, binary.BigEndian, &uncom)
		if uncom == 0 {
			break
		}
		if err != nil {
			panic(err)
		}

		err = binary.Read(s, binary.BigEndian, &com)
		if err != nil {
			panic(err)
		}

		err = binary.Read(s, binary.BigEndian, &check)
		if err != nil {
			panic(err)
		}

		if uncom <= com {
			io.CopyN(w, s, int64(com))

		} else {
			out, err := lzo.Decompress1X(s, int(com), int(uncom))
			if err != nil {
				panic(err)
			}

			_, err = w.Write(out)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	fmt.Println(cur.Name)
}

func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	targetPath := ti.home + "/" + ti.newDir + "/" + cur.Name
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

func ExtractOne(ti TarInterpreter, s io.Reader) {
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
					data, err := http.Get(val)
					if err != nil {
						panic(err)
					}

					r := data.Body
					defer r.Close()
					decompress(pw, r)
				} else if flag == "-f" {
					r, err := os.Open(HOME + "/" + val)
					if err != nil {
						panic(err)
					}
					decompress(pw, r)
				} else {
					log.Fataln("Flag")
				}
				defer pw.Close()
			}()

			ExtractOne(ti, pr)
			sem <- Empty{}
		}(i, val)
	}
	num := runtime.NumGoroutine()
	for i := 0; i < len(files); i++ {
		<-sem
	}
	return num
}

func main() {
	all := os.Args
	flag := all[1]
	dir := all[2]
	f := all[3:]

	ft := FileTarInterpreter{
		home:   HOME,
		newDir: dir,
	}
	makeDir(ft.home, ft.newDir)

	np := NOPTarInterpreter{}

	fmt.Println("NOP Go Routines: ", ExtractAll(&np, f, flag))
	fmt.Println("File Go Routines: ", ExtractAll(&ft, f, flag))

}
