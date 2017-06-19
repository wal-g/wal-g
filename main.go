package main

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"
	"github.com/rasky/go-lzo"
	//"github.com/MediaMath/go-lzop"
	//"github.com/dgryski/go-lzo"
	_ "bytes"
	"encoding/binary"
	_ "io/ioutil"
)

const MAX_BLOCK_SIZE = 64 * 1024 * 1024

type Empty struct{}

type TarInterpreter interface {
	Interpret(tr io.Reader, cur *tar.Header, home, newDir string)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func makeDir(home, name string) {
	dest := home + "/" + name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, 0777); err != nil {
			panic(err)
		}
	}
}


func decompress(home, file string) (name string) {
	path := home + "/" + file
	skip := 33
	s, err := os.Open(path)

	if err != nil {
		panic(err)
	}

	_, err = s.Seek(int64(skip), 0)
	var fileNameLen uint8

	binary.Read(s, binary.BigEndian, &fileNameLen)

	fileName := make([]byte, fileNameLen)

	_, err = s.Read(fileName)
	fmt.Println("fileName", string(fileName))
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

	f, err := os.Create(string(fileName))
	if err != nil {
		panic(err)
	}

	for {

		err = binary.Read(s, binary.BigEndian, &uncom)

		if uncom == 0 {
			break
		}

		if err != nil {
			panic(err)
		}

		log.Printf("uncom: %d\n", uncom)

		err = binary.Read(s, binary.BigEndian, &com)

		if err != nil {
			panic(err)
		}

		log.Printf("com: %d\n", com)

		err = binary.Read(s, binary.BigEndian, &check)
		if err != nil {
			panic(err)
		}

		if uncom <= com {
			//io.CopyN(os.Stdout, s, int64(com))
			io.CopyN(f, s, int64(com))

		} else {
			out, err := lzo.Decompress1X(s, int(com), int(uncom))
			if err != nil {
				panic(err)
			}

			_, err = f.Write(out)
			if err != nil {
				panic(err)
			}
		}
	}
	return string(fileName)
}

func decom_ex(ti TarInterpreter, home, newDir, file string) {
	tar := decompress(home, file)
	fmt.Println(tar)
	ExtractOne(ti, home, newDir, tar)
}

type NOPTarInterpreter struct {

}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header, home, newDir string) {
	fmt.Println(cur.Name)
}

type FileTarInterpreter struct {
}

func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header, home, newDir string) {
	target := home + "/" + newDir + "/" + cur.Name
	switch cur.Typeflag {
	case tar.TypeReg, tar.TypeRegA:

		f, err := os.Create(target)
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
		err := os.Mkdir(target, os.FileMode(cur.Mode))
		if err != nil {
			panic(err)
		}
	case tar.TypeLink:
		if err := os.Link(cur.Name, target); err != nil {
			panic(err)
		}
	case tar.TypeSymlink:
		if err := os.Symlink(cur.Name, target); err != nil {
			panic(err)
		}
	}
	fmt.Println(cur.Name)
}

func ExtractOne(ti TarInterpreter, home, newDir, file string) {
	//s, err := os.Open(home + "/" + file)
	s, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	tr := tar.NewReader(s)

	makeDir(home, newDir) //permission 0777

	for {
		cur, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		ti.Interpret(tr, cur, home, newDir)
	}

}

func ExtractAll(ti TarInterpreter, newDir string, files []string) int {
	defer timeTrack(time.Now(), "Extract With")
	home := os.Getenv("HOME")

	if len(files) < 1 {
		log.Fatalln("No files provided.")
	}

	makeDir(home, newDir)

	sem := make(chan Empty, len(files))

	for i, val := range files {
		//extract_one(home, newDir, val)
		go func(i int, val string) {
			decom_ex(ti, home, newDir, val)
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
	files := all[2:]
	newDir := all[1]

	// r := compress(os.Getenv("HOME"), os.Args[1])
	// decompress(r)
	//decompress(os.Getenv("HOME"), os.Args[1])
	//decom_ex(os.Getenv("HOME"), os.Args[1], os.Args[2])

	//ExtractAll(newDir, files)
	fmt.Println("Go Routines: ", ExtractAll(&NOPTarInterpreter{}, newDir, files))
	//ExtractWithout(os.Args)

}
