package extract

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type TarInterpreter interface {
	Interpret(r io.Reader, hdr *tar.Header)
}

type NOPTarInterpreter struct{}

type FileTarInterpreter struct {
	NewDir string
}

type BufferTarInterpreter struct {
	Out []byte
}

func (ti *BufferTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	//Assumes only regualr files
	out, err := ioutil.ReadAll(tr)
	if err != nil {
		panic(err)
	}
	ti.Out = out
}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	fmt.Println(cur.Name)
}

func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	targetPath := ti.NewDir + "/" + cur.Name
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
