package tools

import (
	"archive/tar"
	"fmt"
	"io"
)

type NOPTarInterpreter struct{}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) {
	fmt.Println(cur.Name)
}
