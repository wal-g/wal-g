package tools

import (
	"archive/tar"
	"github.com/pkg/errors"
	"fmt"
	"io"
	"io/ioutil"
)

/**
 *  Tar interpreter that extracts to a byte slice. Used
 *  for testing purposes.
 */
type BufferTarInterpreter struct {
	Out []byte
}

/**
 *  Handles in memory tar formats. Used for testing purposes.
 */
func (ti *BufferTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	//defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	//Assumes only regular files
	out, err := ioutil.ReadAll(tr)
	if err != nil {
		return errors.Wrap(err, "Interpret: ReadAll failed")
	}
	ti.Out = out
	return nil
}

type NOPTarInterpreter struct{}

func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	fmt.Println(cur.Name)
	return nil
}
