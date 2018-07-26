package testtools

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
)

// BufferTarInterpreter extracts data to a byte slice. Used
// for testing purposes.
type BufferTarInterpreter struct {
	Out []byte
}

// Interpret handles in memory tar formats. Used for testing purposes.
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

// NOPTarInterpreter mocks a tar extractor.
type NOPTarInterpreter struct{}

// Interpret does not do anything except print the
// 'tar member' name.
func (ti *NOPTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	fmt.Println(cur.Name)
	return nil
}
