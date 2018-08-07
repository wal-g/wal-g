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
func (tarInterpreter *BufferTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	//defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	//Assumes only regular files
	out, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "Interpret: ReadAll failed")
	}
	tarInterpreter.Out = out
	return nil
}

// NOPTarInterpreter mocks a tar extractor.
type NOPTarInterpreter struct{}

// Interpret does not do anything except print the
// 'tar member' name.
func (tarInterpreter *NOPTarInterpreter) Interpret(tr io.Reader, header *tar.Header) error {
	fmt.Println(header.Name)
	return nil
}
