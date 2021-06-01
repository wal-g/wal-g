package testtools

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/pkg/errors"
)

// BufferTarInterpreter extracts data to a byte slice. Used
// for testing purposes.
type BufferTarInterpreter struct {
	Out []byte
}

// Interpret handles in memory tar formats. Used for testing purposes.
func (tarInterpreter *BufferTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	// defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	// Assumes only regular files
	out, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "Interpret: ReadAll failed")
	}
	tarInterpreter.Out = out
	return nil
}

// Extracts data (possibly from multiple sources concurrently) and stores it in dictionary.
// Used for testing purposes only.
type ConcurrentConcatBufferTarInterpreter struct {
	Out map[string][]byte
	mu sync.Mutex
}

func NewConcurrentConcatBufferTarInterpreter() *ConcurrentConcatBufferTarInterpreter{
	ccbti := new(ConcurrentConcatBufferTarInterpreter)
	ccbti.Out = make(map[string][]byte)
	return ccbti
}

// Interpret handles in memory tar formats. Used for testing purposes.
func (tarInterpreter *ConcurrentConcatBufferTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	// defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	// Assumes only regular files
	out, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "Interpret: ReadAll failed")
	}

	tarInterpreter.mu.Lock()
	tarInterpreter.Out[header.Name] = out
	tarInterpreter.mu.Unlock()


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
