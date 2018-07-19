package walparser

import (
	"bytes"
	"io"
	"os"
	"testing"
)

//const WalFilePath = "./testdata/00000001000000000000002B"
const WalFilePath = "./testdata/000000010000000000000078"

func TestWalFileParsing(t *testing.T) {
	walFile, err := os.Open(WalFilePath)
	defer walFile.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	pageReader := WalPageReader{walFileReader: walFile}
	parser := WalParser{}
	for i := 0; ; i++ {
		pageData, err := pageReader.ReadPageData()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("error: \"%s\" at page: %v", err.Error(), i)
		}
		_, err = parser.ParseRecordsFromPage(bytes.NewReader(pageData))
		if err != nil {
			t.Fatalf("error: \"%s\" at page: %v", err.Error(), i)
		}
	}
}
