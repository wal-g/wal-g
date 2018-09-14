package walparser

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

//const WalFilePath = "./testdata/whole_wal_file"
const WalSwitchTestPath = "./testdata/wal_switch_test"
const PartialTestPath = "./testdata/partial_test"
const CutWALSwitchTestPath = "./testdata/cut_wal_switch_test"
const SmallPartialTestPath = "./testdata/small_partial_test"

//func TestWalFileParsing(t *testing.T) {
//	walFile, err := os.Open(WalFilePath)
//	defer walFile.Close()
//	if err != nil {
//		t.Fatalf(err.Error())
//	}
//	pageReader := WalPageReader{walFileReader: walFile}
//	parser := WalParser{}
//	for i := 0; ; i++ {
//		pageData, err := pageReader.ReadPageData()
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			t.Fatalf("error: \"%s\" at page: %v", err.Error(), i)
//		}
//		_, err = parser.ParseRecordsFromPage(bytes.NewReader(pageData))
//		if err != nil {
//			t.Fatalf("error: \"%s\" at page: %v", err.Error(), i)
//		}
//	}
//}

func TestZeroPageParsing(t *testing.T) {
	zeroPage := make([]byte, WalPageSize)
	parser := WalParser{}
	_, pageData, err := parser.ParseRecordsFromPage(bytes.NewReader(zeroPage))
	assert.Nilf(t, pageData, "not nil pageData")
	assert.Equal(t, ZeroPageError, err)
}

func doPartialFileParsingTesting(t *testing.T, pageReader WalPageReader, parser WalParser) {
	page, err := pageReader.ReadPageData()
	assert.NoError(t, err)
	_, _, err = parser.ParseRecordsFromPage(bytes.NewReader(page))
	assert.Equal(t, PartialPageError, err)
}

func doWalSwitchParsingTesting(t *testing.T, pageReader WalPageReader, parser WalParser) {
	firstPage, err := pageReader.ReadPageData() // first page contains first part of WAL-Switch record
	assert.NoError(t, err)
	_, _, err = parser.ParseRecordsFromPage(bytes.NewReader(firstPage))
	assert.NoError(t, err)

	secondPage, err := pageReader.ReadPageData() // second page contains second part of WAL-Switch record
	assert.NoError(t, err)
	_, records, err := parser.ParseRecordsFromPage(bytes.NewReader(secondPage))
	assert.NoError(t, err)
	assert.Truef(t, records[len(records)-1].isWALSwitch(), "expected WAL Switch record")
}

func parsingTestCase(t *testing.T, filename string, doTesting func(*testing.T, WalPageReader, WalParser)) {
	walFile, err := os.Open(filename)
	defer walFile.Close()
	assert.NoError(t, err)
	pageReader := WalPageReader{walFileReader: walFile}
	parser := WalParser{}

	doTesting(t, pageReader, parser)
}

func TestParsing(t *testing.T) {
	parsingTestCase(t, SmallPartialTestPath, doPartialFileParsingTesting)
	parsingTestCase(t, PartialTestPath, doPartialFileParsingTesting)
	parsingTestCase(t, CutWALSwitchTestPath, doWalSwitchParsingTesting)
	parsingTestCase(t, WalSwitchTestPath, doWalSwitchParsingTesting)
}
