package walparser

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/utility"
)

const (
	WalSwitchTestPath    = "./testdata/wal_switch_test"
	PartialTestPath      = "./testdata/partial_test"
	CutWALSwitchTestPath = "./testdata/cut_wal_switch_test"
	SmallPartialTestPath = "./testdata/small_partial_test"
	LongRecordTestPath   = "./testdata/long_record"
)

func TestZeroPageParsing(t *testing.T) {
	zeroPage := make([]byte, WalPageSize)
	parser := NewWalParser()
	_, pageData, err := parser.ParseRecordsFromPage(bytes.NewReader(zeroPage))
	assert.Nilf(t, pageData, "not nil pageData")
	assert.IsType(t, err, ZeroPageError{})
}

func doPartialFileParsingTesting(t *testing.T, pageReader WalPageReader, parser WalParser) {
	page, err := pageReader.ReadPageData()
	assert.NoError(t, err)
	_, _, err = parser.ParseRecordsFromPage(bytes.NewReader(page))
	assert.IsType(t, err, PartialPageError{})
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

func doLongRecordParsingTesting(t *testing.T, pageReader WalPageReader, parser WalParser) {
	firstPage, err := pageReader.ReadPageData() // first page contains a beginning of the long record
	assert.NoError(t, err)
	_, _, err = parser.ParseRecordsFromPage(bytes.NewReader(firstPage))
	assert.NoError(t, err)
	assert.NotEmpty(t, parser.currentRecordData)
	assert.True(t, parser.hasCurrentRecordBeginning)

	secondPage, err := pageReader.ReadPageData() // second page consists only of the long record
	assert.NoError(t, err)
	discarded, _, err := parser.ParseRecordsFromPage(bytes.NewReader(secondPage))
	assert.NoError(t, err)
	assert.Nil(t, discarded)
	assert.NotEmpty(t, parser.currentRecordData)
	assert.True(t, parser.hasCurrentRecordBeginning)

	thirdPage, err := pageReader.ReadPageData() // third page starts with long record tail
	assert.NoError(t, err)
	discarded, records, err := parser.ParseRecordsFromPage(bytes.NewReader(thirdPage))
	assert.NoError(t, err)
	assert.Nil(t, discarded)
	assert.NotEmpty(t, records)
}

func parsingTestCase(t *testing.T, filename string, doTesting func(*testing.T, WalPageReader, WalParser)) {
	walFile, err := os.Open(filename)
	defer utility.LoggedClose(walFile, "")
	assert.NoError(t, err)
	pageReader := WalPageReader{walFileReader: walFile}
	parser := NewWalParser()

	doTesting(t, pageReader, *parser)
}

func TestParsing_SmallPartial(t *testing.T) {
	parsingTestCase(t, SmallPartialTestPath, doPartialFileParsingTesting)
}

func TestParsing_Partial(t *testing.T) {
	parsingTestCase(t, PartialTestPath, doPartialFileParsingTesting)
}

func TestParsing_WalSwitch(t *testing.T) {
	parsingTestCase(t, WalSwitchTestPath, doWalSwitchParsingTesting)
}

func TestParsing_CutWALSwitch(t *testing.T) {
	parsingTestCase(t, CutWALSwitchTestPath, doWalSwitchParsingTesting)
}

func TestParsing_LongRecord(t *testing.T) {
	parsingTestCase(t, LongRecordTestPath, doLongRecordParsingTesting)
}

func TestSaveLoadWalParser(t *testing.T) {
	walParser := LoadWalParserFromCurrentRecordHead([]byte{1, 2, 3, 4, 5, 6})

	var walParserData bytes.Buffer
	err := walParser.Save(&walParserData)
	assert.NoError(t, err)

	loadedWalParser, err := LoadWalParser(&walParserData)
	assert.NoError(t, err)

	assert.Equal(t, walParser, loadedWalParser)
}
