package internal_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/memory"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/utility"
	"sort"
	"strings"
	"testing"
)

// MockWalShowOutputWriter is used to capture wal-show command output
type MockWalShowOutputWriter struct {
	timelineInfos []*internal.TimelineInfo
}

func (writer *MockWalShowOutputWriter) Write(timelineInfos []*internal.TimelineInfo) error {
	// append timeline infos in case future implementations will call the Write() multiple times
	writer.timelineInfos = append(writer.timelineInfos, timelineInfos...)
	return nil
}

// TestTimelineSetup holds test setup information about single timeline
type TestTimelineSetup struct {
	existSegments       []string
	missingSegments     []string
	id                  uint32
	parentId            uint32
	switchPointLsn      uint64
	historyFileContents string
}

// GetWalFilenames returns slice of existing wal segments filenames
func (timelineSetup *TestTimelineSetup) GetWalFilenames() []string {
	walFileSuffix := "." + lz4.FileExtension
	filenamesWithExtension := make([]string, 0, len(timelineSetup.existSegments))
	for _, name := range timelineSetup.existSegments {
		filenamesWithExtension = append(filenamesWithExtension, name+walFileSuffix)
	}
	return filenamesWithExtension
}

// GetHistory returns .history file name and compressed contents
func (timelineSetup *TestTimelineSetup) GetHistory() (string, *bytes.Buffer, error) {
	compressor := compression.Compressors[lz4.AlgorithmName]
	var compressedData bytes.Buffer
	compressingWriter := compressor.NewWriter(&compressedData)
	_, err := utility.FastCopy(compressingWriter, strings.NewReader(timelineSetup.historyFileContents))
	if err != nil {
		return "", nil, err
	}
	err = compressingWriter.Close()
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("%08X.history."+lz4.FileExtension, timelineSetup.id), &compressedData, nil
}

// TestWalShow test series is used to test the HandleWalShow() functionality

func TestWalShow_NoSegmentsInStorage(t *testing.T) {
	timelineInfos := executeWalShow([]string{}, make(map[string]*bytes.Buffer))
	assert.Empty(t, timelineInfos)
}

func TestWalShow_NoMissingSegments(t *testing.T) {
	timelineSetup := &TestTimelineSetup{
		existSegments: []string{
			"000000010000000000000090",
			"000000010000000000000091",
			"000000010000000000000092",
			"000000010000000000000093",
		},
		missingSegments: make([]string, 0),
		id:              1,
	}
	testSingleTimeline(t, timelineSetup, make(map[string]*bytes.Buffer))
}

func TestWalShow_OneSegmentMissing(t *testing.T) {
	timelineSetup := &TestTimelineSetup{
		existSegments: []string{
			"000000010000000000000090",
			"000000010000000000000092",
			"000000010000000000000093",
			"000000010000000000000094",
		},
		missingSegments: []string{
			"000000010000000000000091",
		},
		id: 1,
	}
	testSingleTimeline(t, timelineSetup, make(map[string]*bytes.Buffer))
}

func TestWalShow_MultipleSegmentsMissing(t *testing.T) {
	timelineSetup := &TestTimelineSetup{
		existSegments: []string{
			"000000010000000000000090",
			"000000010000000000000092",
			"000000010000000000000093",
			"000000010000000000000095",
		},
		missingSegments: []string{
			"000000010000000000000091",
			"000000010000000000000094",
		},
		id: 1,
	}
	testSingleTimeline(t, timelineSetup, make(map[string]*bytes.Buffer))
}

func TestWalShow_SingleTimelineWithHistory(t *testing.T) {
	timelineSetup := &TestTimelineSetup{
		existSegments: []string{
			"000000020000000000000090",
			"000000020000000000000091",
			"000000020000000000000092",
			"000000020000000000000093",
		},
		missingSegments: make([]string, 0),
		id:              2,
		// parentId and switch point LSN match the .history file record
		parentId: 1,
		// 2420113408 is 0x90400000 (hex)
		switchPointLsn:      2420113408,
		historyFileContents: "1\t0/90400000\tbefore 2000-01-01 05:00:00+05\n\n",
	}

	historyFileName, historyContents, err := timelineSetup.GetHistory()
	assert.NoError(t, err)

	testSingleTimeline(t, timelineSetup, map[string]*bytes.Buffer{historyFileName: historyContents})
}

func TestWalShow_TwoTimelinesWithHistory(t *testing.T) {
	timelineSetups := []*TestTimelineSetup{
		{
			existSegments: []string{
				"00000001000000000000008F",
				"000000010000000000000090",
				"000000010000000000000091",
				"000000010000000000000092",
			},
			missingSegments: make([]string, 0),
			id:              1,
		},
		{
			existSegments: []string{
				"000000020000000000000090",
				"000000020000000000000091",
				"000000020000000000000092",
			},
			missingSegments: make([]string, 0),
			id:              2,
			// parentId and switch point LSN match the .history file record
			parentId: 1,
			// 2420113408 is 0x90400000 (hex)
			switchPointLsn:      2420113408,
			historyFileContents: "1\t0/90400000\tbefore 2000-01-01 05:00:00+05\n\n",
		},
	}

	historyFileName, historyContents, err := timelineSetups[1].GetHistory()
	assert.NoError(t, err)

	testMultipleTimelines(t, timelineSetups, map[string]*bytes.Buffer{
		historyFileName: historyContents,
	})
}

func TestWalShow_MultipleTimelines(t *testing.T) {
	timelineSetups := []*TestTimelineSetup{
		// first timeline
		{
			existSegments: []string{
				"000000010000000000000090",
				"000000010000000000000091",
				"000000010000000000000092",
				"000000010000000000000093",
			},
			id: 1,
		},
		// second timeline
		{
			existSegments: []string{
				"000000020000000000000091",
				"000000020000000000000092",
			},
			id: 2,
		},
	}
	testMultipleTimelines(t, timelineSetups, make(map[string]*bytes.Buffer))
}

// testSingleTimeline is used to test wal-show with only one timeline in WAL storage
func testSingleTimeline(t *testing.T, setup *TestTimelineSetup, walFolderFiles map[string]*bytes.Buffer) {
	timelines := executeWalShow(setup.GetWalFilenames(), walFolderFiles)
	assert.Len(t, timelines, 1)

	verifySingleTimeline(t, setup, timelines[0])
}

// testMultipleTimelines is used to test wal-show in case of multiple timelines in WAL storage
func testMultipleTimelines(t *testing.T, timelineSetups []*TestTimelineSetup, walFolderFiles map[string]*bytes.Buffer) {
	walFilenames := concatWalFilenames(timelineSetups)
	timelineInfos := executeWalShow(walFilenames, walFolderFiles)

	sort.Slice(timelineInfos, func(i, j int) bool {
		return timelineInfos[i].Id < timelineInfos[j].Id
	})
	sort.Slice(timelineSetups, func(i, j int) bool {
		return timelineSetups[i].id < timelineSetups[j].id
	})

	assert.Len(t, timelineInfos, len(timelineSetups))

	for idx, info := range timelineInfos {
		verifySingleTimeline(t, timelineSetups[idx], info)
	}
}

// verifySingleTimeline checks that setup values for timeline matches the output timeline info values
func verifySingleTimeline(t *testing.T, setup *TestTimelineSetup, timelineInfo *internal.TimelineInfo) {
	// sort setup.existSegments to pick the correct start and end segment
	sort.Slice(setup.existSegments, func(i, j int) bool {
		return setup.existSegments[i] < setup.existSegments[j]
	})

	expectedStatus := internal.TimelineOkStatus
	if len(setup.missingSegments) > 0 {
		expectedStatus = internal.TimelineLostSegmentStatus
	}

	expectedTimelineInfo := internal.TimelineInfo{
		Id:               setup.id,
		ParentId:         setup.parentId,
		SwitchPointLsn:   setup.switchPointLsn,
		StartSegment:     setup.existSegments[0],
		EndSegment:       setup.existSegments[len(setup.existSegments)-1],
		SegmentsCount:    len(setup.existSegments),
		MissingSegments:  setup.missingSegments,
		SegmentRangeSize: uint64(len(setup.existSegments) + len(setup.missingSegments)),
		Status:           expectedStatus,
	}

	// check that found missing segments matches with setup values
	assert.ElementsMatch(t, expectedTimelineInfo.MissingSegments, timelineInfo.MissingSegments)

	// avoid equality errors (we ignore missing segments order and we've checked that MissingSegments match before)
	expectedTimelineInfo.MissingSegments = timelineInfo.MissingSegments
	assert.Equal(t, expectedTimelineInfo, *timelineInfo)
}

// executeWalShow invokes the HandleWalShow() with fake storage filled with
// provided wal segments and other wal folder files
func executeWalShow(walFilenames []string, walFolderFiles map[string]*bytes.Buffer) []*internal.TimelineInfo {
	for _, name := range walFilenames {
		// we don't use the WAL file contents so let it be it empty inside
		walFolderFiles[name] = new(bytes.Buffer)
	}
	folder := setupTestStorageFolder(walFolderFiles)
	mockOutputWriter := &MockWalShowOutputWriter{}

	internal.HandleWalShow(folder, false, mockOutputWriter)

	return mockOutputWriter.timelineInfos
}

func setupTestStorageFolder(walFolderFiles map[string]*bytes.Buffer) storage.Folder {
	memoryStorage := memory.NewStorage()
	for name, content := range walFolderFiles {
		memoryStorage.Store("in_memory/wal_005/"+name, *content)
	}

	return memory.NewFolder("in_memory/", memoryStorage)
}

func concatWalFilenames(timelineSetups []*TestTimelineSetup) []string {
	filenames := make([]string, 0)
	for _, timelineSetup := range timelineSetups {
		filenames = append(filenames, timelineSetup.GetWalFilenames()...)
	}
	return filenames
}
