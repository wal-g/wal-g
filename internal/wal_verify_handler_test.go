package internal_test

import (
	"bytes"
	"fmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"reflect"
	"testing"
)

func init() {
	// Set upload disk concurrency to non-zero value
	// Please note: this setting affects wal-verify behavior
	viper.Set(internal.UploadConcurrencySetting, "4")
}

type WalVerifyTestSetup struct {
	expectedIntegrityCheck internal.WalVerifyCheckResult
	expectedTimelineCheck internal.WalVerifyCheckResult

	// currentWalSegment represents the current cluster wal segment
	currentWalSegment internal.WalSegmentDescription
	// list of mock storage wal folder WAL segments
	storageSegments   []string
	// list of other mock storage files
	storageFiles      map[string]*bytes.Buffer
}

// MockWalVerifyOutputWriter is used to capture wal-verify command output
type MockWalVerifyOutputWriter struct {
	lastResult map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult
	// number of time Write() function has been called
	writeCallsCount int
}

func (writer *MockWalVerifyOutputWriter) Write(
	result map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult) error {
	writer.lastResult = result
	writer.writeCallsCount += 1
	return nil
}

// test that wal-verify works correctly on empty storage
func TestWalVerify_EmptyStorage(t *testing.T) {
	currentSegmentName := "00000003000000000000000A"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	storageFiles := make(map[string]*bytes.Buffer, 0)
	storageSegments := make([]string, 0, 0)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    3,
				StartSegment:  "000000030000000000000001",
				EndSegment:    "000000030000000000000009",
				SegmentsCount: 9,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: 0,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageFiles:           storageFiles,
		storageSegments:        storageSegments,
	})
}

// check that storage garbage doesn't affect the wal-verify command
func TestWalVerify_OnlyGarbageInStorage(t *testing.T) {
	storageSegments := []string{
		"00000007000000000000000K",
		"0000000Y000000000000000K",
	}

	storageFiles := map[string]*bytes.Buffer{
		"some_garbage_file": new(bytes.Buffer),
		" ":                 new(bytes.Buffer),
	}

	currentSegmentName := "00000003000000000000000A"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    3,
				StartSegment:  "000000030000000000000001",
				EndSegment:    "000000030000000000000009",
				SegmentsCount: 9,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId: currentSegment.Timeline,
			// WAL storage folder is empty so highest found timeline should be zero
			HighestStorageTimelineId: 0,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageFiles:           storageFiles,
		storageSegments:        storageSegments,
	})
}

// check that wal-verify works for single timeline
func TestWalVerify_SingleTimeline_Ok(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000003",
		"000000050000000000000004",
	}
	currentSegmentName := "000000050000000000000005"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000001",
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           make(map[string]*bytes.Buffer),
	})
}

// check that wal-verify correctly marks delayed segments
func TestWalVerify_SingleTimeline_SomeDelayed(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000003",
		"000000050000000000000004",
	}

	currentSegmentName := "000000050000000000000019"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000001",
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000005",
				EndSegment:    "000000050000000000000018",
				SegmentsCount: 20,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           make(map[string]*bytes.Buffer),
	})
}

// check that wal-verify correctly follows timeline switches
func TestWalVerify_TwoTimelines_Ok(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000003",
		"000000050000000000000004",
		"000000050000000000000005", // should not get into output
		"000000050000000000000006", // should not get into output
		"000000050000000000000007", // should not get into output
		"000000050000000000000008", // should not get into output
		"000000050000000000000009", // should not get into output
		"000000060000000000000005",
		"000000060000000000000006",
		"000000060000000000000007",
		"000000060000000000000008",
	}

	// set switch point to somewhere in the 5th segment
	switchPointLsn := 5*internal.WalSegmentSize + 100
	historyContents := fmt.Sprintf("%d\t0/%X\tsome comment...\n\n", 5, switchPointLsn)
	historyName, historyFile, err := newTimelineHistoryFile(historyContents, 6)
	// .history file should be stored in wal folder
	historyName = utility.WalPath + historyName
	assert.NoError(t, err)

	currentSegmentName := "000000060000000000000009"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000001",
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
			{
				TimelineId:    6,
				StartSegment:  "000000060000000000000005",
				EndSegment:    "000000060000000000000008",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           map[string]*bytes.Buffer{historyName: historyFile},
	})
}

// check that wal-verify correctly reports lost segments
func TestWalVerify_TwoTimelines_SomeLost(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000004",
		"000000050000000000000005",
		"000000050000000000000006",
		"000000060000000000000007",
		"000000060000000000000008",
	}

	// set switch point to somewhere in the 5th segment
	switchPointLsn := 5*internal.WalSegmentSize + 100
	historyContents := fmt.Sprintf("%d\t0/%X\tsome comment...\n\n", 5, switchPointLsn)
	historyName, historyFile, err := newTimelineHistoryFile(historyContents, 6)
	// .history file should be stored in wal folder
	historyName = utility.WalPath + historyName
	assert.NoError(t, err)

	currentSegmentName := "000000060000000000000009"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusFailure,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000001",
				EndSegment:    "000000050000000000000002",
				SegmentsCount: 2,
				Status:        internal.Found,
			},
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000003",
				EndSegment:    "000000050000000000000003",
				SegmentsCount: 1,
				Status:        internal.Lost,
			},
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000004",
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 1,
				Status:        internal.Found,
			},
			{
				TimelineId:    6,
				StartSegment:  "000000060000000000000005",
				EndSegment:    "000000060000000000000006",
				SegmentsCount: 2,
				Status:        internal.ProbablyUploading,
			},
			{
				TimelineId:    6,
				StartSegment:  "000000060000000000000007",
				EndSegment:    "000000060000000000000008",
				SegmentsCount: 2,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           map[string]*bytes.Buffer{historyName: historyFile},
	})
}

// wal-verify timeline check test
func TestWalVerify_HigherTimelineExists(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000003",
		"000000050000000000000004",
		"000000070000000000000003",
		"000000070000000000000004",
	}
	currentSegmentName := "000000050000000000000005"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000001",
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusFailure,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: 7,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           make(map[string]*bytes.Buffer),
	})
}

// Check that correct backup is chosen for wal-verify range start
func TestWalVerify_WalkUntilFirstBackup(t *testing.T) {
	storageSegments := []string{
		"000000050000000000000001",
		"000000050000000000000002",
		"000000050000000000000003",
		"000000050000000000000004",
		"000000050000000000000005", // should not get into output
		"000000050000000000000006", // should not get into output
		"000000050000000000000007", // should not get into output
		"000000050000000000000008", // should not get into output
		"000000050000000000000009", // should not get into output
		"000000060000000000000005",
		"000000060000000000000006",
		"000000060000000000000007",
		"000000060000000000000008",
	}

	storageFiles := make(map[string]*bytes.Buffer, 4)

	backupSentinelNames := []string{
		// this backup should not be selected as the earliest,
		// because it does not belong to the current timeline history
		utility.BackupNamePrefix + "000000050000000000000005" + utility.SentinelSuffix,
		// this backup should not be selected as the earliest,
		// because it is not
		utility.BackupNamePrefix + "000000060000000000000007" + utility.SentinelSuffix,
		// this backup should be selected as the earliest
		utility.BackupNamePrefix + "000000060000000000000006" + utility.SentinelSuffix,
	}
	for _, name := range backupSentinelNames {
		storageFiles[utility.BaseBackupPath+name] = new(bytes.Buffer)
	}

	// set switch point to somewhere in the 5th segment
	switchPointLsn := 5*internal.WalSegmentSize + 100
	historyInfo := fmt.Sprintf("%d\t0/%X\tsome comment...\n\n", 5, switchPointLsn)
	historyName, historyFile, err := newTimelineHistoryFile(historyInfo, 6)
	// .history file should be stored in wal folder
	historyName = utility.WalPath + historyName
	assert.NoError(t, err)

	storageFiles[historyName] = historyFile

	currentSegmentName := "000000060000000000000009"
	currentSegment, _ := internal.NewWalSegmentDescription(currentSegmentName)

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    6,
				StartSegment:  "000000060000000000000006",
				EndSegment:    "000000060000000000000008",
				SegmentsCount: 3,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		currentWalSegment:      currentSegment,
		storageSegments:        storageSegments,
		storageFiles:           storageFiles,
	})
}

func testWalVerify(t *testing.T, setup WalVerifyTestSetup) {
	expectedResult := map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult{
		internal.WalVerifyTimelineCheck:  setup.expectedTimelineCheck,
		internal.WalVerifyIntegrityCheck: setup.expectedIntegrityCheck,
	}

	result, outputCallsCount := executeWalVerify(
		setup.storageSegments,
		setup.storageFiles,
		setup.currentWalSegment)

	assert.Equal(t, 1, outputCallsCount)
	compareResults(t, expectedResult, result)
}

// executeWalShow invokes the HandleWalVerify() with fake storage filled with
// provided wal segments and other storage folder files
func executeWalVerify(
	walFilenames []string,
	storageFiles map[string]*bytes.Buffer,
	currentWalSegment internal.WalSegmentDescription,
) (map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult, int) {
	rootFolder := setupTestStorageFolder()
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	for name, content := range storageFiles {
		_ = rootFolder.PutObject(name, content)
	}
	putWalSegments(walFilenames, walFolder)

	mockOutputWriter := &MockWalVerifyOutputWriter{}
	checkTypes := []internal.WalVerifyCheckType{
		internal.WalVerifyTimelineCheck, internal.WalVerifyIntegrityCheck}

	internal.HandleWalVerify(checkTypes, rootFolder, currentWalSegment, mockOutputWriter)

	return mockOutputWriter.lastResult, mockOutputWriter.writeCallsCount
}

func compareResults(
	t *testing.T,
	expected map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult,
	returned map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult) {

	assert.Equal(t, len(expected), len(returned))

	for checkType, checkResult := range returned {
		assert.Contains(t, expected, checkType)
		assert.Equal(t, expected[checkType].Status, checkResult.Status,
			"Result status doesn't match the expected status")

		assert.True(t, reflect.DeepEqual(expected[checkType].Details, checkResult.Details),
			"Result details don't match the expected values")
	}
}
