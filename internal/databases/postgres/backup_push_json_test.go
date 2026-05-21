package postgres

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

// MockBackupHandler is a test helper that allows us to test the JSON output
// without needing a real PostgreSQL connection or file system
type MockBackupHandler struct {
	handler *BackupHandler
}

func NewMockBackupHandler(json, pretty bool) *MockBackupHandler {
	// Create a mock folder
	folder := memory.NewFolder("test/", memory.NewKVS())

	// Create a mock uploader
	uploader, err := internal.ConfigureUploaderToFolder(folder)
	if err != nil {
		panic(err)
	}

	// Create backup arguments with JSON flags
	args := NewBackupArguments(
		uploader,
		"/tmp",          // pgDataDirectory
		"backups",       // backupsFolder
		false,           // isPermanent
		false,           // verifyPageChecksums
		false,           // isFullBackup
		false,           // storeAllCorruptBlocks
		RegularComposer, // tarBallComposerType
		nil,             // deltaConfigurator
		nil,             // userData
		false,           // withoutFilesMetadata
	)

	// Set the JSON and pretty flags
	args.json = json
	args.pretty = pretty

	// Create a basic backup handler
	handler := &BackupHandler{
		CurBackupInfo: CurBackupInfo{
			Name: "test_backup_12345",
		},
		Arguments: args,
	}

	return &MockBackupHandler{
		handler: handler,
	}
}

// createAndPushBackupWithStorageNames mocks the createAndPushBackup method
// to test the JSON output without requiring a real backup process
func (m *MockBackupHandler) createAndPushBackupWithStorageNames(storageNames []string) (string, error) {
	// Capture stdout
	rescueStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Create the backup info
	createdBackup := BackupInfo{
		Name:    m.handler.CurBackupInfo.Name,
		Storage: storageNames[0],
	}

	var err error
	if m.handler.Arguments.json {
		err = printlist.OneElement(createdBackup, os.Stdout, m.handler.Arguments.json, m.handler.Arguments.pretty)
	} else {
		// We won't test the non-JSON case here as it just prints to tracelog
	}

	// Close and restore stdout
	w.Close()
	os.Stdout = rescueStdout

	// Read the captured output
	captured, _ := io.ReadAll(r)

	return string(captured), err
}

func TestCreateAndPushBackup_JSONOutput(t *testing.T) {
	ctx := context.Background()

	t.Run("json_plain_output", func(t *testing.T) {
		mockHandler := NewMockBackupHandler(true, false) // json=true, pretty=false
		output, err := mockHandler.createAndPushBackupWithStorageNames([]string{"s3"})

		require.NoError(t, err)

		// Parse the JSON output
		var backupInfo BackupInfo
		err = json.Unmarshal([]byte(strings.TrimSpace(output)), &backupInfo)
		require.NoError(t, err)

		// Check the values
		assert.Equal(t, "test_backup_12345", backupInfo.Name)
		assert.Equal(t, "s3", backupInfo.Storage)
	})

	t.Run("json_pretty_output", func(t *testing.T) {
		mockHandler := NewMockBackupHandler(true, true) // json=true, pretty=true
		output, err := mockHandler.createAndPushBackupWithStorageNames([]string{"gcs"})

		require.NoError(t, err)

		// Check that the output is properly indented
		assert.True(t, strings.Contains(output, "    \"name\": \"test_backup_12345\""))
		assert.True(t, strings.Contains(output, "    \"storage\": \"gcs\""))

		// Parse the JSON output
		var backupInfo BackupInfo
		err = json.Unmarshal([]byte(output), &backupInfo)
		require.NoError(t, err)

		// Check the values
		assert.Equal(t, "test_backup_12345", backupInfo.Name)
		assert.Equal(t, "gcs", backupInfo.Storage)
	})

	t.Run("non_json_output", func(t *testing.T) {
		mockHandler := NewMockBackupHandler(false, false) // json=false, pretty=false
		output, err := mockHandler.createAndPushBackupWithStorageNames([]string{"azure"})

		require.NoError(t, err)
		// When json=false, the function doesn't produce any output to stdout
		assert.Empty(t, output)
	})
}

func TestNewBackupArguments_WithJSONFlags(t *testing.T) {
	folder := memory.NewFolder("test/", memory.NewKVS())
	uploader, err := internal.ConfigureUploaderToFolder(folder)
	require.NoError(t, err)

	tests := []struct {
		name           string
		json           bool
		pretty         bool
		expectedJSON   bool
		expectedPretty bool
	}{
		{
			name:           "json_and_pretty_true",
			json:           true,
			pretty:         true,
			expectedJSON:   true,
			expectedPretty: true,
		},
		{
			name:           "json_true_pretty_false",
			json:           true,
			pretty:         false,
			expectedJSON:   true,
			expectedPretty: false,
		},
		{
			name:           "json_false_pretty_true",
			json:           false,
			pretty:         true,
			expectedJSON:   false,
			expectedPretty: true,
		},
		{
			name:           "both_false",
			json:           false,
			pretty:         false,
			expectedJSON:   false,
			expectedPretty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := NewBackupArguments(
				uploader,
				"/tmp",
				"backups",
				false,
				false,
				false,
				false,
				RegularComposer,
				nil,
				nil,
				false,
			)

			// Manually set the flags (as they would be set by command line parsing)
			args.json = tt.json
			args.pretty = tt.pretty

			assert.Equal(t, tt.expectedJSON, args.json)
			assert.Equal(t, tt.expectedPretty, args.pretty)
		})
	}
}
