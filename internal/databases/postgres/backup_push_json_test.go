package postgres

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupPushJSONOutput(t *testing.T) {
	// Test that theBackupInfo can be marshaled to JSON
	backupInfo := BackupInfo{
		Name:    "test_backup",
		Storage: "default",
	}

	// Test JSON marshaling
	jsonData, err := json.Marshal(backupInfo)
	require.NoError(t, err)

	// Test that it can be unmarshaled back
	var unmarshaled BackupInfo
	err = json.Unmarshal(jsonData, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, backupInfo, unmarshaled)

	// Test JSON with pretty output
	prettyJsonData, err := json.MarshalIndent(backupInfo, "", "    ")
	require.NoError(t, err)

	// Verify pretty JSON format includes indentation
	assert.Contains(t, string(prettyJsonData), "\n    ")
	assert.Contains(t, string(prettyJsonData), "\"Name\": \"test_backup\"")
}

func TestBackupPushJSONFlagHandling(t *testing.T) {
	// Test that JSON flag is properly passed through to BackupArguments
	// This is a simplified test since we're working with private fields

	// Create a buffer to capture output
	var buf bytes.Buffer

	// Create test backup info
	backupInfo := BackupInfo{
		Name:    "test_backup_json",
		Storage: "test_storage",
	}

	// Test printlist.OneElement function used in createAndPushBackup
	// Correct order: entity, output, pretty, json
	// We want json=true, pretty=false
	err := printlist.OneElement(backupInfo, &buf, false, true)
	require.NoError(t, err)

	// Get the string from buffer
	outputStr := buf.String()
	t.Logf("Captured output: %q", outputStr)

	// For plain JSON, the format is: {"Name":"...","Storage":"..."}
	// Check if it matches expected JSON format
	expectedJSON := "{\"Name\":\"test_backup_json\",\"Storage\":\"test_storage\"}"
	assert.Equal(t, expectedJSON+"\n", outputStr)

	// Ensure there's content in the output
	assert.NotEmpty(t, outputStr)

	// Trim newline for JSON parsing
	trimmedOutput := strings.TrimSpace(outputStr)

	// Verify the output is valid JSON
	var jsonOutput map[string]interface{}
	err = json.Unmarshal([]byte(trimmedOutput), &jsonOutput)
	require.NoError(t, err, "Failed to parse JSON output: %s", outputStr)

	// Verify the content
	assert.Equal(t, "test_backup_json", jsonOutput["Name"])
	assert.Equal(t, "test_storage", jsonOutput["Storage"])
}

func TestBackupPushJSONPrettyFlagHandling(t *testing.T) {
	// Test that JSON with pretty flag works correctly

	// Create a buffer to capture output
	var buf bytes.Buffer

	// Create test backup info
	backupInfo := BackupInfo{
		Name:    "test_backup_pretty",
		Storage: "test_storage",
	}

	// Test printlist.OneElement function with json=true and pretty=true
	// Correct order: entity, output, pretty, json
	// We want json=true, pretty=true
	err := printlist.OneElement(backupInfo, &buf, true, true)
	require.NoError(t, err)

	outputStr := buf.String()
	t.Logf("Captured output: %q", outputStr)

	// Ensure there's content in the output
	assert.NotEmpty(t, outputStr)

	// Verify output contains indentation for pretty format
	assert.Contains(t, outputStr, "    \"Name\": \"test_backup_pretty\"")
	assert.Contains(t, outputStr, "    \"Storage\": \"test_storage\"")

	// Verify the output is valid JSON
	var jsonOutput map[string]interface{}
	err = json.Unmarshal([]byte(outputStr), &jsonOutput)
	require.NoError(t, err, "Failed to parse JSON output: %s", outputStr)

	// Verify the content
	assert.Equal(t, "test_backup_pretty", jsonOutput["Name"])
	assert.Equal(t, "test_storage", jsonOutput["Storage"])
}
