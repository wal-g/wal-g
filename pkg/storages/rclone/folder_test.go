package rclone

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureStorage_MissingRemoteName(t *testing.T) {
	settings := map[string]string{}
	prefix := "rcloneremote://bucket/path"

	_, err := ConfigureStorage(context.Background(), prefix, settings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RCLONE_REMOTE")
}

func TestConfigureStorage_ValidConfig(t *testing.T) {
	settings := map[string]string{
		"RCLONE_REMOTE":      "myremote",
		"RCLONE_BINARY_PATH": "echo",
	}
	prefix := "myremote://mybucket/path"

	storage, err := ConfigureStorage(context.Background(), prefix, settings)
	require.NoError(t, err)
	assert.NotNil(t, storage)
	assert.NotEmpty(t, storage.ConfigHash())
}

func TestConfigureStorage_CustomSettings(t *testing.T) {
	settings := map[string]string{
		"RCLONE_REMOTE":      "myremote",
		"RCLONE_BINARY_PATH": "echo",
		"RCLONE_TRANSFERS":   "8",
		"RCLONE_RETRIES":     "5",
	}
	prefix := "myremote://mybucket"

	storage, err := ConfigureStorage(context.Background(), prefix, settings)
	require.NoError(t, err)
	assert.NotNil(t, storage)
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				RemoteName:   "myremote",
				Bucket:       "mybucket",
				RcloneBinary: "echo", // Use echo as mock
			},
			wantErr: false,
		},
		{
			name: "missing remote name",
			config: &Config{
				Bucket:       "mybucket",
				RcloneBinary: "echo",
			},
			wantErr: true,
			errMsg:  "remote name is required",
		},
		{
			name: "missing bucket",
			config: &Config{
				RemoteName:   "myremote",
				RcloneBinary: "echo",
			},
			wantErr: true,
			errMsg:  "bucket/container name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFolder_GetPath(t *testing.T) {
	config := &Config{
		RemoteName:   "myremote",
		Bucket:       "mybucket",
		RcloneBinary: "rclone",
	}

	tests := []struct {
		name     string
		subPath  string
		expected string
	}{
		{
			name:     "root folder",
			subPath:  "",
			expected: "/",
		},
		{
			name:     "subfolder",
			subPath:  "backups",
			expected: "backups/",
		},
		{
			name:     "nested subfolder",
			subPath:  "backups/mysql",
			expected: "backups/mysql/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			folder := NewFolder("myremote:mybucket", tt.subPath, config)
			assert.Equal(t, tt.expected, folder.GetPath())
		})
	}
}

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "unrelated error",
			err:      fmt.Errorf("connection refused"),
			expected: false,
		},
		{
			name:     "not found",
			err:      fmt.Errorf("object not found"),
			expected: true,
		},
		{
			name:     "no such file",
			err:      fmt.Errorf("no such file or directory"),
			expected: true,
		},
		{
			name:     "doesn't exist",
			err:      fmt.Errorf("path doesn't exist"),
			expected: true,
		},
		{
			name:     "directory not found",
			err:      fmt.Errorf("directory not found"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNotFoundError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
