package internal

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestS3StorageAdapterLoadsS3UploadConcurrency(t *testing.T) {
	config := viper.New()
	config.Set("WALG_UPLOAD_CONCURRENCY", "4")
	config.Set("WALG_S3_UPLOAD_CONCURRENCY", "1")

	var s3Adapter *StorageAdapter
	for i := range StorageAdapters {
		if StorageAdapters[i].storageType == "S3" {
			s3Adapter = &StorageAdapters[i]
			break
		}
	}

	if !assert.NotNil(t, s3Adapter) {
		return
	}

	settings := s3Adapter.loadSettings(config)
	assert.Equal(t, "4", settings["UPLOAD_CONCURRENCY"])
	assert.Equal(t, "1", settings["S3_UPLOAD_CONCURRENCY"])
}
