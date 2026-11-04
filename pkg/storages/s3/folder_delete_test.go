package s3_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// mockS3ClientDelete answers DeleteObjects with a fixed output and records
// every request it receives.
type mockS3ClientDelete struct {
	mockS3ClientVersioning
	output *s3.DeleteObjectsOutput
	inputs []*s3.DeleteObjectsInput
}

func (m *mockS3ClientDelete) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput,
	_ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.inputs = append(m.inputs, input)
	return m.output, nil
}

func newDeleteTestFolder(client walgs3.API) *walgs3.Folder {
	return walgs3.NewFolder(client, nil, "", &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "disabled",
		DeleteBatchSize:  1000,
	})
}

func TestDeleteObjects_ReturnsErrorWhenS3RefusesKeys(t *testing.T) {
	client := &mockS3ClientDelete{output: &s3.DeleteObjectsOutput{
		Errors: []types.Error{{
			Key:       aws.String("basebackups_005/base_000000010000000000000002_backup_stop_sentinel.json"),
			VersionId: aws.String(""),
			Code:      aws.String("NoSuchVersion"),
			Message:   aws.String("The specified version does not exist."),
		}},
	}}

	err := newDeleteTestFolder(client).DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("basebackups_005/base_000000010000000000000002_backup_stop_sentinel.json", time.Now(), 1),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "refused to delete 1 object(s)")
	assert.Contains(t, err.Error(), "NoSuchVersion")
	assert.Contains(t, err.Error(), "base_000000010000000000000002_backup_stop_sentinel.json")
}

func TestDeleteObjects_CapsReportedErrors(t *testing.T) {
	refused := make([]types.Error, 0, 25)
	for i := 0; i < 25; i++ {
		refused = append(refused, types.Error{
			Key:  aws.String("wal_005/segment"),
			Code: aws.String("AccessDenied"),
		})
	}
	client := &mockS3ClientDelete{output: &s3.DeleteObjectsOutput{Errors: refused}}

	err := newDeleteTestFolder(client).DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("wal_005/segment", time.Now(), 1),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "refused to delete 25 object(s)")
	assert.Equal(t, 10, strings.Count(err.Error(), "AccessDenied"))
}

func TestDeleteObjects_SucceedsWhenS3DeletesEveryKey(t *testing.T) {
	client := &mockS3ClientDelete{output: &s3.DeleteObjectsOutput{
		Deleted: []types.DeletedObject{{Key: aws.String("wal_005/000000010000000000000002.br")}},
	}}

	err := newDeleteTestFolder(client).DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("wal_005/000000010000000000000002.br", time.Now(), 1),
	})

	require.NoError(t, err)
	require.Len(t, client.inputs, 1)
	assert.Nil(t, client.inputs[0].Delete.Objects[0].VersionId,
		"an object without a version must be deleted without a VersionId")
}
