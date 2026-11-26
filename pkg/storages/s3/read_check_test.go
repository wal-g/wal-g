package s3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// readCheckS3Client records listing calls. Its ListObjectsV2 always reports a
// truncated page so that any pagination would show up as extra calls.
type readCheckS3Client struct {
	*mockS3ClientVersioning
	listV2Inputs       []*s3.ListObjectsV2Input
	listVersionsCalls  int
	getVersioningCalls int
	listV2Err          error
}

func (m *readCheckS3Client) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input,
	_ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.listV2Inputs = append(m.listV2Inputs, in)
	if m.listV2Err != nil {
		return nil, m.listV2Err
	}
	return &s3.ListObjectsV2Output{
		Contents:              []types.Object{{Key: aws.String("wal_005/000000010000000000000001.br")}},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String("next"),
	}, nil
}

func (m *readCheckS3Client) ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput,
	opts ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	m.listVersionsCalls++
	return m.mockS3ClientVersioning.ListObjectVersions(ctx, in, opts...)
}

func (m *readCheckS3Client) GetBucketVersioning(ctx context.Context, in *s3.GetBucketVersioningInput,
	opts ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	m.getVersioningCalls++
	return m.mockS3ClientVersioning.GetBucketVersioning(ctx, in, opts...)
}

func newReadCheckClient() *readCheckS3Client {
	return &readCheckS3Client{mockS3ClientVersioning: &mockS3ClientVersioning{}}
}

func TestCheckReadAccessIsSingleBoundedRequest(t *testing.T) {
	for _, versioning := range []string{"", walgs3.VersioningEnabled, walgs3.VersioningDisabled} {
		t.Run("versioning="+versioning, func(t *testing.T) {
			client := newReadCheckClient()
			config := &walgs3.Config{Bucket: "test-bucket", EnableVersioning: versioning}
			root := walgs3.NewFolder(client, nil, "root", config)

			for _, folder := range []storage.Folder{root, root.GetSubFolder("wal_005")} {
				client.listV2Inputs = nil
				require.NoError(t, storage.CheckReadAccess(t.Context(), folder))

				require.Len(t, client.listV2Inputs, 1, "the probe must not paginate")
				in := client.listV2Inputs[0]
				assert.Equal(t, "test-bucket", aws.ToString(in.Bucket))
				assert.Equal(t, folder.GetPath(), aws.ToString(in.Prefix))
				assert.Equal(t, int32(1), aws.ToInt32(in.MaxKeys))
				assert.Nil(t, in.Delimiter)
				assert.Nil(t, in.ContinuationToken)
				assert.Nil(t, in.StartAfter)
			}
			assert.Zero(t, client.listVersionsCalls)
			assert.Zero(t, client.getVersioningCalls)
		})
	}
}

func TestCheckReadAccessErrors(t *testing.T) {
	t.Run("access error is returned", func(t *testing.T) {
		client := newReadCheckClient()
		client.listV2Err = &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
		folder := walgs3.NewFolder(client, nil, "root", &walgs3.Config{Bucket: "test-bucket"})

		err := storage.CheckReadAccess(t.Context(), folder)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AccessDenied")
		assert.Contains(t, err.Error(), "root/")
	})

	t.Run("missing folder is not an error", func(t *testing.T) {
		client := newReadCheckClient()
		client.listV2Err = &types.NoSuchKey{}
		folder := walgs3.NewFolder(client, nil, "root", &walgs3.Config{Bucket: "test-bucket"})

		require.NoError(t, storage.CheckReadAccess(t.Context(), folder))
	})
}
