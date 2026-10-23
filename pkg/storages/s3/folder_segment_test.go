package s3_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/testtools"
)

type segmentS3Client struct {
	*testtools.MockS3Client
	startAfter *string
}

func (c *segmentS3Client) ListObjectsV2(
	_ context.Context,
	input *awss3.ListObjectsV2Input,
	_ ...func(*awss3.Options),
) (*awss3.ListObjectsV2Output, error) {
	c.startAfter = input.StartAfter
	now := time.Now()
	return &awss3.ListObjectsV2Output{Contents: []types.Object{
		{Key: aws.String("root/gap_100.0_110.0.br"), LastModified: &now},
		{Key: aws.String("root/gap_110.0_120.0.br"), LastModified: &now},
		{Key: aws.String("root/oplog_120.0_130.0.br"), LastModified: &now},
	}}, nil
}

func TestListFolderSegmentAppliesRelativeBoundsInsideSubfolder(t *testing.T) {
	client := &segmentS3Client{MockS3Client: testtools.NewMockS3Client(false, false)}
	config := &walgs3.Config{Bucket: "test"}
	folder := walgs3.NewFolder(client, nil, "root/", config)
	startAfter := "gap_090.0"
	endBefore := "gap_~"

	objects, _, err := folder.ListFolderSegment(t.Context(), &startAfter, &endBefore)

	require.NoError(t, err)
	require.Len(t, objects, 2)
	require.Equal(t, "root/"+startAfter, aws.ToString(client.startAfter))
}
