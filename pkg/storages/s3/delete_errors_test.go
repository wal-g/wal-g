package s3_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/test/mocks"
	"go.uber.org/mock/gomock"
)

func TestDeleteObjectsReportsPerObjectFailures(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	api.EXPECT().DeleteObjects(gomock.Any(), gomock.Any(), gomock.Any()).Return(&awss3.DeleteObjectsOutput{Errors: []types.Error{{Key: aws.String("sentinel"), Code: aws.String("AccessDenied")}}}, nil)
	folder := s3.NewFolder(api, nil, "", &s3.Config{Bucket: "test", DeleteBatchSize: 500})
	require.ErrorContains(t, folder.DeleteObjects(t.Context(), []storage.Object{storage.NewLocalObject("sentinel", time.Time{}, 0)}), "AccessDenied")
}

func TestWalkObjectsPreservesVersionsAcrossPages(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	when := time.Unix(1, 0)
	gomock.InOrder(
		api.EXPECT().ListObjectVersions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&awss3.ListObjectVersionsOutput{
			IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("root/nested/file"), NextVersionIdMarker: aws.String("v1"),
			Versions: []types.ObjectVersion{{Key: aws.String("root/nested/file"), VersionId: aws.String("v1"), LastModified: &when, Size: aws.Int64(0)}},
		}, nil),
		api.EXPECT().ListObjectVersions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&awss3.ListObjectVersionsOutput{
			Versions:      []types.ObjectVersion{{Key: aws.String("root/nested/file"), VersionId: aws.String("v2"), LastModified: &when, Size: aws.Int64(3)}},
			DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("root/nested/file"), VersionId: aws.String("deleted"), LastModified: &when}},
		}, nil),
	)
	folder := s3.NewFolder(api, nil, "root/", &s3.Config{Bucket: "test", EnableVersioning: "enabled"})
	folder.SetShowAllVersions(true)
	var versions []string
	require.NoError(t, folder.WalkObjects(t.Context(), func(obj storage.Object) error {
		require.Equal(t, "nested/file", obj.GetName())
		versions = append(versions, obj.GetVersionID())
		return nil
	}))
	require.Equal(t, []string{"v1", "v2", "deleted"}, versions)
}
