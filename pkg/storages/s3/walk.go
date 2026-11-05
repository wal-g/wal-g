package s3

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	if folder.showsAllVersions() && folder.isVersioningEnabled(ctx) {
		pages := awss3.NewListObjectVersionsPaginator(folder.s3API,
			&awss3.ListObjectVersionsInput{Bucket: folder.bucket, Prefix: aws.String(folder.path)})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return err
			}
			for i := range page.Versions {
				v := &page.Versions[i]
				obj := storage.NewLocalObjectWithVersion(strings.TrimPrefix(aws.ToString(v.Key), folder.path),
					aws.ToTime(v.LastModified), aws.ToInt64(v.Size), aws.ToString(v.VersionId), "")
				if err := visit(obj); err != nil {
					return err
				}
			}
			for _, v := range page.DeleteMarkers {
				obj := storage.NewLocalObjectWithVersion(strings.TrimPrefix(aws.ToString(v.Key), folder.path),
					aws.ToTime(v.LastModified), 0, aws.ToString(v.VersionId), "DELETE")
				if err := visit(obj); err != nil {
					return err
				}
			}
		}
		return nil
	}
	pages := awss3.NewListObjectsV2Paginator(folder.s3API, &awss3.ListObjectsV2Input{Bucket: folder.bucket, Prefix: aws.String(folder.path)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			if isAwsNotExist(err) {
				return nil
			}
			return err
		}
		for _, v := range page.Contents {
			if err := visit(storage.NewLocalObject(strings.TrimPrefix(aws.ToString(v.Key), folder.path),
				aws.ToTime(v.LastModified), aws.ToInt64(v.Size))); err != nil {
				return err
			}
		}
	}
	return nil
}
