package oss

import (
	"context"
	"fmt"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (f *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	if f.isVersioningEnabled(ctx) {
		return fmt.Errorf("versioning is not supported for oss")
	}
	prefix := f.GetPath()
	var token *string
	for {
		page, err := f.ossAPI.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
			Bucket: oss.Ptr(f.bucket), Prefix: oss.Ptr(prefix), ContinuationToken: token})
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(*obj.Key, prefix)
			if err := visit(storage.NewLocalObject(key, *obj.LastModified, obj.Size)); err != nil {
				return err
			}
		}
		if !page.IsTruncated {
			return nil
		}
		token = page.NextContinuationToken
	}
}
