package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/sync/errgroup"
)

const (
	maxSingleCopySize = int64(5 << 30)
	maxCopyPartSize   = int64(5 << 30)
	abortCopyTimeout  = 30 * time.Second
)

type multipartCopyAPI interface {
	CreateMultipartUpload(ctx context.Context, params *awss3.CreateMultipartUploadInput,
		optFns ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPartCopy(ctx context.Context, params *awss3.UploadPartCopyInput,
		optFns ...func(*awss3.Options)) (*awss3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *awss3.CompleteMultipartUploadInput,
		optFns ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *awss3.AbortMultipartUploadInput,
		optFns ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
}

var _ storage.CrossFolderCopier = (*Folder)(nil)

func (folder *Folder) CanCopyFrom(source storage.Folder) bool {
	sourceFolder, ok := source.(*Folder)
	return ok && sameS3Service(sourceFolder.config, folder.config)
}

func sameS3Service(source, destination *Config) bool {
	if source == nil || destination == nil {
		return false
	}
	if source.Endpoint == "" && source.EndpointSource == "" &&
		destination.Endpoint == "" && destination.EndpointSource == "" {
		return true
	}
	return normalizeEndpoint(source.Endpoint) == normalizeEndpoint(destination.Endpoint) &&
		normalizeEndpoint(source.EndpointSource) == normalizeEndpoint(destination.EndpointSource) &&
		source.EndpointPort == destination.EndpointPort &&
		strings.EqualFold(source.EndpointProtocol, destination.EndpointProtocol)
}

func normalizeEndpoint(endpoint string) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(endpoint)), "/")
}

func (folder *Folder) CopyObjectFrom(
	ctx context.Context,
	source storage.Folder,
	sourcePath, destinationPath string,
	size int64,
) error {
	sourceFolder, ok := source.(*Folder)
	if !ok || !sameS3Service(sourceFolder.config, folder.config) {
		return fmt.Errorf("S3 folders do not use the same service endpoint")
	}
	if folder.config == nil || folder.config.Uploader == nil {
		return fmt.Errorf("destination S3 uploader configuration is missing")
	}

	head, err := sourceFolder.headObject(ctx, sourcePath)
	if err != nil {
		return err
	}
	if head == nil {
		return storage.NewObjectNotFoundError(sourcePath)
	}
	if head.ContentLength != nil && *head.ContentLength != size {
		return fmt.Errorf("source object %q changed size: planned %d, current %d", sourcePath, size, *head.ContentLength)
	}

	partSize := int64(folder.config.Uploader.MaxPartSize)
	if size <= maxSingleCopySize && partSize > 0 && size <= partSize {
		if err := folder.copyObjectFrom(ctx, sourceFolder, sourcePath, destinationPath, head); err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("Copied %q to %q using S3 server-side copy (single).", sourcePath, destinationPath)
		return nil
	}
	if err := folder.copyObjectMultipart(ctx, sourceFolder, sourcePath, destinationPath, size, head); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Copied %q to %q using S3 server-side copy (multipart).", sourcePath, destinationPath)
	return nil
}

func (folder *Folder) copyObjectFrom(
	ctx context.Context,
	source *Folder,
	sourcePath, destinationPath string,
	head *awss3.HeadObjectOutput,
) error {
	copySource := buildCopySource(*source.bucket, source.path+sourcePath, aws.ToString(head.VersionId))
	input := &awss3.CopyObjectInput{
		Bucket:            folder.bucket,
		Key:               aws.String(folder.path + destinationPath),
		CopySource:        aws.String(copySource),
		CopySourceIfMatch: head.ETag,
		MetadataDirective: types.MetadataDirectiveReplace,
		StorageClass:      types.StorageClass(folder.uploader.StorageClass),
		TaggingDirective:  types.TaggingDirectiveReplace,
	}
	source.applyCopySourceSSE(input)
	folder.applyCopyDestination(input)
	_, err := folder.s3API.CopyObject(ctx, input)
	if err != nil {
		return fmt.Errorf("copy S3 object %q to %q: %w", sourcePath, destinationPath, err)
	}
	return nil
}

func (folder *Folder) copyObjectMultipart(
	ctx context.Context,
	source *Folder,
	sourcePath, destinationPath string,
	size int64,
	head *awss3.HeadObjectOutput,
) error {
	api, ok := folder.s3API.(multipartCopyAPI)
	if !ok {
		return fmt.Errorf("S3 client does not support multipart copy")
	}
	partSize, partCount, err := multipartCopyLayout(size, int64(folder.config.Uploader.MaxPartSize))
	if err != nil {
		return err
	}

	destinationKey := folder.path + destinationPath
	createInput := &awss3.CreateMultipartUploadInput{
		Bucket:       folder.bucket,
		Key:          aws.String(destinationKey),
		StorageClass: types.StorageClass(folder.uploader.StorageClass),
	}
	folder.applyMultipartDestination(createInput)
	created, err := api.CreateMultipartUpload(ctx, createInput)
	if err != nil {
		return fmt.Errorf("create multipart copy for %q: %w", destinationPath, err)
	}
	if created.UploadId == nil || *created.UploadId == "" {
		return fmt.Errorf("create multipart copy for %q returned an empty upload ID", destinationPath)
	}
	uploadID := *created.UploadId

	abort := func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortCopyTimeout)
		defer cancel()
		_, abortErr := api.AbortMultipartUpload(cleanupCtx, &awss3.AbortMultipartUploadInput{
			Bucket: folder.bucket, Key: aws.String(destinationKey), UploadId: aws.String(uploadID),
		})
		if abortErr != nil {
			tracelog.WarningLogger.Printf("Failed to abort multipart copy %q (upload %s): %v", destinationPath, uploadID, abortErr)
		}
	}

	copySource := buildCopySource(*source.bucket, source.path+sourcePath, aws.ToString(head.VersionId))
	completedParts := make([]types.CompletedPart, partCount)
	group, groupCtx := errgroup.WithContext(ctx)
	concurrency := folder.config.Uploader.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	group.SetLimit(concurrency)
	for index := 0; index < partCount; index++ {
		group.Go(func() error {
			start := int64(index) * partSize
			end := min(start+partSize, size) - 1
			partNumber := int32(index + 1)
			input := &awss3.UploadPartCopyInput{
				Bucket:            folder.bucket,
				Key:               aws.String(destinationKey),
				UploadId:          aws.String(uploadID),
				PartNumber:        aws.Int32(partNumber),
				CopySource:        aws.String(copySource),
				CopySourceRange:   aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				CopySourceIfMatch: head.ETag,
			}
			source.applyUploadPartCopySourceSSE(input)
			folder.applyUploadPartCopyDestinationSSE(input)
			output, err := api.UploadPartCopy(groupCtx, input)
			if err != nil {
				return fmt.Errorf("copy part %d of %q: %w", partNumber, destinationPath, err)
			}
			if output.CopyPartResult == nil || output.CopyPartResult.ETag == nil {
				return fmt.Errorf("copy part %d of %q returned no ETag", partNumber, destinationPath)
			}
			completedParts[index] = types.CompletedPart{ETag: output.CopyPartResult.ETag, PartNumber: aws.Int32(partNumber)}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		abort()
		return err
	}

	completeInput := &awss3.CompleteMultipartUploadInput{
		Bucket:   folder.bucket,
		Key:      aws.String(destinationKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	folder.applyCompleteMultipartDestinationSSE(completeInput)
	if _, err := api.CompleteMultipartUpload(ctx, completeInput); err != nil {
		abort()
		return fmt.Errorf("complete multipart copy for %q: %w", destinationPath, err)
	}
	return nil
}

func multipartCopyLayout(size, configuredPartSize int64) (partSize int64, partCount int, err error) {
	if size <= 0 {
		return 0, 0, fmt.Errorf("multipart copy requires a positive object size")
	}
	if configuredPartSize < manager.MinUploadPartSize {
		return 0, 0, fmt.Errorf("S3 multipart copy part size must be at least %d bytes", manager.MinUploadPartSize)
	}
	partSize = max(configuredPartSize, (size+int64(manager.MaxUploadParts)-1)/int64(manager.MaxUploadParts))
	if partSize > maxCopyPartSize {
		return 0, 0, fmt.Errorf("S3 multipart copy part size %d exceeds the maximum %d", partSize, maxCopyPartSize)
	}
	partCount = int((size + partSize - 1) / partSize)
	if partCount > int(manager.MaxUploadParts) {
		return 0, 0, fmt.Errorf("S3 multipart copy requires %d parts, maximum is %d", partCount, manager.MaxUploadParts)
	}
	return partSize, partCount, nil
}

func buildCopySource(bucket, key, versionID string) string {
	source := url.PathEscape(strings.TrimPrefix(bucket+"/"+key, "/"))
	if versionID != "" {
		source += "?versionId=" + url.QueryEscape(versionID)
	}
	return source
}

func (folder *Folder) applyCopySourceSSE(input *awss3.CopyObjectInput) {
	if folder.uploader.serverSideEncryption == "" || folder.uploader.SSECustomerKey == "" {
		return
	}
	algorithm, key, keyMD5 := folder.sseCustomerHeaders()
	input.CopySourceSSECustomerAlgorithm = algorithm
	input.CopySourceSSECustomerKey = key
	input.CopySourceSSECustomerKeyMD5 = keyMD5
}

func (folder *Folder) applyCopyDestination(input *awss3.CopyObjectInput) {
	input.StorageClass = types.StorageClass(folder.uploader.StorageClass)
	if folder.uploader.RetentionPeriod != defaultDisabledRetentionPeriod {
		retainUntil := time.Now().Add(time.Second * folder.uploader.RetentionPeriod)
		input.ObjectLockMode = types.ObjectLockMode(folder.uploader.RetentionMode)
		input.ObjectLockRetainUntilDate = &retainUntil
	}
	if folder.uploader.serverSideEncryption == "" {
		return
	}
	if folder.uploader.SSECustomerKey != "" {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5 = folder.sseCustomerHeaders()
	} else {
		input.ServerSideEncryption = types.ServerSideEncryption(folder.uploader.serverSideEncryption)
	}
	if folder.uploader.SSEKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(folder.uploader.SSEKMSKeyID)
	}
}

func (folder *Folder) applyMultipartDestination(input *awss3.CreateMultipartUploadInput) {
	if folder.uploader.RetentionPeriod != defaultDisabledRetentionPeriod {
		retainUntil := time.Now().Add(time.Second * folder.uploader.RetentionPeriod)
		input.ObjectLockMode = types.ObjectLockMode(folder.uploader.RetentionMode)
		input.ObjectLockRetainUntilDate = &retainUntil
	}
	if folder.uploader.serverSideEncryption == "" {
		return
	}
	if folder.uploader.SSECustomerKey != "" {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5 = folder.sseCustomerHeaders()
	} else {
		input.ServerSideEncryption = types.ServerSideEncryption(folder.uploader.serverSideEncryption)
	}
	if folder.uploader.SSEKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(folder.uploader.SSEKMSKeyID)
	}
}

func (folder *Folder) applyUploadPartCopySourceSSE(input *awss3.UploadPartCopyInput) {
	if folder.uploader.serverSideEncryption == "" || folder.uploader.SSECustomerKey == "" {
		return
	}
	input.CopySourceSSECustomerAlgorithm, input.CopySourceSSECustomerKey,
		input.CopySourceSSECustomerKeyMD5 = folder.sseCustomerHeaders()
}

func (folder *Folder) applyUploadPartCopyDestinationSSE(input *awss3.UploadPartCopyInput) {
	if folder.uploader.serverSideEncryption == "" || folder.uploader.SSECustomerKey == "" {
		return
	}
	input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5 = folder.sseCustomerHeaders()
}

func (folder *Folder) applyCompleteMultipartDestinationSSE(input *awss3.CompleteMultipartUploadInput) {
	if folder.uploader.serverSideEncryption == "" || folder.uploader.SSECustomerKey == "" {
		return
	}
	input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5 = folder.sseCustomerHeaders()
}

func (folder *Folder) sseCustomerHeaders() (algorithm, key, keyMD5 *string) {
	return aws.String(folder.uploader.serverSideEncryption),
		aws.String(sseCustomerKeyB64(folder.uploader.SSECustomerKey)),
		aws.String(GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey))
}
