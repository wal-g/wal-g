package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/katie31/wal-g"
	"github.com/katie31/wal-g/testTools"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

var correctKeys = []string{"mockServer/base_backup/second.nop",
	"mockServer/base_backup/fourth.nop",
	"mockServer/base_backup/fifth.nop",
	"mockServer/base_backup/first.nop",
	"mockServer/base_backup/third.nop"}

/**
 *  Mock out S3 client. Includes these methods:
 *  ListObjectsV2(*ListObjectsV2Input)
 *  GetObject(*GetObjectInput)
 *  HeadObject(*HeadObjectInput)
 */
type mockS3Client struct {
	s3iface.S3API
	err bool
}

func (m *mockS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	contents := fakeContents()
	output := &s3.ListObjectsV2Output{
		Contents: contents,
		Name:     input.Bucket,
	}
	return output, nil
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	output := &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("mock content")),
	}
	return output, nil
}

func (m *mockS3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	var err error
	if m.err {
		err = awserr.New("NotFound", "object not found", nil)
	}

	return &s3.HeadObjectOutput{}, err
}

/**
 *  Mock out uploader client for S3. Includes these methods:
 *  Upload(*UploadInput, ...func(*s3manager.Uploader))
 */
type mockS3Uploader struct {
	s3manageriface.UploaderAPI
}

func (u *mockS3Uploader) Upload(input *s3manager.UploadInput, f ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	var err error
	output := &s3manager.UploadOutput{
		Location:  *input.Bucket,
		VersionID: input.Key,
	}

	/***	Discard bytes to unblock pipe.	***/
	_, e := io.Copy(ioutil.Discard, input.Body)
	if e != nil {
		err = e
	}

	return output, err
}

/**
 *  Creates 5 fake s3 objects with Key and LastModified field.
 */
func fakeContents() []*s3.Object {
	c := make([]*s3.Object, 5)

	ob := &s3.Object{
		Key:          aws.String("mockServer/base_backup/second.nop"),
		LastModified: aws.Time(time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC)),
	}
	c[0] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fourth.nop"),
		LastModified: aws.Time(time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC)),
	}
	c[1] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fifth.nop"),
		LastModified: aws.Time(time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC)),
	}
	c[2] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/first.nop"),
		LastModified: aws.Time(time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC)),
	}
	c[3] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/third.nop"),
		LastModified: aws.Time(time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC)),
	}
	c[4] = ob

	return c
}

/**
 *  Tests that backup fetch methods work. Tests:
 *  GetLatest()
 *  CheckExistence()
 *  GetKeys()
 */
func TestBackup(t *testing.T) {
	pre := &walg.Prefix{
		Svc:    &mockS3Client{},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	bk := &walg.Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
		Name:   aws.String("base_backupmockBackup"),
	}

	latest := bk.GetLatest()
	if latest != "first.nop" {
		t.Errorf("backup: expected %s from 'GetLatest' but got %s", "first", latest)
	}

	exists := bk.CheckExistence()
	if !exists {
		t.Errorf("backup: expected mock backup to exist but 'CheckExistence' returned false")
	}

	keys := bk.GetKeys()

	for i, val := range correctKeys {
		if keys[i] != val {
			t.Errorf("backup: expected %s but got %s", val, keys[i])
		}
	}

	pre.Svc = &mockS3Client{
		err: true,
	}

	exists = bk.CheckExistence()
	if exists {
		t.Errorf("backup: expected mock backup to not exist")
	}

	/***	Test S3 ReaderMaker		***/
	n := &tools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := &walg.S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: walg.CheckType(key),
		}
		out[i] = s
		if out[i].Path() != correctKeys[i] {
			t.Errorf("backup: expected S3ReaderMaker key to be %s but got %s", correctKeys[i], out[i].Path())
		}
	}

	err := walg.ExtractAll(n, out)
	if err != nil {
		t.Errorf("backup: could not extract from S3ReaderMaker")
	}

}

/**
 *  Tests Archive functions including:
 *  CheckExistence()
 *  GetArchive()
 */
func TestArchive(t *testing.T) {
	pre := &walg.Prefix{
		Svc:    &mockS3Client{},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	arch := &walg.Archive{
		Prefix:  pre,
		Archive: aws.String("mockArchive"),
	}

	exists := arch.CheckExistence()
	if !exists {
		t.Errorf("backup: expected mock archive to exist but 'CheckExistence' returned false")
	}

	body := arch.GetArchive()
	allBody, err := ioutil.ReadAll(body)

	if err != nil {
		t.Log(err)
	}

	if string(allBody[:]) != "mock content" {
		t.Errorf("backup: expected archive body to be %s but got %v", "mock content", allBody)
	}

	pre.Svc = &mockS3Client{
		err: true,
	}

	exists = arch.CheckExistence()
	if exists {
		t.Errorf("backup: expected mock backup to not exist")
	}
}
