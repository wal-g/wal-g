package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strings"
    "io"
    "io/ioutil"
)

type Prefix struct {
	Creds *credentials.Credentials
    Bucket *string
	Path *string
}

type Backup struct {
	p    *Prefix
	name string
}

func stripName(key string) string {
    all := strings.SplitAfter(key, "/")
    name := strings.Split(all[2], "_backup")[0]
    return name
}

func Volumes(path string, p Prefix, svc *s3.S3) []io.Reader{
    objects := &s3.ListObjectsInput{
        Bucket: p.Bucket,
        Prefix: aws.String(path),
        Delimiter: aws.String(".txt"),
    }

    files, err := svc.ListObjects(objects)
    if err != nil {
        panic(err)
    }

    //fmt.Println(len(files.Contents))
    arr := make([]io.Reader, len(files.Contents))

    for i, ob := range files.Contents {
        key := *ob.Key
        fmt.Println(key)
        input := &s3.GetObjectInput{
            Bucket: p.Bucket,
            Key: aws.String(key),
        }
        rdr, err := svc.GetObject(input)
        if err != nil {
            panic(err)
        }

        arr[i] = rdr.Body
    }
    for _, rdr := range arr {
        out, err := ioutil.ReadAll(rdr)
        if err != nil {
            panic(err)
        }
        fmt.Println(len(out))
    }
    return arr
}

func main() {
	sess := session.Must(session.NewSession())
    pre := Prefix {
        Creds: credentials.NewStaticCredentials("AKIAIZHEPBQI5NAXIFIQ", "9pGrHd5fDB/fmGYHvNGs6Sw9TJDIb7uwbO3CTCqn", ""),
        Bucket: aws.String("umrd6xaarmk35xwocqht2nqzwem5smds"),
        Path: aws.String("157247f4-2101-4e46-ae1c-3c32d5775c6d/basebackups_005/"),
    }

	config := &aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: pre.Creds,
	}

	sess, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}
	svc := s3.New(sess)

	objects := &s3.ListObjectsInput{
		Bucket:   pre.Bucket,
        Prefix: pre.Path,
		Delimiter: aws.String("/"),
	}

	sen, err := svc.ListObjects(objects)
	if err != nil {
		panic(err)
	}
	//fmt.Println(sen)
	for _, ob := range sen.Contents {
		key := *ob.Key
        name := *pre.Path + stripName(key) + "/"
        fmt.Println(name)
        Volumes(name, pre, svc)
        //fmt.Println(key)
        //fmt.Println(stripName(key))
		
	}

}
