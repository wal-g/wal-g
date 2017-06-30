package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/katie31/extract"
	"net/url"
	"os"
	//"sort"
)

func main() {
	/*** Configure and start session with bucket, region, and path names. Layer: Server ***/
	u, err := url.Parse(os.Getenv("WALE_S3_PREFIX"))
	if err != nil {
		panic(err)
	}

	pre := &extract.Prefix{
		Creds:  credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECURITY_TOKEN")),
		Bucket: aws.String(u.Host),
		Server: aws.String(u.Path[1:]),
	}

	config := &aws.Config{
		Region:      aws.String(os.Getenv("AWS_REGION")),
		Credentials: pre.Creds,
	}

	sess, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}

	pre.Svc = s3.New(sess)

	fmt.Println("BUCKET:", *pre.Bucket)
	fmt.Println("PATH:", *pre.Server)

	/*** Grab arguments from command line ***/
	all := os.Args
	fetch := all[1]
	dirArc := all[2]
	backupName := all[3]

	//replace with os.args()
	//tempBackupName := "base_000000010000000000000003_00000040"

	/*** OPTION: BACKUP-FETCH ***/
	if fetch == "backup-fetch" {
		var keys []string
		var bk *extract.Backup
		/*** Check if backup specified in COMMAND LINE exists and if it does extract to NEWDIR. ***/
		if backupName != "LATEST" {
			bk = &extract.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
				Name:   aws.String(backupName),
			}

			bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

			fmt.Println("NEWDIR", dirArc)
			fmt.Println("PATH:", *bk.Path)
			fmt.Println("NAME:", *bk.Name)
			fmt.Println("JSON:", *bk.Js)
			fmt.Println(bk.CheckExistence())

			if bk.CheckExistence() {
				keys = extract.GetKeys(bk)
			} else {
				fmt.Printf("Backup '%s' does not exist.\n", *bk.Name)
				os.Exit(1)
			}

			/*** Find the LATEST valid backup (checks against JSON file and grabs name from there) and extract to NEWDIR. ***/
		} else {
			bk = &extract.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
			}

			bk.Name = aws.String(extract.GetLatest(bk))
			keys = extract.GetKeys(bk)

			fmt.Println("NEWDIR", dirArc)
			fmt.Println("PATH:", *bk.Path)
			fmt.Println("NAME:", *bk.Name)

		}

		out := make([]extract.ReaderMaker, len(keys))
		for i, key := range keys {
			s := &extract.S3ReaderMaker{
				Backup: bk,
				Key:    aws.String(key),
			}
			out[i] = s
		}

		f := &extract.FileTarInterpreter{
			NewDir: dirArc,
		}
		extract.ExtractAll(f, out)

		//np := &extract.NOPTarInterpreter{}
		//extract.ExtractAll(np, out)
	} else if fetch == "wal-fetch" {
		a := &extract.Archive{
			Prefix:  pre,
			Archive: aws.String(*pre.Server + "/wal_005/" + dirArc + ".lzo"),
		}

		if a.CheckExistence() {
			arch := extract.GetArchive(a)
			f, err := os.Create(backupName)
			if err != nil {
				panic(err)
			}
			extract.Decompress(f, arch)
			f.Close()
		} else {
			fmt.Printf("Archive '%s' does not exist.\n", dirArc)
			os.Exit(1)
		}
		// fmt.Println(*a.Archive)
		// fmt.Println(a.CheckExistence())
		//a.CheckExistence()

	}

}
