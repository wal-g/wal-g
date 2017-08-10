package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/katie31/wal-g"
	"log"
	"os"
	"path/filepath"
	"time"
)

var help bool
var l *log.Logger
var helpMsg = "\tbackup-fetch\tfetch a backup from S3\n" +
	"\tbackup-push\tstarts and uploads a finished backup to S3\n" +
	"\twal-fetch\tfetch a WAL file from S3\n" +
	"\twal-push\tupload a WAL file to S3\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
	l = log.New(os.Stderr, "", 0)
}

func main() {
	// Configure and start S3 session with bucket, region, and path names.
	// Checks that environment variables are properly set.
	flag.Parse()
	all := flag.Args()
	if len(all) < 2 {
		l.Fatalf("Please choose a command:\n%s", helpMsg)
	}
	command := all[0]
	dirArc := all[1]

	var backupName string
	if len(all) == 3 {
		backupName = all[2]
	}

	tu, pre, err := walg.Configure()
	if err != nil {
		log.Fatalf("FATAL: %+v\n", err)
	}

	fmt.Println("BUCKET:", *pre.Bucket)
	fmt.Println("SERVER:", *pre.Server)

	if command == "backup-fetch" {
		var allKeys []string
		var keys []string
		var bk *walg.Backup

		// Check if BACKUPNAME exists and if it does extract to DIRARC.
		if backupName != "LATEST" {
			bk = &walg.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
				Name:   aws.String(backupName),
			}
			bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

			// fmt.Println("NEWDIR:", dirArc)
			// fmt.Println("PATH:", *bk.Path)
			// fmt.Println("NAME:", *bk.Name)
			// fmt.Println("JSON:", *bk.Js)
			// fmt.Println(bk.CheckExistence())
			exists, err := bk.CheckExistence()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			if exists {
				allKeys, err := bk.GetKeys()
				if err != nil {
					log.Fatalf("%+v\n", err)
				}
				keys = allKeys[:len(allKeys)-1]

			} else {
				log.Fatalf("Backup '%s' does not exist.\n", *bk.Name)
			}

			// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to DIRARC.
		} else if backupName == "LATEST" {
			bk = &walg.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
			}

			latest, err := bk.GetLatest()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			bk.Name = aws.String(latest)
			allKeys, err = bk.GetKeys()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			keys = allKeys[:len(allKeys)-1]
		}

		f := &walg.FileTarInterpreter{
			NewDir: dirArc,
		}

		out := make([]walg.ReaderMaker, len(keys))
		for i, key := range keys {
			s := &walg.S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(key),
				FileFormat: walg.CheckType(key),
			}
			out[i] = s
		}

		// Extract all compressed tar members except `pg_control.tar.lz4`.
		err = walg.ExtractAll(f, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			log.Fatalf("%v\n", serr)
		} else if err != nil {
			log.Fatalf("%+v\n", err)
		}

		// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
		name := *bk.Path + *bk.Name + "/tar_partitions/pg_control.tar.lz4"
		pgControl := &walg.Archive{
			Prefix:  pre,
			Archive: aws.String(name),
		}

		exists, err := pgControl.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if exists {
			sentinel := make([]walg.ReaderMaker, 1)
			sentinel[0] = &walg.S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(name),
				FileFormat: walg.CheckType(name),
			}
			err := walg.ExtractAll(f, sentinel)
			if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
				log.Fatalf("%v\n", serr)
			} else if err != nil {
				log.Fatalf("%+v\n", err)
			}
			fmt.Printf("\nBackup extraction complete.\n")
		} else {
			log.Fatal("Corrupt backup: missing pg_control")
		}
	} else if command == "wal-fetch" {
		// Fetch and decompress a WAL file from S3.
		a := &walg.Archive{
			Prefix:  pre,
			Archive: aws.String(*pre.Server + "/wal_005/" + dirArc + ".lzo"),
		}

		// Check existence of compressed LZO WAL file
		exists, err := a.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			arch, err := a.GetArchive()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			f, err := os.Create(backupName)
			if err != nil {
				log.Fatalf("%v\n", err)
			}

			err = walg.DecompressLzo(f, arch)
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			f.Close()
		} else if !exists {
			// Check existence of compressed LZ4 WAL file
			a.Archive = aws.String(*pre.Server + "/wal_005/" + dirArc + ".lz4")
			exists, err = a.CheckExistence()
			if err != nil {
				log.Fatalf("%+v\n", err)
			}

			if exists {
				arch, err := a.GetArchive()
				if err != nil {
					log.Fatalf("%+v\n", err)
				}
				f, err := os.Create(backupName)
				if err != nil {
					log.Fatalf("%v\n", err)
				}

				err = walg.DecompressLz4(f, arch)
				if err != nil {
					log.Fatalf("%+v\n", err)
				}
				f.Close()
			} else {
				log.Fatalf("Archive '%s' does not exist.\n", dirArc)
			}
		}
	} else if command == "wal-push" {
		// Upload a WAL file to S3.
		path, err := tu.UploadWal(dirArc)
		if re, ok := err.(walg.Lz4Error); ok {
			log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, re)
		} else if err != nil {
			log.Printf("upload: could not upload '%s' after %v retries\n", path, tu.MaxRetries)
			log.Fatalf("FATAL%+v\n", err)
		}
	} else if command == "backup-push" {
		// Connect to postgres and start/finish a nonexclusive backup.
		bundle := &walg.Bundle{
			MinSize: int64(1000000000), //MINSIZE = 1GB
		}
		c, err := walg.Connect()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		lbl, sc, err := walg.QueryFile(c, time.Now().String())
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		n, err := walg.FormatName(lbl)
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		// Start a new tar bundle and walk the DIRARC directory and upload to S3.
		bundle.Tbm = &walg.S3TarBallMaker{
			BaseDir:  filepath.Base(dirArc),
			Trim:     dirArc,
			BkupName: n,
			Tu:       tu,
		}

		bundle.NewTarBall()
		fmt.Println("Walking ...")
		err = filepath.Walk(dirArc, bundle.TarWalker)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		err = bundle.Tb.CloseTar()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		// Write and upload postgres `backup_label` and `tablespace_map` files
		err = bundle.HandleLabelFiles(lbl, sc)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		// Upload `pg_control`.
		err = bundle.HandleSentinel()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		// Wait for all uploads to finish.
		err = bundle.Tb.Finish()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
	} else {
		l.Fatalf("Command '%s' is unsupported by WAL-G.", command)
	}

}
