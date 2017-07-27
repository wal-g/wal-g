package main

import (
	"flag"
	"fmt"
	"github.com/katie31/wal-g"
	"github.com/katie31/wal-g/tools"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"
)

var profile bool
var mem bool
var nop bool
var s3 bool
var outDir string

func init() {
	flag.BoolVar(&profile, "p", false, "Profiler (false on default)")
	flag.BoolVar(&mem, "m", false, "Memory profiler (false on default)")
	flag.BoolVar(&nop, "n", false, "Use a NOP writer (false on default).")
	flag.BoolVar(&s3, "s", false, "Upload compressed tar files to s3 (write to disk on default)")
	flag.StringVar(&outDir, "out", "", "Directory compressed tar files will be written to (unset on default)")
}

func main() {
	flag.Parse()
	all := flag.Args()
	part, err := strconv.Atoi(all[0])
	if err != nil {
		panic(err)
	}
	in := all[1]

	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	bundle := &walg.Bundle{
		MinSize: int64(part),
	}

	if nop {
		bundle.Tbm = &tools.NOPTarBallMaker{
			BaseDir: filepath.Base(in),
			Trim:    in,
			Nop:     true,
		}
	} else if !s3 && outDir == "" {
		fmt.Printf("Please provide a directory to write to.\n")
		os.Exit(1)
	} else if !s3 {
		bundle.Tbm = &tools.FileTarBallMaker{
			BaseDir: filepath.Base(in),
			Trim:    in,
			Out:     outDir,
		}
		os.MkdirAll(outDir, 0766)

	} else if s3 {
		tu, _ := walg.Configure()
		c, err := walg.Connect()
		if err != nil {
			panic(err)
		}
		lbl, sc := walg.QueryFile(c, time.Now().String())
		n, err := walg.FormatName(lbl)
		if err != nil {
			panic(err)
		}

		bundle.Tbm = &walg.S3TarBallMaker{
			BaseDir:  filepath.Base(in),
			Trim:     in,
			BkupName: n,
			Tu:       tu,
		}

		bundle.NewTarBall()
		bundle.HandleLabelFiles(lbl, sc)

	}

	bundle.NewTarBall()
	defer walg.TimeTrack(time.Now(), "MAIN")
	fmt.Println("Walking ...")
	err = filepath.Walk(in, bundle.TarWalker)
	if err != nil {
		panic(err)
	}
	err = bundle.Tb.CloseTar()
	if err != nil {
		panic(err)
	}
	bundle.Tb.Finish()

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		f.Close()
	}

}
