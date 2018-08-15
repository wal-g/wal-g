package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
)

var profile bool
var mem bool
var noOp bool
var data []string

func init() {
	flag.BoolVar(&profile, "p", false, "Profiler (false on default)")
	flag.BoolVar(&mem, "m", false, "Memory profiler (false on default)")
	flag.BoolVar(&noOp, "n", false, "NOP extractor (write on default)")

}

func main() {
	flag.Parse()
	all := flag.Args()
	dir := all[0]
	data := all[1:]

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		f.Close()
	}
	out := make([]walg.ReaderMaker, len(data))
	for i, val := range data {
		if strings.HasPrefix(val, "https://") {
			tls := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}

			h := &testtools.HTTPReaderMaker{
				Client:     &http.Client{Transport: tls},
				Key:        val,
			}

			out[i] = h
		} else {
			f := &testtools.FileReaderMaker{
				Key:        val,
			}
			out[i] = f
		}
	}

	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if !noOp {
		ft := &walg.FileTarInterpreter{
			NewDir: dir,
		}

		testtools.MakeDir(ft.NewDir)
		err := walg.ExtractAll(ft, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			fmt.Println(serr.Error())
			os.Exit(1)
		} else if err != nil {
			panic(err)
		}

	} else {
		np := &testtools.NOPTarInterpreter{}
		err := walg.ExtractAll(np, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			fmt.Println(serr.Error())
			os.Exit(1)
		} else if err != nil {
			panic(err)
		}

	}
}
