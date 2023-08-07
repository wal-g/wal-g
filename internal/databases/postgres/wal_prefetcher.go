package postgres

import (
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
)

type WalPrefetcher interface {
	Prefetch(reader internal.StorageFolderReader, walFileName string, location string)
}

type NopPrefetcher struct {
}

func (p NopPrefetcher) Prefetch(reader internal.StorageFolderReader, walFileName string, location string) {

}

type RegularPrefetcher struct {
}

func (p RegularPrefetcher) Prefetch(_ internal.StorageFolderReader, walFileName string, location string) {
	if !checkPrefetchPossible(walFileName) {
		return
	}
	prefetchArgs := []string{"wal-prefetch", walFileName, location}
	if internal.CfgFile != "" {
		prefetchArgs = append(prefetchArgs, "--config", internal.CfgFile)
	}
	storagePrefix := viper.GetString(internal.StoragePrefixSetting)
	if storagePrefix != "" {
		prefetchArgs = append(prefetchArgs, "--walg-storage-prefix", storagePrefix)
	}
	cmd := exec.Command(os.Args[0], prefetchArgs...)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()

	if err != nil {
		tracelog.ErrorLogger.Println("WAL-prefetch failed: ", err)
	}
}

type DaemonPrefetcher struct {
}

func (p DaemonPrefetcher) Prefetch(reader internal.StorageFolderReader, walFileName string, location string) {
	if !checkPrefetchPossible(walFileName) {
		return
	}

	go func() {
		tracelog.DebugLogger.Printf("Invoking daemon WAL-prefetch (%s)", walFileName)
		err := HandleWALPrefetch(reader, walFileName, location)
		if err != nil {
			tracelog.ErrorLogger.Printf("WAL-prefetch (%s): %v", walFileName, err)
		}
	}()
}

func checkPrefetchPossible(walFileName string) bool {
	concurrency, err := internal.GetMaxDownloadConcurrency()
	if err != nil {
		tracelog.ErrorLogger.Printf("WAL-prefetch: get max concurrency: %v", err)
		return false
	}
	return !strings.Contains(walFileName, "history") &&
		!strings.Contains(walFileName, "partial") &&
		concurrency != 1 // There will be nothing to prefetch anyway
}
