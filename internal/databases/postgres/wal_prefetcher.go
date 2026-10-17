package postgres

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
)

type WalPrefetcher interface {
	Prefetch(ctx context.Context, reader internal.StorageFolderReader, walFileName string, location string)
}

type NopPrefetcher struct {
}

func (p NopPrefetcher) Prefetch(_ context.Context, reader internal.StorageFolderReader, walFileName string, location string) {

}

type RegularPrefetcher struct {
}

func (p RegularPrefetcher) Prefetch(_ context.Context, _ internal.StorageFolderReader, walFileName string, location string) {
	if !checkPrefetchPossible(walFileName) {
		return
	}
	prefetchArgs := []string{"wal-prefetch", walFileName, location}
	if conf.CfgFile != "" {
		prefetchArgs = append(prefetchArgs, "--config", conf.CfgFile)
	}
	storagePrefix := viper.GetString(conf.StoragePrefixSetting)
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

func (p DaemonPrefetcher) Prefetch(ctx context.Context, reader internal.StorageFolderReader, walFileName string, location string) {
	if !checkPrefetchPossible(walFileName) {
		return
	}

	go func() {
		tracelog.DebugLogger.Printf("Invoking daemon WAL-prefetch (%s)", walFileName)
		prefetchCount, err := getDaemonPrefetchCount()
		if err != nil {
			tracelog.ErrorLogger.Printf("WAL-prefetch (%s): %v", walFileName, err)
			return
		}
		err = HandleWALPrefetchCount(ctx, reader, walFileName, location, prefetchCount)
		if err != nil {
			tracelog.ErrorLogger.Printf("WAL-prefetch (%s): %v", walFileName, err)
		}
	}()
}

// getDaemonPrefetchCount returns the total number of WAL segments to prefetch:
// download concurrency plus the extra amount configured by WALG_DAEMON_WAL_PREFETCH_COUNT
func getDaemonPrefetchCount() (int, error) {
	concurrency, err := conf.GetMaxDownloadConcurrency()
	if err != nil {
		return 0, fmt.Errorf("get max concurrency: %v", err)
	}
	return concurrency + conf.GetDaemonWALPrefetchCount(), nil
}

func checkPrefetchPossible(walFileName string) bool {
	concurrency, err := conf.GetMaxDownloadConcurrency()
	if err != nil {
		tracelog.ErrorLogger.Printf("WAL-prefetch: get max concurrency: %v", err)
		return false
	}
	return !strings.Contains(walFileName, "history") &&
		!strings.Contains(walFileName, "partial") &&
		(concurrency != 1 || // There will be nothing to prefetch anyway
			conf.GetDaemonWALPrefetchCount() > 0)
}
