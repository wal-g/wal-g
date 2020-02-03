package mongo

import (
	"fmt"
	"io"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
)

// HandleStreamPush prints sentinel contents.
func HandleBackupShow(downloader archive.Downloader, backup string, marshaller archive.SentinelMarshalFunc, output io.Writer) error {
	sentinel, err := downloader.Sentinel(backup)
	if err != nil {
		return err
	}
	report, err := marshaller(sentinel)
	if err != nil {
		return fmt.Errorf("can not marshal sentinel: %w", err)
	}

	if _, err := fmt.Fprintf(output, "%s\n", report); err != nil {
		return err
	}
	return nil
}
