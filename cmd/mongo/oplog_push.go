package mongo

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
)

const oplogPushShortDescription = ""

// oplogPushCmd represents the continuous oplog archiving procedure
var oplogPushCmd = &cobra.Command{
	Use:   "oplog-push",
	Short: oplogPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		defer func() {
			signal.Stop(ch)
			cancel()
		}()
		go func() {
			select {
			case s := <-ch:
				tracelog.InfoLogger.Printf("Received %s signal. Shutting down", s.String())
				cancel()
			case <-ctx.Done():
			}
		}()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		mongo.HandleOplogPush(ctx, &mongo.Uploader{Uploader: uploader})
	},
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
