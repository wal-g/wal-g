package st

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const folderListShortDescription = "Prints objects in the provided storage folder"
const recursiveFlag = "recursive"
const recursiveShortHand = "r"
const allVersionsFlag = "all-versions"

// folderListCmd represents the folderList command
var folderListCmd = &cobra.Command{
	Use:   "ls [relative folder path]",
	Short: folderListShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		tracelog.ErrorLogger.FatalOnError(runFolderList(cmd, args))
	},
}

func runFolderList(cmd *cobra.Command, args []string) error {
	withPrefix := cmd.Flags().Changed(prefixFlag)
	if withPrefix {
		if objectPrefix == "" {
			return fmt.Errorf("--prefix must not be empty")
		}
		if glob || recursive || showAllVersions {
			return fmt.Errorf("--prefix cannot be combined with --glob, --recursive or --all-versions")
		}
	}
	var path string
	if len(args) > 0 {
		path = args[0]
	} else {
		path = ""
	}

	ctx := cmd.Context()
	var prefixErr error
	err := exec.OnStorage(ctx, targetStorage, func(folder storage.Folder) error {
		if withPrefix {
			if path != "" {
				folder = folder.GetSubFolder(path)
			}
			err := storagetools.HandleFolderListWithPrefix(ctx, folder, objectPrefix)
			// OnStorage("all") tolerates individual failures for other storage commands.
			// A filtered inventory is complete only if every selected storage succeeds.
			prefixErr = errors.Join(prefixErr, err)
			return err
		}
		if showAllVersions {
			storage.SetShowAllVersions(folder, true)
		}
		if glob {
			return storagetools.HandleFolderListWithGlob(ctx, folder, path, recursive)
		}
		subfolder := folder.GetSubFolder(path)
		return storagetools.HandleFolderList(ctx, subfolder, recursive)
	})
	if prefixErr != nil {
		return prefixErr
	}
	return err
}

var recursive bool
var showAllVersions bool
var objectPrefix string

func init() {
	folderListCmd.Flags().StringVar(&objectPrefix, prefixFlag, "", "List current objects with this literal name prefix (S3, GCS, Azure)")
	folderListCmd.Flags().BoolVarP(&recursive, recursiveFlag, recursiveShortHand, false, "List folder recursively")
	folderListCmd.Flags().BoolVar(&showAllVersions, allVersionsFlag, false, "Show all object versions including deleted (S3 with versioning)")
	StorageToolsCmd.AddCommand(folderListCmd)
}
