package st

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const transferShortDescription = "Moves objects from one storage to another (Postgres only)"

// transferCmd represents the transfer command
var transferCmd = &cobra.Command{
	Use:   "transfer parent_dir_path --source='source_storage' [--target='target_storage']",
	Short: transferShortDescription,
	Long: "The command is usually used to move objects from a failover storage to the primary one, when it becomes alive. " +
		"By default, objects that exist in both storages are neither overwritten in the target storage nor deleted from the source one.",
	Args: cobra.RangeArgs(1, 1),
	Run: func(cmd *cobra.Command, args []string) {
		err := validateFlags()
		if err != nil {
			tracelog.ErrorLogger.FatalError(fmt.Errorf("invalid flags: %w", err))
		}

		sourceFolder, err := multistorage.ConfigureStorageFolder(transferSourceStorage)
		if err != nil {
			tracelog.ErrorLogger.FatalError(fmt.Errorf("can't configure source storage folder: %w", err))
		}
		targetFolder, err := multistorage.ConfigureStorageFolder(targetStorage)
		if err != nil {
			tracelog.ErrorLogger.FatalError(fmt.Errorf("can't configure target storage folder: %w", err))
		}

		parentDirPath := args[0]
		err = transferFiles(sourceFolder, targetFolder, parentDirPath)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
	},
}

var (
	transferSourceStorage            string
	transferOverwrite                bool
	transferConcurrency              int
	transferMax                      int
	transferAppearanceChecks         uint
	transferAppearanceChecksInterval time.Duration
)

func init() {
	transferCmd.Flags().StringVarP(&transferSourceStorage, "source", "s", "",
		"storage name to move files from. Use 'default' to select the primary storage")
	transferCmd.Flags().BoolVarP(&transferOverwrite, "overwrite", "o", false,
		"whether to overwrite already existing files in the target storage and remove them from the source one")
	transferCmd.Flags().IntVarP(&transferConcurrency, "concurrency", "c", 10,
		"number of concurrent workers to move files. Value 1 turns concurrency off")
	transferCmd.Flags().IntVarP(&transferMax, "max", "m", -1,
		"max number of files to move in this run. Negative numbers turn the limit off")
	transferCmd.Flags().UintVar(&transferAppearanceChecks, "appearance-checks", 3,
		"number of times to check if a file is appeared for reading in the target storage after writing it. Value 0 turns checking off")
	transferCmd.Flags().DurationVar(&transferAppearanceChecksInterval, "appearance-checks-interval", time.Second,
		"minimum time interval between performing checks for files to appear in the target storage")

	StorageToolsCmd.AddCommand(transferCmd)
}

func validateFlags() error {
	if transferSourceStorage == "" {
		return fmt.Errorf("source storage must be specified")
	}
	if transferSourceStorage == "all" {
		return fmt.Errorf("an explicit source storage must be specified instead of 'all'")
	}
	if targetStorage == "all" {
		return fmt.Errorf("an explicit target storage must be specified instead of 'all'")
	}
	if transferSourceStorage == targetStorage {
		return fmt.Errorf("source and target storages must be different")
	}
	if transferConcurrency < 1 {
		return fmt.Errorf("concurrency level must be >= 1 (which turns it off)")
	}
	return nil
}

func transferFiles(source, target storage.Folder, parentDirPath string) error {
	files, err := listFilesToMove(source, target, parentDirPath)
	if err != nil {
		return err
	}

	fileStates := make(chan transferFileState, len(files))
	for _, f := range files {
		fileStates <- transferFileState{
			path: f.GetName(),
		}
	}

	workersNum := utility.Min(transferConcurrency, len(files))
	errs := make(chan error, workersNum)

	workersWG := new(sync.WaitGroup)
	workersWG.Add(workersNum)
	for i := 0; i < workersNum; i++ {
		go moveFilesWorker(source, target, fileStates, errs, workersWG)
	}

	errsWG := new(sync.WaitGroup)
	errsWG.Add(1)
	errsNum := 0
	go func() {
		defer errsWG.Done()
		for e := range errs {
			errsNum++
			tracelog.ErrorLogger.PrintError(e)
		}
	}()

	workersWG.Wait()
	close(errs)
	errsWG.Wait()

	if errsNum > 0 {
		return fmt.Errorf("finished with %d errors", errsNum)
	}

	return nil
}

func listFilesToMove(source, target storage.Folder, parentDirPath string) ([]storage.Object, error) {
	isSubDir := func(dirPath string) bool {
		return strings.HasPrefix(dirPath, parentDirPath)
	}

	targetFiles, err := storage.ListFolderRecursivelyWithFilter(target, isSubDir)
	if err != nil {
		return nil, fmt.Errorf("can't list files in the target storage: %w", err)
	}
	sourceFiles, err := storage.ListFolderRecursivelyWithFilter(source, isSubDir)
	if err != nil {
		return nil, fmt.Errorf("can't list files in the source storage: %w", err)
	}
	tracelog.InfoLogger.Printf("Total files in the source storage: %d", len(sourceFiles))

	missingFiles := make(map[string]storage.Object, len(sourceFiles))
	for _, sourceFile := range sourceFiles {
		missingFiles[sourceFile.GetName()] = sourceFile
	}
	for _, targetFile := range targetFiles {
		if transferOverwrite {
			sourceFile := missingFiles[targetFile.GetName()]
			if sourceFile.GetSize() != targetFile.GetSize() {
				tracelog.WarningLogger.Printf(
					"File present in both storages and its size is different: %q (source %d bytes VS target %d bytes)",
					targetFile.GetName(),
					sourceFile.GetSize(),
					targetFile.GetSize(),
				)
			}
		} else {
			delete(missingFiles, targetFile.GetName())
		}
	}
	tracelog.InfoLogger.Printf("Files missing in the target storage: %d", len(missingFiles))

	if transferMax < 0 {
		transferMax = math.MaxInt
	}
	count := 0
	limitedFiles := make([]storage.Object, 0, utility.Min(transferMax, len(missingFiles)))
	for _, file := range missingFiles {
		if count >= transferMax {
			break
		}
		limitedFiles = append(limitedFiles, file)
		count++
	}
	tracelog.InfoLogger.Printf("Files will be transferred: %d", len(limitedFiles))

	return limitedFiles, nil
}

type transferFileState struct {
	path            string
	copied          bool
	prevCheck       time.Time
	performedChecks uint
}

func moveFilesWorker(
	source, target storage.Folder,
	fileStates chan transferFileState,
	errs chan error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		var state transferFileState

		select {
		case state = <-fileStates:
			// Go on
		default:
			// No more files to process, exit
			return
		}

		newState, err := moveFileStep(source, target, state)
		if err != nil {
			errs <- fmt.Errorf("error with file %q: %w", state.path, err)
			continue
		}

		if newState != nil {
			// Enqueue file again to process it later
			fileStates <- *newState
			continue
		}

		tracelog.InfoLogger.Printf("File is transferred (%d left): %q", len(fileStates), state.path)
	}
}

func moveFileStep(
	source, target storage.Folder,
	state transferFileState,
) (newState *transferFileState, err error) {
	switch {
	case !state.copied:
		err = copyToTarget(source, target, state)
		if err != nil {
			return nil, err
		}
		state.copied = true
		return &state, nil

	case state.copied:
		var appeared bool

		skipCheck := transferAppearanceChecks == 0
		if skipCheck {
			appeared = true
		} else {
			appeared, err = checkForAppearance(target, state)
			if err != nil {
				return nil, err
			}
		}

		if appeared {
			err = source.DeleteObjects([]string{state.path})
			if err != nil {
				return nil, fmt.Errorf("can't delete file from the source storage: %w", err)
			}
		} else {
			state.prevCheck = time.Now()
			state.performedChecks++
			if state.performedChecks >= transferAppearanceChecks {
				return nil, fmt.Errorf("couldn't wait for the file to appear in the target storage (%d checks performed)", transferAppearanceChecks)
			}
			tracelog.WarningLogger.Printf("Written file hasn't appeared in the target storage (check %d of %d)", state.performedChecks, transferAppearanceChecks)
			return &state, nil
		}
	}

	return nil, nil
}

func copyToTarget(source storage.Folder, target storage.Folder, state transferFileState) error {
	content, err := source.ReadObject(state.path)
	if err != nil {
		return fmt.Errorf("can't read file from the source storage: %w", err)
	}
	defer func() {
		_ = content.Close()
	}()

	err = target.PutObject(state.path, content)
	if err != nil {
		return fmt.Errorf("can't write file to the target storage: %w", err)
	}

	return nil
}

func checkForAppearance(target storage.Folder, state transferFileState) (appeared bool, err error) {
	nextCheck := state.prevCheck.Add(transferAppearanceChecksInterval)
	waitTime := nextCheck.Sub(time.Now())
	if waitTime > 0 {
		time.Sleep(waitTime)
	}

	appeared, err = target.Exists(state.path)
	if err != nil {
		return false, fmt.Errorf("can't check if file exists in the target storage: %w", err)
	}
	return appeared, nil
}
