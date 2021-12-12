package pgbackrest

import (
	"path"
	"sort"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleWalShow(rootFolder storage.Folder, stanza string, outputWriter postgres.WalShowOutputWriter) error {
	archiveName, err := GetArchiveName(rootFolder, stanza)
	if err != nil {
		return err
	}

	archiveFolder := rootFolder.GetSubFolder(WalArchivePath).GetSubFolder(stanza).GetSubFolder(*archiveName)
	walFiles, err := getWalFiles(archiveFolder)
	if err != nil {
		return err
	}

	walSegments, err := getWalSegments(walFiles)
	if err != nil {
		return err
	}
	walSequencesByTimelines := getWalSequencesByTimelines(walSegments)
	
	var timelineInfos []*postgres.TimelineInfo
	for _, segmentsSequence := range walSequencesByTimelines {
		historyRecords, err := postgres.GetTimeLineHistoryRecords(segmentsSequence.TimelineID, archiveFolder)
		if err != nil {
			if _, ok := err.(postgres.HistoryFileNotFoundError); !ok {
				tracelog.ErrorLogger.Fatalf("Error while loading .history file %v\n", err)
			}
		}

		info, err := postgres.NewTimelineInfo(segmentsSequence, historyRecords)
		tracelog.ErrorLogger.FatalfOnError("Error while creating TimeLineInfo %v\n", err)
		timelineInfos = append(timelineInfos, info)
	}
	
	sort.Slice(timelineInfos, func(i, j int) bool {
		return timelineInfos[i].ID < timelineInfos[j].ID
	})

	return outputWriter.Write(timelineInfos)
	
}

func getWalSequencesByTimelines(segments []postgres.WalSegmentDescription) map[uint32]*postgres.WalSegmentsSequence {
	segmentsByTimelines := make(map[uint32]*postgres.WalSegmentsSequence)
	for _, segment := range segments {
		if timelineInfo, ok := segmentsByTimelines[segment.Timeline]; ok {
			timelineInfo.AddWalSegmentNo(segment.Number)
			continue
		}
		segmentsByTimelines[segment.Timeline] = postgres.NewSegmentsSequence(segment.Timeline, segment.Number)
	}
	return segmentsByTimelines

}

func getWalSegments(filenames []string) ([]postgres.WalSegmentDescription, error) {
	var segments []postgres.WalSegmentDescription
	for _, filename := range filenames {
		extension := path.Ext(filename)
		if extension == ".backup" || extension == ".history" {
			continue
		}

		segmentName := strings.Split(path.Base(filename), "-")[0]
		segment, err := postgres.NewWalSegmentDescription(segmentName)
		if err != nil {
			return nil, err
		}

		segments = append(segments, segment)
	}
	return segments, nil
}

func getWalFiles(archiveFolder storage.Folder) ([]string, error) {
	var walFiles []string
	_, walDirectories, err := archiveFolder.ListFolder()
	if err != nil {
		return nil, err
	}

	for _, walDirectory := range walDirectories {
		files, _, err := walDirectory.ListFolder()
		if err != nil {
			return nil, err
		}

		for _, file := range files {
			walFiles = append(walFiles, path.Join(walDirectory.GetPath(), file.GetName()))
		}
	}
	return walFiles, nil
}
