package postgres

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

type walSendTimeline struct {
	end LSN
}

// refreshWalSegments builds a compact timeline catalog. ListFolder visits all
// retained WAL objects (potentially millions), so this is only called for an
// explicit IDENTIFY_SYSTEM/START_REPLICATION or while negotiating a missing
// segment, never for each streamed segment. Concurrent callers share one scan
// per poll interval; the retained index contains one entry per timeline.
func (s *walSendServer) refreshWalSegments(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timelines != nil && time.Since(s.listRefreshedAt) < s.pollInterval {
		return nil
	}
	objects, _, err := s.walFolder.ListFolder(ctx)
	if err != nil {
		return fmt.Errorf("list WAL archive: %w", err)
	}
	timelines := make(map[uint32]walSendTimeline)
	for _, obj := range objects {
		name := obj.GetName()
		if i := strings.LastIndexByte(name, '.'); i >= 0 && compression.FindDecompressor(name[i+1:]) != nil {
			name = name[:i]
		}
		if strings.HasSuffix(name, ".history") {
			hex := strings.TrimSuffix(name, ".history")
			if tli, err := strconv.ParseUint(hex, 16, 32); err == nil && len(hex) == 8 && tli > 0 {
				if _, ok := timelines[uint32(tli)]; !ok {
					timelines[uint32(tli)] = walSendTimeline{}
				}
			}
			continue
		}
		partial := strings.HasSuffix(name, ".partial")
		name = strings.TrimSuffix(name, ".partial")
		tli, segNo, err := ParseWALFilename(name)
		if err != nil || tli == 0 {
			continue
		}
		end := LSN((segNo + 1) * WalSegmentSize)
		if partial {
			// The name alone proves nothing beyond the start of this segment.
			end = LSN(segNo * WalSegmentSize)
		}
		entry := timelines[tli]
		entry.end = max(entry.end, end)
		timelines[tli] = entry
	}
	s.timelines = timelines
	s.listRefreshedAt = time.Now()
	return nil
}

func (s *walSendServer) newerTimelines(after uint32) []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var timelines []uint32
	for tli := range s.timelines {
		if tli > after {
			timelines = append(timelines, tli)
		}
	}
	sort.Slice(timelines, func(i, j int) bool { return timelines[i] > timelines[j] })
	return timelines
}

type walSendHistoryEntry struct {
	timeline uint32
	end      LSN
}

type walSendTimelineSwitch struct {
	timeline       uint32 // immediate successor, which need not be ancestor + 1
	lsn            LSN
	sourceTimeline uint32 // descendant whose segment contains the old prefix
}

func (s *walSendServer) readTimelineHistory(ctx context.Context, timeline uint32) ([]walSendHistoryEntry, error) {
	name := fmt.Sprintf("%08X.history", timeline)
	content, err := internal.DownloadAndDecompressStorageFile(ctx, s.reader, name)
	if err != nil {
		return nil, err
	}
	defer utility.LoggedClose(content, "wal-send: close "+name)
	const maxHistorySize = 1 << 20
	data, err := io.ReadAll(io.LimitReader(content, maxHistorySize+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if len(data) > maxHistorySize {
		return nil, fmt.Errorf("timeline history %s is too large", name)
	}
	var history []walSendHistoryEntry
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var parent uint32
	var end LSN
	for scanner.Scan() {
		line := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("malformed timeline history %s", name)
		}
		tli, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil || tli <= uint64(parent) || tli >= uint64(timeline) {
			return nil, fmt.Errorf("invalid ancestor in %s", name)
		}
		lsn, err := ParseLSN(fields[1])
		if err != nil || lsn == 0 || lsn < end {
			return nil, fmt.Errorf("invalid switch position in %s", name)
		}
		parent, end = uint32(tli), lsn
		history = append(history, walSendHistoryEntry{timeline: parent, end: end})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
	}
	if len(history) == 0 {
		return nil, fmt.Errorf("empty timeline history %s", name)
	}
	return history, nil
}

func walSendHistorySwitch(history []walSendHistoryEntry, candidate, ancestor uint32) *walSendTimelineSwitch {
	for i, entry := range history {
		if entry.timeline == ancestor {
			next := candidate
			if i+1 < len(history) {
				next = history[i+1].timeline
			}
			return &walSendTimelineSwitch{timeline: next, lsn: entry.end, sourceTimeline: candidate}
		}
	}
	return nil
}

// A missing history may simply not have been uploaded yet. Every other read or
// parse error is returned; neither case authorizes a timeline transition.
func (st *walSendStream) findSuccessorTimeline(ctx context.Context, timeline uint32, pos LSN) (*walSendTimelineSwitch, error) {
	s := st.client.server
	identify, err := s.identifySystem(ctx)
	if err != nil {
		return nil, err
	}
	if identify.timeline <= timeline {
		return nil, nil
	}
	history, err := s.readTimelineHistory(ctx, identify.timeline)
	if err != nil {
		return nil, err
	}
	next := walSendHistorySwitch(history, identify.timeline, timeline)
	if next != nil && next.lsn < pos {
		return nil, fmt.Errorf("archive timeline %d forked from timeline %d at %s, before requested position %s",
			identify.timeline, timeline, next.lsn, pos)
	}
	return next, nil
}
