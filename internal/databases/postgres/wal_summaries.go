package postgres

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/utility"
)

// WAL summaries are produced by the walsummarizer (PG17+, summarize_wal=on) and
// live under $PGDATA/pg_wal/summaries. Filename encodes timeline + [start,end)
// LSN; body is a BlockRefTable per src/common/blkreftable.c.

const (
	walSummariesDir = "pg_wal/summaries"

	blockRefTableMagic uint32 = 0x652b137b

	// Matches BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY / MAX_ENTRIES_PER_CHUNK in blkreftable.c
	blocksPerChunk      = 1 << 16
	blocksPerEntry      = 8 * 2 // BITS_PER_BYTE * sizeof(uint16)
	maxEntriesPerChunk  = blocksPerChunk / blocksPerEntry
	serializedEntrySize = 24 // spcOid+dbOid+relNumber+forknum+limit_block+nchunks

	invalidBlockNumber uint32 = 0xFFFFFFFF
	mainForkNum        int32  = 0
)

var walSummaryFilenameRegexp = regexp.MustCompile(
	`^([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})\.summary$`)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// walSummaryFile names one on-disk summary, decoded from its filename.
type walSummaryFile struct {
	path     string
	timeline uint32
	startLSN LSN
	endLSN   LSN
}

// ReadWalSummariesForRange lists summaries under pgDataDir, picks the ones
// covering [firstUsedLSN, firstNotUsedLSN) on the given timeline, verifies
// contiguous coverage, parses them in chronological order, and returns the
// combined set of changed MAIN_FORKNUM blocks as a PagedFileDeltaMap.
func ReadWalSummariesForRange(pgDataDir string, timeline uint32,
	firstUsedLSN, firstNotUsedLSN LSN) (PagedFileDeltaMap, error) {
	files, err := listWalSummaryFiles(filepath.Join(pgDataDir, walSummariesDir))
	if err != nil {
		return nil, err
	}
	selected, err := selectWalSummariesForRange(files, timeline, firstUsedLSN, firstNotUsedLSN)
	if err != nil {
		return nil, err
	}

	// Process chronologically so truncation semantics combine correctly.
	state := make(map[relForkKey]*roaring.Bitmap)
	for _, f := range selected {
		tracelog.InfoLogger.Printf("Reading WAL summary %s", filepath.Base(f.path))
		if err := parseWalSummaryFile(f.path, state); err != nil {
			return nil, errors.Wrapf(err, "parsing %s", f.path)
		}
	}

	// Project main-fork entries into wal-g's PagedFileDeltaMap, which is keyed
	// by RelFileNode (no fork). Other forks aren't paged-file-incrementable in
	// wal-g's current model; same restriction the legacy delta path has.
	deltaMap := NewPagedFileDeltaMap()
	for key, blocks := range state {
		if key.forkNum != mainForkNum {
			continue
		}
		if blocks.IsEmpty() {
			continue
		}
		deltaMap[key.rel] = blocks
	}
	return deltaMap, nil
}

type relForkKey struct {
	rel     walparser.RelFileNode
	forkNum int32
}

func listWalSummaryFiles(dir string) ([]walSummaryFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "reading %s", dir)
	}
	out := make([]walSummaryFile, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		f, ok := parseWalSummaryFilename(ent.Name())
		if !ok {
			continue
		}
		f.path = filepath.Join(dir, ent.Name())
		out = append(out, f)
	}
	return out, nil
}

func parseWalSummaryFilename(name string) (walSummaryFile, bool) {
	m := walSummaryFilenameRegexp.FindStringSubmatch(name)
	if m == nil {
		return walSummaryFile{}, false
	}
	parts := make([]uint32, 5)
	for i := 0; i < 5; i++ {
		v, err := strconv.ParseUint(m[i+1], 16, 32)
		if err != nil {
			return walSummaryFile{}, false
		}
		parts[i] = uint32(v)
	}
	return walSummaryFile{
		timeline: parts[0],
		startLSN: LSN(uint64(parts[1])<<32 | uint64(parts[2])),
		endLSN:   LSN(uint64(parts[3])<<32 | uint64(parts[4])),
	}, true
}

// selectWalSummariesForRange filters to a timeline, keeps files overlapping
// [firstUsedLSN, firstNotUsedLSN), sorts by startLSN, and asserts that the
// union fully covers the requested range with no gaps.
func selectWalSummariesForRange(files []walSummaryFile, timeline uint32,
	firstUsedLSN, firstNotUsedLSN LSN) ([]walSummaryFile, error) {
	if firstNotUsedLSN <= firstUsedLSN {
		return nil, errors.Errorf("empty LSN range [%s, %s)", firstUsedLSN, firstNotUsedLSN)
	}

	var kept []walSummaryFile
	for _, f := range files {
		if f.timeline != timeline {
			continue
		}
		if f.endLSN <= firstUsedLSN || f.startLSN >= firstNotUsedLSN {
			continue
		}
		kept = append(kept, f)
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].startLSN < kept[j].startLSN })

	if len(kept) == 0 {
		return nil, errors.Errorf("no WAL summaries cover [%s, %s) on timeline %d "+
			"(enable summarize_wal and retain summaries for the full range)",
			firstUsedLSN, firstNotUsedLSN, timeline)
	}
	if kept[0].startLSN > firstUsedLSN {
		return nil, errors.Errorf("WAL summary gap at start: first summary begins at %s, need %s",
			kept[0].startLSN, firstUsedLSN)
	}
	for i := 1; i < len(kept); i++ {
		if kept[i].startLSN > kept[i-1].endLSN {
			return nil, errors.Errorf("WAL summary gap between %s and %s",
				kept[i-1].endLSN, kept[i].startLSN)
		}
	}
	if kept[len(kept)-1].endLSN < firstNotUsedLSN {
		return nil, errors.Errorf("WAL summary gap at end: last summary ends at %s, need %s",
			kept[len(kept)-1].endLSN, firstNotUsedLSN)
	}
	return kept, nil
}

// parseWalSummaryFile streams a summary file and folds its entries into state.
// Mirrors src/common/blkreftable.c serialization: magic, repeated serialized
// entries (24 bytes) each followed by nchunks uint16 usage values plus per-chunk
// payloads, terminated by a zero entry and a 4-byte CRC-32C (Castagnoli) over
// everything preceding the CRC.
func parseWalSummaryFile(path string, state map[relForkKey]*roaring.Bitmap) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	hasher := crc32.New(castagnoliTable)
	r := io.TeeReader(f, hasher)

	magicBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, magicBuf); err != nil {
		return errors.Wrap(err, "reading magic")
	}
	if got := binary.LittleEndian.Uint32(magicBuf); got != blockRefTableMagic {
		return errors.Errorf("wrong magic: expected %#x, got %#x", blockRefTableMagic, got)
	}

	entryBuf := make([]byte, serializedEntrySize)
	for {
		if _, err := io.ReadFull(r, entryBuf); err != nil {
			return errors.Wrap(err, "reading entry")
		}
		if utility.AllZero(entryBuf) {
			break
		}

		spcOid := binary.LittleEndian.Uint32(entryBuf[0:4])
		dbOid := binary.LittleEndian.Uint32(entryBuf[4:8])
		relNumber := binary.LittleEndian.Uint32(entryBuf[8:12])
		forkNum := int32(binary.LittleEndian.Uint32(entryBuf[12:16]))
		limitBlock := binary.LittleEndian.Uint32(entryBuf[16:20])
		nchunks := binary.LittleEndian.Uint32(entryBuf[20:24])

		if err := parseSummaryChunks(r, nchunks, spcOid, dbOid, relNumber,
			forkNum, limitBlock, state); err != nil {
			return err
		}
	}

	// Compare accumulated CRC (over everything up to and including the zero
	// entry) with the stored 4-byte CRC. Use a non-tee read so the CRC bytes
	// themselves aren't folded into the hash.
	want := hasher.Sum32()
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, crcBuf); err != nil {
		return errors.Wrap(err, "reading CRC")
	}
	got := binary.LittleEndian.Uint32(crcBuf)
	if got != want {
		return errors.Errorf("CRC mismatch: expected %08X, got %08X", want, got)
	}
	return nil
}

func parseSummaryChunks(r io.Reader, nchunks uint32,
	spcOid, dbOid, relNumber uint32, forkNum int32, limitBlock uint32,
	state map[relForkKey]*roaring.Bitmap) error {
	key := relForkKey{
		rel: walparser.RelFileNode{
			SpcNode: walparser.Oid(spcOid),
			DBNode:  walparser.Oid(dbOid),
			RelNode: walparser.Oid(relNumber),
		},
		forkNum: forkNum,
	}
	blocks, ok := state[key]
	if !ok {
		blocks = roaring.New()
		state[key] = blocks
	}

	// Apply truncation first: drop any previously-recorded blocks >= limit.
	// limitBlock == InvalidBlockNumber means the summary has no truncation to
	// announce; do nothing in that case.
	if limitBlock != invalidBlockNumber {
		blocks.RemoveRange(uint64(limitBlock), 1<<32)
	}

	if nchunks == 0 {
		return nil
	}

	usage := make([]uint16, nchunks)
	usageBuf := make([]byte, int(nchunks)*2)
	if _, err := io.ReadFull(r, usageBuf); err != nil {
		return errors.Wrap(err, "reading chunk usage")
	}
	for i := range usage {
		usage[i] = binary.LittleEndian.Uint16(usageBuf[i*2 : (i+1)*2])
	}

	for chunkNo, used := range usage {
		if used == 0 {
			continue
		}
		if err := readChunk(r, blocks, uint32(chunkNo), used); err != nil {
			return err
		}
	}
	return nil
}

// readChunk decodes one chunk's payload, either as a 4096-entry bitmap or as
// `used` offset uint16s, and ORs the chunk's set bits into blocks.
func readChunk(r io.Reader, blocks *roaring.Bitmap, chunkNo uint32, used uint16) error {
	base := chunkNo * blocksPerChunk
	if used == maxEntriesPerChunk {
		// Bitmap representation: 4096 × uint16 = 8 KiB.
		buf := make([]byte, maxEntriesPerChunk*2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return errors.Wrap(err, "reading chunk bitmap")
		}
		for i := 0; i < maxEntriesPerChunk; i++ {
			w := binary.LittleEndian.Uint16(buf[i*2 : (i+1)*2])
			if w == 0 {
				continue
			}
			for bit := 0; bit < blocksPerEntry; bit++ {
				if w&(1<<bit) != 0 {
					blocks.Add(base + uint32(i*blocksPerEntry+bit))
				}
			}
		}
		return nil
	}
	// Array representation: `used` × uint16 offsets within chunk.
	buf := make([]byte, int(used)*2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return errors.Wrap(err, "reading chunk array")
	}
	for i := 0; i < int(used); i++ {
		off := binary.LittleEndian.Uint16(buf[i*2 : (i+1)*2])
		blocks.Add(base + uint32(off))
	}
	return nil
}

// Stringer for LSN-range debugging.
func (f walSummaryFile) String() string {
	return fmt.Sprintf("%08X%016X%016X.summary", f.timeline, uint64(f.startLSN), uint64(f.endLSN))
}
