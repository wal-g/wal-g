package blob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/wal-g/storages/storage"
	"io/ioutil"
	"sort"
	"sync"
)

const IndexFileName = "__blob_index.json"

type Index struct {
	sync.Mutex
	folder    storage.Folder
	Size      uint64            `json:"size"`
	Blocks    []*Block          `json:"blocks"`
	icache    map[string]*Block // cache by id's
	ocache    []*Block          // cache by offset, ordered, only committed
	srequests chan chan error
}

type Block struct {
	ID            string `json:"id"`
	Offset        uint64 `json:"of"`
	UploadedSize  uint64 `json:"us"`
	UploadedRev   uint   `json:"ur"`
	CommittedSize uint64 `json:"cs"`
	CommittedRev  uint   `json:"cr"`
}

type Section struct {
	Path   string
	Offset uint64
	Limit  uint64
}

func NewIndex(f storage.Folder) *Index {
	idx := &Index{
		folder:    f,
		icache:    make(map[string]*Block),
		srequests: make(chan chan error, 100),
	}
	go idx.saver()
	return idx
}

func (idx *Index) Load() error {
	reader, err := idx.folder.ReadObject(IndexFileName)
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			return ErrNotFound
		}
		return err
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	idx.Lock()
	defer idx.Unlock()
	err = json.Unmarshal(data, idx)
	if err != nil {
		return err
	}
	idx.buildCache()
	return nil
}

func (idx *Index) buildCache() {
	idx.ocache = make([]*Block, 0, len(idx.Blocks))
	idx.icache = make(map[string]*Block, len(idx.Blocks))
	for _, b := range idx.Blocks {
		idx.icache[b.ID] = b
		if b.CommittedRev > 0 {
			idx.ocache = append(idx.ocache, b)
		}
	}
	sort.Slice(idx.ocache, func(i, j int) bool {
		return idx.ocache[i].Offset < idx.ocache[j].Offset
	})
}

func (idx *Index) saver() {
	for {
		requests := make([]chan error, 0, 10)
		// get next save request
		req := <-idx.srequests
		requests = append(requests, req)
		// and other pending
		select {
		case req := <-idx.srequests:
			requests = append(requests, req)
		default:
			break
		}
		idx.Lock()
		data, err := json.Marshal(idx)
		idx.Unlock()
		if err == nil {
			err = idx.folder.PutObject(IndexFileName, bytes.NewBuffer(data))
		}
		for _, req := range requests {
			req <- err
		}
	}
}

func (idx *Index) Save() error {
	req := make(chan error, 1)
	idx.srequests <- req
	return <-req
}

func (idx *Index) PutBlock(id string, size uint64) string {
	idx.Lock()
	defer idx.Unlock()
	block, ok := idx.icache[id]
	if !ok {
		block = &Block{ID: id, UploadedRev: 1}
		idx.Blocks = append(idx.Blocks, block)
		idx.icache[id] = block
	} else {
		block.UploadedRev = block.CommittedRev + 1
	}
	block.UploadedSize = size
	return fmt.Sprintf("%s.%d", block.ID, block.UploadedRev)
}

func (idx *Index) PutBlockList(xblocklist *XBlockListIn) ([]string, error) {
	idx.Lock()
	defer idx.Unlock()

	oldBlocks := idx.Blocks
	newBlocks := make([]*Block, 0, len(xblocklist.Blocks))
	garbage := make([]string, 0, len(xblocklist.Blocks))
	size := uint64(0)

	for _, xb := range xblocklist.Blocks {
		block, ok := idx.icache[xb.ID]
		if !ok {
			return nil, fmt.Errorf("proxy: blocklist operation contains not-uploaded block ID: %s", xb.ID)
		}
		block.Offset = size
		newBlocks = append(newBlocks, block)
		switch xb.Mode {
		case BlockCommitted:
			if block.CommittedRev == 0 {
				return nil, fmt.Errorf("proxy: blocklist operation block %s is not committed", xb.ID)
			}
			if block.UploadedRev != 0 {
				block.UploadedRev = 0
				block.UploadedSize = 0
				garbage = append(garbage, fmt.Sprintf("%s.%d", xb.ID, block.UploadedRev))
			}
		case BlockUncommitted:
			if block.UploadedRev == 0 {
				return nil, fmt.Errorf("proxy: blocklist operation block %s is not committed", xb.ID)
			}
			if block.CommittedRev != 0 {
				garbage = append(garbage, fmt.Sprintf("%s.%d", xb.ID, block.CommittedRev))
			}
			block.CommittedRev = block.UploadedRev
			block.CommittedSize = block.UploadedSize
			block.UploadedRev = 0
			block.UploadedSize = 0
		case BlockLatest:
			if block.UploadedRev != 0 {
				if block.CommittedRev != 0 {
					garbage = append(garbage, fmt.Sprintf("%s.%d", xb.ID, block.CommittedRev))
				}
				block.CommittedRev = block.UploadedRev
				block.CommittedSize = block.UploadedSize
				block.UploadedRev = 0
				block.UploadedSize = 0
			}
		default:
			panic(fmt.Sprintf("unexpected block mode: %s", xb.Mode))
		}
		size += block.CommittedSize
	}

	idx.Size = size
	idx.Blocks = newBlocks
	idx.buildCache()

	for _, block := range oldBlocks {
		if _, ok := idx.icache[block.ID]; !ok {
			if block.UploadedRev != 0 {
				garbage = append(garbage, fmt.Sprintf("%s.%d", block.ID, block.UploadedRev))
			}
			if block.CommittedRev != 0 {
				garbage = append(garbage, fmt.Sprintf("%s.%d", block.ID, block.CommittedRev))
			}
		}
	}
	return garbage, nil
}

func (idx *Index) GetBlockList(ltype string) *XBlockListOut {
	idx.Lock()
	defer idx.Unlock()
	var bl XBlockListOut
	if ltype == BlockCommitted || ltype == BlockAll {
		for _, b := range idx.ocache {
			bl.CommittedBlocks.Blocks = append(bl.CommittedBlocks.Blocks, XBlockOut{
				Name: b.ID,
				Size: b.CommittedSize,
			})
		}
	}
	if ltype == BlockUncommitted || ltype == BlockAll {
		for _, b := range idx.Blocks {
			if b.UploadedRev > 0 {
				bl.UncommittedBlocks.Blocks = append(bl.UncommittedBlocks.Blocks, XBlockOut{
					Name: b.ID,
					Size: b.UploadedSize,
				})
			}
		}
		sort.Slice(bl.UncommittedBlocks.Blocks, func(i, j int) bool {
			return bl.UncommittedBlocks.Blocks[i].Name < bl.UncommittedBlocks.Blocks[i].Name
		})
	}
	return &bl
}

func (idx *Index) Clear() []string {
	idx.Lock()
	defer idx.Unlock()

	var garbage []string
	for _, block := range idx.Blocks {
		if block.UploadedRev != 0 {
			garbage = append(garbage, fmt.Sprintf("%s.%d", block.ID, block.UploadedRev))
		}
		if block.CommittedRev != 0 {
			garbage = append(garbage, fmt.Sprintf("%s.%d", block.ID, block.CommittedRev))
		}
	}
	idx.Blocks = []*Block{}
	idx.buildCache()
	return garbage
}

func (idx *Index) GetSections(rangeMin, rangeMax uint64) []Section {
	idx.Lock()
	defer idx.Unlock()
	var sections []Section
	// binary search section start
	l := 0
	r := len(idx.ocache) - 1
	for r > l {
		m := (r + l) / 2
		block := idx.ocache[m]
		if (block.Offset + block.CommittedSize - 1) < rangeMin {
			l = m + 1
		} else if block.Offset > rangeMin {
			r = m - 1
		} else {
			l = m
			break
		}
	}
	// scan next blocks
	for i := l; i < len(idx.ocache); i++ {
		block := idx.ocache[i]
		if block.Offset > rangeMax {
			break
		}
		offset := uint64(0)
		if rangeMin > block.Offset {
			offset = rangeMin - block.Offset
		}
		limit := block.CommittedSize
		if rangeMax < (block.Offset + block.CommittedSize - 1) {
			limit = rangeMax - block.Offset + 1
		}
		sections = append(sections, Section{
			Path:   fmt.Sprintf("%s.%d", block.ID, block.CommittedRev),
			Offset: offset,
			Limit:  limit,
		})
	}
	return sections
}
