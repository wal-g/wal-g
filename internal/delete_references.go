package internal

import (
	"bufio"
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const deletionReferenceSize = sha256.Size + 8

var ErrConflictingReferenceSize = errors.New("conflicting sizes for a shared object")

type DeletionReferences struct {
	files         []*os.File
	input, sorted *os.File
	count         int64
}

func NewDeletionReferences() (*DeletionReferences, error) {
	r := &DeletionReferences{}
	f, err := r.newFile()
	r.input = f
	return r, err
}

func (r *DeletionReferences) newFile() (*os.File, error) {
	f, err := os.CreateTemp("", "wal-g-references-*")
	if err == nil {
		r.files = append(r.files, f)
	}
	return f, err
}

func (r *DeletionReferences) Close() {
	for _, f := range r.files {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name)
	}
}

func (r *DeletionReferences) Add(key string) error { return r.AddSized(key, 0) }

func (r *DeletionReferences) AddSized(key string, size int64) error {
	if r.sorted != nil {
		return fmt.Errorf("reference set is sealed")
	}
	if size < 0 {
		return fmt.Errorf("negative reference size")
	}
	hash := sha256.Sum256([]byte(key))
	var record [deletionReferenceSize]byte
	copy(record[:], hash[:])
	binary.BigEndian.PutUint64(record[sha256.Size:], uint64(size))
	_, err := r.input.Write(record[:])
	return err
}

func (r *DeletionReferences) Seal() error {
	if r.sorted != nil {
		return nil
	}
	if _, err := r.input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	runs, err := r.sortRuns()
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		r.sorted = r.input
		return nil
	}
	for len(runs) > 1 {
		var next []*os.File
		for len(runs) > 0 {
			n := min(32, len(runs))
			file, err := r.merge(runs[:n])
			if err != nil {
				return err
			}
			next = append(next, file)
			runs = runs[n:]
		}
		runs = next
	}
	r.sorted, err = os.Open(runs[0].Name())
	if err != nil {
		return err
	}
	r.files = append(r.files, r.sorted)
	stat, err := r.sorted.Stat()
	if err != nil {
		return err
	}
	r.count = stat.Size() / deletionReferenceSize
	return nil
}

func (r *DeletionReferences) sortRuns() ([]*os.File, error) {
	var runs []*os.File
	for {
		buffer := make([]byte, 32768*deletionReferenceSize)
		n, err := io.ReadFull(r.input, buffer)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
		if n%deletionReferenceSize != 0 {
			return nil, fmt.Errorf("truncated reference inventory")
		}
		items := make([][deletionReferenceSize]byte, n/deletionReferenceSize)
		for i := range items {
			copy(items[i][:], buffer[i*deletionReferenceSize:(i+1)*deletionReferenceSize])
		}
		sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i][:], items[j][:]) < 0 })
		file, err := r.newFile()
		if err != nil {
			return nil, err
		}
		out := bufio.NewWriter(file)
		for _, h := range items {
			if _, err := out.Write(h[:]); err != nil {
				return nil, err
			}
		}
		if err := out.Flush(); err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		runs = append(runs, file)
	}
	return runs, nil
}

type referenceRun struct {
	hash   [deletionReferenceSize]byte
	reader *bufio.Reader
}
type referenceHeap []referenceRun

func (h referenceHeap) Len() int            { return len(h) }
func (h referenceHeap) Less(i, j int) bool  { return bytes.Compare(h[i].hash[:], h[j].hash[:]) < 0 }
func (h referenceHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *referenceHeap) Push(v interface{}) { *h = append(*h, v.(referenceRun)) }
func (h *referenceHeap) Pop() interface{} {
	last := len(*h) - 1
	v := (*h)[last]
	*h = (*h)[:last]
	return v
}

func (r *DeletionReferences) merge(runs []*os.File) (*os.File, error) {
	out, err := r.newFile()
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(out)
	h := &referenceHeap{}
	for _, runFile := range runs {
		f, err := os.Open(runFile.Name())
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		run := referenceRun{reader: bufio.NewReader(f)}
		if _, err := io.ReadFull(run.reader, run.hash[:]); err != nil {
			return nil, err
		}
		heap.Push(h, run)
	}
	for h.Len() > 0 {
		run := heap.Pop(h).(referenceRun)
		if _, err := w.Write(run.hash[:]); err != nil {
			return nil, err
		}
		_, err := io.ReadFull(run.reader, run.hash[:])
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			return nil, err
		}
		heap.Push(h, run)
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := out.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *DeletionReferences) Contains(key string) (bool, error) {
	hash := sha256.Sum256([]byte(key))
	return r.containsHash(hash[:])
}
func (r *DeletionReferences) containsHash(wanted []byte) (bool, error) {
	if r.sorted == nil {
		return false, fmt.Errorf("reference set is not sealed")
	}
	low, high := int64(0), r.count
	for low < high {
		mid := low + (high-low)/2
		var current [sha256.Size]byte
		if _, err := r.sorted.ReadAt(current[:], mid*deletionReferenceSize); err != nil {
			return false, err
		}
		cmp := bytes.Compare(current[:], wanted)
		if cmp == 0 {
			return true, nil
		}
		if cmp < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return false, nil
}

func (r *DeletionReferences) SumNotIn(previous *DeletionReferences) (int64, error) {
	if r.sorted == nil {
		return 0, fmt.Errorf("reference set is not sealed")
	}
	if _, err := r.sorted.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(r.sorted)
	var last, current [deletionReferenceSize]byte
	var sum int64
	hasLast := false
	for {
		_, err := io.ReadFull(reader, current[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if hasLast && bytes.Equal(last[:sha256.Size], current[:sha256.Size]) {
			if !bytes.Equal(last[:], current[:]) {
				return 0, ErrConflictingReferenceSize
			}
			continue
		}
		last = current
		hasLast = true
		seen, err := previous.containsHash(current[:sha256.Size])
		if err != nil {
			return 0, err
		}
		if seen {
			continue
		}
		size := int64(binary.BigEndian.Uint64(current[sha256.Size:]))
		if size < 0 || sum > int64(^uint64(0)>>1)-size {
			return 0, fmt.Errorf("shared size overflow")
		}
		sum += size
	}
	return sum, nil
}

func WalkDeletionManifest(reader io.Reader, visit func(string, int64) error, sizeField string) (problem, operational error) {
	tracked := &deletionTrackingReader{Reader: reader}
	var visitorErr error
	problem = decodeDeletionManifest(json.NewDecoder(tracked), func(key string, size int64) error {
		visitorErr = visit(key, size)
		return visitorErr
	}, sizeField)
	if visitorErr != nil {
		return nil, visitorErr
	}
	if tracked.err != nil {
		return nil, tracked.err
	}
	return problem, nil
}

func decodeDeletionManifest(d *json.Decoder, visit func(string, int64) error, sizeField string) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("manifest is not an object")
	}
	found := false
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return err
		}
		if token != "Files" {
			var discard json.RawMessage
			if err = d.Decode(&discard); err != nil {
				return err
			}
			continue
		}
		found = true
		if err := decodeDeletionManifestFiles(d, visit, sizeField); err != nil {
			return err
		}
	}
	if _, err = d.Token(); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("missing Files in manifest")
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("trailing manifest data")
		}
		return err
	}
	return nil
}

func decodeDeletionManifestFiles(d *json.Decoder, visit func(string, int64) error, sizeField string) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("manifest Files is not an object")
	}
	for d.More() {
		if _, err := d.Token(); err != nil {
			return err
		}
		key, size, err := decodeDeletionFile(d, sizeField)
		if err != nil {
			return err
		}
		if key != "" {
			if err := visit(key, size); err != nil {
				return err
			}
		}
	}
	_, err = d.Token()
	return err
}

func decodeDeletionFile(d *json.Decoder, sizeField string) (string, int64, error) {
	var raw json.RawMessage
	if err := d.Decode(&raw); err != nil {
		return "", 0, err
	}
	var desc map[string]json.RawMessage
	if err := jsonv2.Unmarshal(raw, &desc); err != nil {
		return "", 0, err
	}
	if desc == nil {
		return "", 0, fmt.Errorf("null manifest descriptor")
	}
	if string(desc["StoragePath"]) == "null" || (sizeField != "" && string(desc[sizeField]) == "null") {
		return "", 0, fmt.Errorf("null shared file reference or size")
	}
	var key string
	if err := json.Unmarshal(desc["StoragePath"], &key); err != nil {
		return "", 0, err
	}
	var size int64
	if sizeField != "" {
		if err := json.Unmarshal(desc[sizeField], &size); err != nil {
			return "", 0, err
		}
		if size < 0 {
			return "", 0, fmt.Errorf("negative shared file size")
		}
	}
	if key != "" {
		if err := ValidateRecoveryName(key); err != nil {
			return "", 0, err
		}
	}
	return key, size, nil
}

type deletionTrackingReader struct {
	io.Reader
	err error
}

func (r *deletionTrackingReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}
	return n, err
}
