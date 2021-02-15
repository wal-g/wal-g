package blob

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const ProxyStartTimeout = 10 * time.Second

type Server struct {
	sync.Mutex
	folder   storage.Folder
	certFile string
	keyFile  string
	endpoint string
	server   http.Server
	indexes  map[string]*Index
	leases   map[string]*Lease
}

func NewServer(folder storage.Folder) (*Server, error) {
	var err error
	bs := new(Server)
	bs.folder = folder
	bs.certFile, err = internal.GetRequiredSetting(internal.SQLServerBlobCertFile)
	if err != nil {
		return nil, err
	}
	bs.keyFile, err = internal.GetRequiredSetting(internal.SQLServerBlobKeyFile)
	if err != nil {
		return nil, err
	}
	hostname, err := internal.GetRequiredSetting(internal.SQLServerBlobHostname)
	if err != nil {
		return nil, err
	}
	bs.endpoint = fmt.Sprintf("%s:%d", hostname, 443)
	bs.server = http.Server{Addr: bs.endpoint, Handler: bs}
	bs.indexes = make(map[string]*Index)
	bs.leases = make(map[string]*Lease)
	return bs, nil
}

func (bs *Server) Run(ctx context.Context) error {
	errs := make(chan error)
	go func() {
		tracelog.InfoLogger.Printf("running proxy at %s", bs.endpoint)
		errs <- bs.server.ListenAndServeTLS(bs.certFile, bs.keyFile)
	}()
	select {
	case <-ctx.Done():
		return bs.Shutdown()
	case err := <-errs:
		return err
	}
}

func (bs *Server) RunBackground(ctx context.Context, cancel context.CancelFunc) error {
	go func() {
		err := bs.Run(ctx)
		if err != nil {
			tracelog.ErrorLogger.Printf("proxy error: %v", err)
			if cancel != nil {
				cancel()
			}
		}
	}()
	return bs.WaitReady(ctx, ProxyStartTimeout)
}

func (bs *Server) WaitReady(ctx context.Context, timeout time.Duration) error {
	sctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := fmt.Sprintf("https://%s/", bs.endpoint)
	c := http.Client{Timeout: 100 * time.Millisecond}
	t := time.NewTicker(200 * time.Millisecond)
	for {
		select {
		case <-t.C:
			resp, _ := c.Head(url)
			if resp != nil {
				return resp.Body.Close()
			}
		case <-sctx.Done():
			return fmt.Errorf("proxy not ready in %s", timeout)
		}
	}
}

func (bs *Server) Shutdown() error {
	tracelog.InfoLogger.Printf("stopping proxy")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := bs.server.Shutdown(sctx)
	if err != nil {
		tracelog.ErrorLogger.Printf("proxy shutdown error: %v", err)
	}
	return err
}

func (bs *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if _, ok := internal.GetSetting(internal.SQLServerBlobKeyFile); ok {
		b, _ := httputil.DumpRequest(req, false)
		tracelog.DebugLogger.Println(string(b))
		bs.ServeHTTP2(&DebugResponseWriter{w}, req)
	} else {
		bs.ServeHTTP2(w, req)
	}
}

func (bs *Server) ServeHTTP2(w http.ResponseWriter, req *http.Request) {
	// default headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", "0")
	w.Header().Set("X-Ms-Version", "2014-02-14")
	w.Header().Set("X-Ms-Blob-Type", "BlockBlob")
	w.Header().Set("X-Ms-Request-Id", uuid.New().String())
	w.Header().Set("Accept-Ranges", "bytes")
	if err := req.ParseForm(); err != nil {
		tracelog.WarningLogger.Printf("blob proxy: failed to parse form: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch req.Form.Get("comp") {
	case "lease":
		bs.HandleLease(w, req)
	case "block":
		bs.HandleBlock(w, req)
	case "blocklist":
		bs.HandleBlockList(w, req)
	case "":
		bs.HandleBlob(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Lease operations
func (bs *Server) HandleLease(w http.ResponseWriter, req *http.Request) {
	// fake lease for now
	leaseAction := req.Header.Get("X-Ms-Lease-Action")

	switch leaseAction {
	case "Acquire":
		bs.HandleAcquireLease(w, req)
	case "Renew":
		bs.HandleRenewLease(w, req)
	case "Change":
		bs.HandleChangeLease(w, req)
	case "Release":
		bs.HandleReleaseLease(w, req)
	case "Break":
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (bs *Server) HandleAcquireLease(w http.ResponseWriter, req *http.Request) {
	leaseId := req.Header.Get("X-Ms-Proposed-Lease-Id")
	if leaseId == "" {
		leaseId = uuid.New().String()
	}
	leaseDurationStr := req.Header.Get("X-Ms-Lease-Duration")
	if leaseDurationStr == "" {
		leaseDurationStr = "31536000"
	}
	leaseDuration, err := strconv.Atoi(leaseDurationStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	folder := bs.getBlobFolder(req.URL.Path)
	lease, ok := bs.leases[folder.GetPath()]
	if !ok || lease.End.Before(time.Now()) {
		lease = &Lease{
			ID:  leaseId,
			End: time.Now().Add(time.Duration(leaseDuration * int(time.Second))),
		}
		bs.leases[folder.GetPath()] = lease
	}
	if lease.ID == leaseId {
		w.Header().Set("X-Ms-Lease-Id", leaseId)
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusPreconditionFailed)
	}
}

func (bs *Server) HandleRenewLease(w http.ResponseWriter, req *http.Request) {
	leaseId := req.Header.Get("X-Ms-Lease-Id")
	if leaseId == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	leaseDurationStr := req.Header.Get("X-Ms-Lease-Duration")
	if leaseDurationStr == "" {
		leaseDurationStr = "31536000"
	}
	leaseDuration, err := strconv.Atoi(leaseDurationStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	folder := bs.getBlobFolder(req.URL.Path)
	lease, ok := bs.leases[folder.GetPath()]
	if !ok || lease.ID != leaseId {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	lease.End = time.Now().Add(time.Duration(leaseDuration * int(time.Second)))
	w.WriteHeader(http.StatusOK)
}

func (bs *Server) HandleChangeLease(w http.ResponseWriter, req *http.Request) {
	leaseId := req.Header.Get("X-Ms-Lease-Id")
	if leaseId == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	newLeaseId := req.Header.Get("X-Ms-Proposed-Lease-Id")
	if newLeaseId == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	folder := bs.getBlobFolder(req.URL.Path)
	lease, ok := bs.leases[folder.GetPath()]
	if !ok || lease.ID != leaseId {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	lease.ID = newLeaseId
	w.WriteHeader(http.StatusOK)
}

func (bs *Server) HandleReleaseLease(w http.ResponseWriter, req *http.Request) {
	leaseId := req.Header.Get("X-Ms-Lease-Id")
	if leaseId == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	folder := bs.getBlobFolder(req.URL.Path)
	lease, ok := bs.leases[folder.GetPath()]
	if !ok || lease.ID != leaseId {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	delete(bs.leases, folder.GetPath())
	w.WriteHeader(http.StatusOK)
}

func (bs *Server) checkLease(req *http.Request, folder storage.Folder) error {
	lease, ok := bs.leases[folder.GetPath()]
	if ok && lease.End.After(time.Now()) {
		if lease.ID != req.Header.Get("X-Ms-Lease-Id") {
			return ErrNoLease
		}
	}
	return nil
}

func (bs *Server) setLeaseHeaders(w http.ResponseWriter, req *http.Request, folder storage.Folder) {
	lease, ok := bs.leases[folder.GetPath()]
	if !ok {
		w.Header().Set("X-Ms-Lease-State", "Available")
		return
	}
	if lease.End.After(time.Now()) {
		w.Header().Set("X-Ms-Lease-State", "Expired")
	} else {
		w.Header().Set("X-Ms-Lease-State", "Leased")
		w.Header().Add("X-Ms-Lease-Duration", "fixed")
		if req.Header.Get("X-Ms-Lease-Id") == lease.ID {
			w.Header().Add("X-Ms-Lease-Status", "locked")
		}
	}
}

// Block operations
func (bs *Server) HandleBlock(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodPut:
		bs.HandleBlockPut(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (bs *Server) HandleBlockPut(w http.ResponseWriter, req *http.Request) {
	if req.Form.Get("x-ms-copy-source:name") != "" {
		tracelog.ErrorLogger.Printf("proxy: put block from url is not supported")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	if err := bs.checkLease(req, folder); err != nil {
		bs.returnError(w, req, err)
		return
	}
	blockId := strings.TrimSpace(req.Form.Get("blockid"))
	blockSizeStr := req.Header.Get("Content-Length")
	blockSize, err := strconv.ParseUint(blockSizeStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	filename := idx.PutBlock(blockId, blockSize)
	err = folder.PutObject(filename, req.Body)
	req.Body.Close()
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	// TODO: delayed write ?
	err = idx.Save()
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// BlockList operations
func (bs *Server) HandleBlockList(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodPut:
		bs.HandleBlockListPut(w, req)
	case http.MethodGet:
		bs.HandleBlockListGet(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (bs *Server) HandleBlockListPut(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	if err := bs.checkLease(req, folder); err != nil {
		bs.returnError(w, req, err)
		return
	}
	data, err := ioutil.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	xblocklist, err := ParseBlocklistXML(data)
	if err != nil {
		tracelog.ErrorLogger.Printf("proxy: failed to read blocklist xml: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	garbage, err := idx.PutBlockList(xblocklist)
	if err != nil {
		tracelog.ErrorLogger.Print(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = idx.Save()
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	bs.deleteGarbage(folder, garbage)
	w.WriteHeader(http.StatusCreated)
}

func (bs *Server) HandleBlockListGet(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	blocklisttype := strings.Title(strings.ToLower(req.Form.Get("blocklisttype")))
	xblocklist := idx.GetBlockList(blocklisttype)
	data, err := SerializeBlocklistXML(xblocklist)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	tracelog.ErrorLogger.PrintOnError(err)
}

// Index operations
func (bs *Server) HandleBlob(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodHead:
		bs.HandleBlobHead(w, req)
	case http.MethodGet:
		bs.HandleBlobGet(w, req)
	case http.MethodPut:
		bs.HandleBlobPut(w, req)
	case http.MethodDelete:
		bs.HandleBlobDelete(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (bs *Server) HandleBlobHead(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatUint(idx.Size, 10))
	bs.setLeaseHeaders(w, req, folder)
	w.WriteHeader(http.StatusOK)
}

func (bs *Server) HandleBlobGet(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	if err := bs.checkLease(req, folder); err != nil {
		bs.returnError(w, req, err)
		return
	}

	rangeMin := uint64(0)
	rangeMax := idx.Size - 1
	rangeHeader := req.Header.Get("X-Ms-Range")
	if rangeHeader == "" {
		rangeHeader = req.Header.Get("Range")
	}
	if rangeHeader != "" {
		var err error
		rangeMin, rangeMax, err = bs.parseBytesRange(req)
		if err != nil {
			bs.returnError(w, req, err)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatUint(rangeMax-rangeMin+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeMin, rangeMax, idx.Size))
		bs.setLeaseHeaders(w, req, folder)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatUint(idx.Size, 10))
		bs.setLeaseHeaders(w, req, folder)
		w.WriteHeader(http.StatusOK)
	}

	sections := idx.GetSections(rangeMin, rangeMax)
	for _, s := range sections {
		r, err := folder.ReadObject(s.Path)
		if err != nil {
			tracelog.ErrorLogger.Printf("proxy: failed to read object from storage: %v", err)
			break
		}
		r2 := io.LimitReader(NewSkipReader(r, s.Offset), int64(s.Limit))
		_, err = io.Copy(w, r2)
		r.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("proxy: failed to copy data from storage: %v", err)
			break
		}
	}
}

func (bs *Server) HandleBlobPut(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	idx, err := bs.loadBlobIndex(folder)
	if err == ErrNotFound {
		idx = NewIndex(folder)
	} else if err != nil {
		bs.returnError(w, req, err)
		return
	}
	if err := bs.checkLease(req, folder); err != nil {
		bs.returnError(w, req, err)
		return
	}
	blobSizeStr := req.Header.Get("Content-Length")
	blobSize, err := strconv.ParseUint(blobSizeStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	garbage := idx.Clear()
	if blobSize > 0 {
		name := idx.PutBlock("data", blobSize)
		err := folder.PutObject(name, req.Body)
		req.Body.Close()
		if err != nil {
			bs.returnError(w, req, err)
			return
		}
		_, err = idx.PutBlockList(&XBlockListIn{Blocks: []XBlockIn{{ID: "data", Mode: BlockLatest}}})
		if err != nil {
			bs.returnError(w, req, err)
			return
		}
	}
	err = idx.Save()
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	bs.deleteGarbage(folder, garbage)
	w.WriteHeader(http.StatusCreated)
}

func (bs *Server) HandleBlobDelete(w http.ResponseWriter, req *http.Request) {
	folder := bs.getBlobFolder(req.URL.Path)
	if err := bs.checkLease(req, folder); err != nil {
		bs.returnError(w, req, err)
		return
	}
	parts := strings.Split(req.URL.Path, "/")
	blob := parts[len(parts)-1]
	upperFolder := bs.folder
	for _, p := range parts[:len(parts)-1] {
		upperFolder = upperFolder.GetSubFolder(p)
	}
	bs.Lock()
	defer bs.Unlock()
	err := upperFolder.DeleteObjects([]string{blob})
	if err != nil {
		bs.returnError(w, req, err)
		return
	}
	delete(bs.indexes, folder.GetPath())
	w.WriteHeader(http.StatusCreated)
}

// utils
func (bs *Server) returnError(w http.ResponseWriter, req *http.Request, err error) {
	switch {
	case err == ErrNoLease:
		w.WriteHeader(http.StatusPreconditionFailed)
	case err == ErrNotFound:
		w.WriteHeader(http.StatusNotFound)
	case err == ErrBadRequest:
		w.WriteHeader(http.StatusBadRequest)
	default:
		tracelog.ErrorLogger.Printf("proxy: failed to load blob index: %s %v", req.URL.Path, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (bs *Server) getBlobFolder(path string) storage.Folder {
	f := bs.folder
	for _, p := range strings.Split(path, "/") {
		f = f.GetSubFolder(p)
	}
	return f
}

func (bs *Server) loadBlobIndex(folder storage.Folder) (*Index, error) {
	bs.Lock()
	defer bs.Unlock()
	path := folder.GetPath()
	if idx, ok := bs.indexes[path]; ok {
		return idx, nil
	}
	idx := NewIndex(folder)
	err := idx.Load()
	if err != nil {
		return nil, err
	}
	bs.indexes[path] = idx
	return idx, nil
}

func (bs *Server) deleteGarbage(folder storage.Folder, garbage []string) {
	if len(garbage) == 0 {
		return
	}
	err := folder.DeleteObjects(garbage)
	if err != nil {
		tracelog.WarningLogger.Printf("proxy: failed to delete garbage objects: %v", err)
	}
}

func (bs *Server) parseBytesRange(req *http.Request) (uint64, uint64, error) {
	rangeStr := req.Header.Get("X-Ms-Range")
	if rangeStr == "" {
		rangeStr = req.Header.Get("Range")
	}
	if rangeStr[:6] != "bytes=" {
		return 0, 0, ErrBadRequest
	}
	rangeStr = rangeStr[6:]
	rangeSlice := strings.Split(rangeStr, "-")
	if len(rangeSlice) != 2 {
		return 0, 0, ErrBadRequest
	}
	rangeMin, err := strconv.ParseUint(rangeSlice[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	rangeMax, err := strconv.ParseUint(rangeSlice[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return rangeMin, rangeMax, nil
}
