package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/webserver"
	"github.com/wal-g/wal-g/utility"
)

const (
	DefaultStatsPrefix          = "/stats"
	DefaultOplogPushStatsPrefix = DefaultStatsPrefix + "/oplog_push"
)

// OplogArchivedStatsReporter defines oplog archiving upload statistics fetching interface
type OplogArchivedStatsReporter interface {
	Report() OplogArchivedStatsReport
}

// OplogUploadStatsUpdater defines oplog archiving upload statistics update interface
type OplogUploadStatsUpdater interface {
	Update(batchDocs, batchBytes int, lastArchivedTS models.Timestamp)
}

// OplogArchivedStatsReport defines oplog archiving upload statistics report
type OplogArchivedStatsReport struct {
	LastTS models.Timestamp `json:"last_ts"`
	Docs   uint64           `json:"docs"`
	Bytes  uint64           `json:"bytes"`
}

// OplogUploadStats implements OplogUploadStats -Reporter and -OplogPushUpdater
type OplogUploadStats struct {
	sync.Mutex
	rep OplogArchivedStatsReport
}

// NewOplogUploadStats builds OplogUploadStats
func NewOplogUploadStats(LastUploadedTS models.Timestamp) *OplogUploadStats {
	return &OplogUploadStats{rep: OplogArchivedStatsReport{LastTS: LastUploadedTS}}
}

// Update ...
func (r *OplogUploadStats) Update(batchDocs, batchBytes int, lastArchivedTS models.Timestamp) {
	r.Lock()
	defer r.Unlock()
	r.rep.LastTS = lastArchivedTS
	r.rep.Docs += uint64(batchDocs)
	r.rep.Bytes += uint64(batchBytes)
}

// Report ...
func (r *OplogUploadStats) Report() OplogArchivedStatsReport {
	return r.rep
}

// RefreshWithInterval renews OplogPushUpdater with given time interval
func RefreshWithInterval(ctx context.Context, interval time.Duration, stats OplogPushUpdater, logger logFunc) {
	archiveTimer := time.NewTimer(interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-archiveTimer.C:
		}
		utility.ResetTimer(archiveTimer, interval)
		if err := stats.Update(); err != nil {
			logger("Failed to update stats with error: %+v", err)
		}
	}
}

// OplogPushUpdater defines oplog-push procedure statistics update interface
type OplogPushUpdater interface {
	Update() error
}

// OplogPushReport defines oplog-push statistics report
type OplogPushReport struct {
	Archived OplogArchivedStatsReport `json:"archived"`
	Mongo    struct {
		LastKnownMajTS models.Timestamp `json:"last_known_maj_ts"`
	} `json:"mongo"`
}

// OplogPushStats implements OplogPushUpdater
type OplogPushStats struct {
	ctx      context.Context
	uploader OplogArchivedStatsReporter
	mc       client.MongoDriver
	sync.Mutex
	rep OplogPushReport
}

type logFunc func(format string, args ...interface{})

type OplogPushStatsOption func(*OplogPushStats)

// EnableLogReport runs logging stats procedure in new goroutine
func EnableLogReport(logInterval time.Duration, logger logFunc) OplogPushStatsOption {
	return func(st *OplogPushStats) {
		go st.RunLogging(logInterval, logger)
	}
}

// EnableHTTPHandler registers stats handler at given web server
func EnableHTTPHandler(httpPattern string, srv webserver.WebServer) OplogPushStatsOption {
	return func(st *OplogPushStats) {
		srv.HandleFunc(httpPattern, st.ServeHTTP)
	}
}

// NewOplogPushStats builds OplogPushStats
func NewOplogPushStats(ctx context.Context, opRep OplogArchivedStatsReporter, mc client.MongoDriver, opts ...OplogPushStatsOption) *OplogPushStats {
	st := &OplogPushStats{
		ctx:      ctx,
		uploader: opRep,
		mc:       mc,
	}
	for _, optFunc := range opts {
		optFunc(st)
	}

	return st
}

// ServeHTTP implements stats http-handler
func (st *OplogPushStats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st.Lock()
	data, err := json.Marshal(st.rep)
	st.Unlock()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(data); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// RunLogging executes logFunc every logInterval with current stats
func (st *OplogPushStats) RunLogging(logInterval time.Duration, logger logFunc) {
	archiveTimer := time.NewTimer(logInterval)
	for {
		select {
		case <-st.ctx.Done():
			return
		case <-archiveTimer.C:
		}
		utility.ResetTimer(archiveTimer, logInterval)
		st.Lock()
		logger("OplogPushStatus: docs %d, bytes %d, lag %d seconds",
			st.rep.Archived.Docs,
			st.rep.Archived.Bytes,
			st.rep.Mongo.LastKnownMajTS.TS-st.rep.Archived.LastTS.TS)
		st.Unlock()
	}
}

// Update initiates stats update from underlying reports
func (st *OplogPushStats) Update() error {
	im, err := st.mc.IsMaster(st.ctx)
	if err != nil {
		return fmt.Errorf("can not update oplog push stats: %w", err)
	}
	uploader := st.uploader.Report()

	st.Lock()
	defer st.Unlock()

	st.rep.Archived.LastTS = uploader.LastTS
	st.rep.Archived.Docs = uploader.Docs
	st.rep.Archived.Bytes = uploader.Bytes
	st.rep.Mongo.LastKnownMajTS = im.LastWrite.MajorityOpTime.TS
	return nil
}
