package progress

import "time"

// Snapshot is a point-in-time view, shared by terminal and file reporting.
// TransferredBytes is cumulative across the command (including retries).
// DownloadedBytes and TotalBytes describe only the current backup's estimate.
type Snapshot struct {
	Version             int        `json:"version"`
	RunID               string     `json:"run_id"`
	PID                 int        `json:"pid"`
	Operation           string     `json:"operation"`
	Status              string     `json:"status"`
	Stage               string     `json:"stage"`
	StartedAt           time.Time  `json:"started_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	FinishedAt          *time.Time `json:"finished_at"`
	LastTransferAt      *time.Time `json:"last_transfer_at"`
	ElapsedSeconds      float64    `json:"elapsed_seconds"`
	StageElapsedSeconds float64    `json:"stage_elapsed_seconds"`
	IdleSeconds         float64    `json:"idle_seconds"`
	TransferredBytes    int64      `json:"transferred_bytes"`
	BytesPerSecond      float64    `json:"bytes_per_second"`
	DownloadedBytes     *int64     `json:"downloaded_bytes"`
	TotalBytes          *int64     `json:"total_bytes"`
	ETASeconds          *int64     `json:"eta_seconds"`
}

// snapshot samples counters under Tracker.mu, without filesystem I/O.
func (p *Tracker) snapshot(now time.Time) Snapshot {
	current := p.bytes.Load()
	var rate float64
	if seconds := now.Sub(p.sampled).Seconds(); seconds > 0 {
		rate = float64(current-p.lastBytes) / seconds
	}
	if current != p.lastBytes {
		p.lastProgress = now
	}
	p.sampled, p.lastBytes = now, current
	s := Snapshot{
		Version: 1, Operation: p.label, Status: "running", StartedAt: p.started.UTC(), UpdatedAt: now.UTC(),
		ElapsedSeconds: now.Sub(p.started).Seconds(), StageElapsedSeconds: now.Sub(p.stageStarted).Seconds(),
		IdleSeconds: now.Sub(p.lastProgress).Seconds(), TransferredBytes: current, BytesPerSecond: rate,
	}
	if stage := p.stage.Load(); stage != nil {
		s.Stage = *stage
	}
	if current > 0 {
		last := p.lastProgress.UTC()
		s.LastTransferAt = &last
	}
	if d := p.estimateDownload(now, current, now.Sub(p.lastProgress) >= idleThreshold); d != nil {
		s.DownloadedBytes, s.TotalBytes, s.ETASeconds = &d.transferred, &d.total, d.etaSeconds
	}
	return s
}
