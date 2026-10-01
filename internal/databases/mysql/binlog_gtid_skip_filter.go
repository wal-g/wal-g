package mysql

import (
	"os"
	"path"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
)

// gtidSkipFilter wraps a binlogHandler and drops binlog files whose entire
// content is already covered by checkpoint -- needed after a failover,
// when binlog file numbers no longer line up across servers and can't be
// used to tell what's already applied.
//
// Deciding whether a file is fully covered needs the *next* file's
// starting GTID set (its own PREVIOUS_GTIDS event), so the filter holds
// one file back (pending) until the next one arrives.
type gtidSkipFilter struct {
	inner      binlogHandler
	checkpoint gomysql.GTIDSet // nil disables filtering: everything is forwarded
	flavor     string

	getPreviousGTIDs func(filename, flavor string) (gomysql.GTIDSet, error)

	pending  string
	skipping bool
}

func newGTIDSkipFilter(inner binlogHandler, checkpoint gomysql.GTIDSet, flavor string) *gtidSkipFilter {
	return &gtidSkipFilter{
		inner:            inner,
		checkpoint:       checkpoint,
		flavor:           flavor,
		getPreviousGTIDs: GetBinlogPreviousGTIDs,
		skipping:         checkpoint != nil,
	}
}

// parseGTIDChecked parses gtidStr as a GTID set, trying MariaDB then MySQL
// format, returning the set and which flavor matched. Returns (nil, "")
// if gtidStr is empty or matches neither -- no checkpoint recorded.
func parseGTIDChecked(gtidStr string) (gomysql.GTIDSet, string) {
	if gtidStr == "" {
		return nil, ""
	}
	if set, err := gomysql.ParseGTIDSet(gomysql.MariaDBFlavor, gtidStr); err == nil {
		return set, gomysql.MariaDBFlavor
	}
	if set, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, gtidStr); err == nil {
		return set, gomysql.MySQLFlavor
	}
	return nil, ""
}

func (f *gtidSkipFilter) handleBinlog(binlogPath string) error {
	if f.checkpoint == nil {
		return f.inner.handleBinlog(binlogPath)
	}

	if f.pending != "" {
		if err := f.resolvePending(binlogPath); err != nil {
			return err
		}
	}

	if f.skipping {
		f.pending = binlogPath
		return nil
	}

	return f.inner.handleBinlog(binlogPath)
}

// flush forwards a still-pending file once the stream ends -- nothing left
// to compare against, so replay it rather than silently drop data.
func (f *gtidSkipFilter) flush() error {
	if f.pending == "" {
		return nil
	}
	pending := f.pending
	f.pending = ""
	return f.inner.handleBinlog(pending)
}

// resolvePending checks whether the pending file is fully covered by
// checkpoint, using nextPath's starting GTID set. On any failure or new
// content it forwards pending and turns off skipping for the rest of the run.
func (f *gtidSkipFilter) resolvePending(nextPath string) error {
	pendingPath := f.pending
	f.pending = ""
	pendingName := path.Base(pendingPath)

	endState, err := f.getPreviousGTIDs(nextPath, f.flavor)
	if err != nil {
		tracelog.WarningLogger.Printf(
			"could not determine GTID checkpoint for %s (from %s): %v -- replaying it to be safe",
			pendingName, path.Base(nextPath), err)
		f.skipping = false
		return f.inner.handleBinlog(pendingPath)
	}

	if f.checkpoint.Contain(endState) {
		tracelog.InfoLogger.Printf("skipping %s (already covered by checkpoint %s)", pendingName, f.checkpoint.String())
		os.Remove(pendingPath)
		return nil
	}

	tracelog.InfoLogger.Printf("replaying %s (introduces GTIDs beyond checkpoint %s)", pendingName, f.checkpoint.String())
	f.skipping = false
	return f.inner.handleBinlog(pendingPath)
}
