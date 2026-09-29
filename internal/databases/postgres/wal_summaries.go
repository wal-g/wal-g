package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/walparser"
)

// WAL summaries are written by walsummarizer (PG17+, summarize_wal=on),
// each covering [start_lsn, end_lsn) of one timeline

const walSummarizerWaitTimeout = time.Minute

type walSummary struct {
	startLSN LSN
	endLSN   LSN
}

// ReadWalSummaryDeltaMap collects main fork blocks changed in [firstUsedLSN, firstNotUsedLSN) on timeline.
// Summaries do not follow timeline history, so range must not span a promotion
func (queryRunner *PgQueryRunner) ReadWalSummaryDeltaMap(ctx context.Context, timeline uint32,
	firstUsedLSN, firstNotUsedLSN LSN) (PagedFileDeltaMap, error) {
	if err := queryRunner.waitWalSummarized(ctx, firstNotUsedLSN); err != nil {
		return nil, err
	}

	queryRunner.Mu.Lock()
	defer queryRunner.Mu.Unlock()
	conn := queryRunner.Connection

	summaries, err := listWalSummaries(ctx, conn, timeline, firstUsedLSN, firstNotUsedLSN)
	if err != nil {
		return nil, err
	}
	if err := checkWalSummaryCoverage(summaries, firstUsedLSN, firstNotUsedLSN); err != nil {
		return nil, errors.Wrapf(err, "timeline %d (a timeline switch since base backup needs a full backup)", timeline)
	}

	starts := make([]string, len(summaries))
	ends := make([]string, len(summaries))
	for i, summary := range summaries {
		starts[i] = summary.startLSN.String()
		ends[i] = summary.endLSN.String()
	}
	// Summary removed after listing makes pg_wal_summary_contents error out instead of silently shrinking delta
	rows, err := conn.Query(ctx, "SELECT c.reltablespace, c.reldatabase, c.relfilenode, c.relblocknumber, c.is_limit_block"+
		" FROM unnest($2::text[]::pg_lsn[], $3::text[]::pg_lsn[]) AS s(start_lsn, end_lsn),"+
		" pg_catalog.pg_wal_summary_contents($1, s.start_lsn, s.end_lsn) AS c WHERE c.relforknumber = 0",
		timeline, starts, ends)
	if err != nil {
		return nil, errors.Wrap(err, "reading WAL summaries")
	}
	defer rows.Close()
	deltaMap := NewPagedFileDeltaMap()
	for rows.Next() {
		var rel walparser.RelFileNode
		var block int64
		var isLimit bool
		if err := rows.Scan(&rel.SpcNode, &rel.DBNode, &rel.RelNode, &block, &isLimit); err != nil {
			return nil, err
		}
		location := walparser.BlockLocation{RelationFileNode: rel, BlockNo: uint32(block)}
		deltaMap.AddLocationToDelta(location)
		if isLimit {
			// Relation truncated to block, so blocks past it may be re-extended without WAL,
			// back them all up as basebackup_incremental.c does
			deltaMap[rel].AddRange(uint64(block), 1<<32)
		}
	}
	return deltaMap, rows.Err()
}

func listWalSummaries(ctx context.Context, conn *pgx.Conn, timeline uint32,
	firstUsedLSN, firstNotUsedLSN LSN) ([]walSummary, error) {
	rows, err := conn.Query(ctx, "SELECT start_lsn::text, end_lsn::text FROM pg_catalog.pg_available_wal_summaries()"+
		" WHERE tli = $1 AND end_lsn > $2::pg_lsn AND start_lsn < $3::pg_lsn ORDER BY start_lsn",
		timeline, firstUsedLSN.String(), firstNotUsedLSN.String())
	if err != nil {
		return nil, errors.Wrap(err, "listing WAL summaries")
	}
	defer rows.Close()
	var summaries []walSummary
	for rows.Next() {
		var start, end string
		if err := rows.Scan(&start, &end); err != nil {
			return nil, err
		}
		var summary walSummary
		if summary.startLSN, err = ParseLSN(start); err != nil {
			return nil, err
		}
		if summary.endLSN, err = ParseLSN(end); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, rows.Err()
}

// waitWalSummarized blocks until walsummarizer has summarized WAL up to lsn,
// as backup start LSN is usually ahead of it
func (queryRunner *PgQueryRunner) waitWalSummarized(ctx context.Context, lsn LSN) error {
	ctx, cancel := context.WithTimeout(ctx, walSummarizerWaitTimeout)
	defer cancel()
	for {
		queryRunner.Mu.Lock()
		var summarized string
		err := queryRunner.Connection.QueryRow(ctx,
			"SELECT summarized_lsn::text FROM pg_catalog.pg_get_wal_summarizer_state()").Scan(&summarized)
		queryRunner.Mu.Unlock()
		if err != nil {
			return errors.Wrapf(err, "waiting for WAL summarization up to %s", lsn)
		}
		summarizedLSN, err := ParseLSN(summarized)
		if err != nil {
			return err
		}
		if summarizedLSN >= lsn {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "WAL summarized up to %s, waiting for %s", summarizedLSN, lsn)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// checkWalSummaryCoverage asserts summaries sorted by startLSN cover [firstUsedLSN, firstNotUsedLSN) without gaps
func checkWalSummaryCoverage(summaries []walSummary, firstUsedLSN, firstNotUsedLSN LSN) error {
	covered := firstUsedLSN
	for _, summary := range summaries {
		if summary.startLSN > covered {
			return errors.Errorf("no WAL summary covers [%s, %s)", covered, summary.startLSN)
		}
		covered = max(covered, summary.endLSN)
	}
	if covered < firstNotUsedLSN {
		return errors.Errorf("no WAL summary covers [%s, %s)", covered, firstNotUsedLSN)
	}
	return nil
}
