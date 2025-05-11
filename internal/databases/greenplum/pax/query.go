package pax

import (
	"context"
	"fmt"
	"maps"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/walparser"
)

const paxRelationsQuery = `
SELECT
    c.oid                                                                  AS pax_oid,
    md5(quote_ident(n.nspname) || '.' || quote_ident(c.relname))           AS rel_md5,
    c.relfilenode,
    c.reltablespace,
    paxt.auxrelid::regclass::text                                          AS aux_table_fqn
FROM pg_class c
JOIN pg_am am          ON c.relam = am.oid
JOIN pg_namespace n    ON c.relnamespace = n.oid
JOIN pg_ext_aux.pg_pax_tables paxt ON paxt.relid = c.oid
WHERE am.amname = 'pax' AND c.relpersistence = 'p';
`

type paxRelation struct {
	oid         uint32
	relMd5      string
	relfilenode uint32
	spcNode     uint32
	auxTableFqn string
}

// FetchStorageMetadata queries the connected database for every PAX file referenced
// from the aux catalog (`pg_ext_aux.pg_pax_tables` and `pg_ext_aux.pg_pax_blocks_*`)
// and returns one RelFileStorageMap entry per data / toast / visimap file.
//
// If the `pg_ext_aux` schema or `pg_pax_tables` is absent (PAX extension not loaded
// in this database), the function returns an empty map and a non-nil error so the
// caller can decide whether to ignore it or surface a warning.
func FetchStorageMetadata(ctx context.Context, conn *pgx.Conn, dbInfo postgres.PgDatabaseInfo) (RelFileStorageMap, error) {
	relations, err := fetchPaxRelations(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list PAX relations")
	}

	result := make(RelFileStorageMap)
	for _, r := range relations {
		spc := walparser.Oid(r.spcNode)
		if spc == 0 {
			spc = dbInfo.TblSpcOid
		}
		blocks, err := fetchPaxBlocks(ctx, conn, dbInfo, spc, r)
		if err != nil {
			tracelog.WarningLogger.Printf(
				"failed to load PAX blocks for relation oid=%d (%s) in db=%s: %v",
				r.oid, r.auxTableFqn, dbInfo.Name, err)
		}
		maps.Copy(result, blocks)
	}
	return result, nil
}

func fetchPaxRelations(ctx context.Context, conn *pgx.Conn) ([]paxRelation, error) {
	rows, err := conn.Query(ctx, paxRelationsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []paxRelation
	for rows.Next() {
		var r paxRelation
		if err := rows.Scan(&r.oid, &r.relMd5, &r.relfilenode, &r.spcNode, &r.auxTableFqn); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func fetchPaxBlocks(ctx context.Context, conn *pgx.Conn, dbInfo postgres.PgDatabaseInfo,
	spcNode walparser.Oid, r paxRelation) (RelFileStorageMap, error) {
	result := make(RelFileStorageMap)
	// table FQN comes from regclass::text, which is pre-escaped by Postgres. It is safe to put into queries.
	query := fmt.Sprintf("SELECT ptblockname, ptvisimapname, ptexistexttoast FROM %s", r.auxTableFqn)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keyBase := FileKey{
		SpcNode:     spcNode,
		DBNode:      dbInfo.Oid,
		RelFileNode: walparser.Oid(r.relfilenode),
	}

	for rows.Next() {
		var blockID int64
		var visimapName *string
		var hasToast bool
		if err := rows.Scan(&blockID, &visimapName, &hasToast); err != nil {
			return nil, err
		}

		dataKey := keyBase
		dataKey.Filename = fmt.Sprintf("%d", blockID)
		result[dataKey] = RelFileMetadata{
			RelNameMd5: r.relMd5,
			BlockID:    blockID,
			Kind:       FileKindData,
		}

		if hasToast {
			toastKey := keyBase
			toastKey.Filename = fmt.Sprintf("%d.toast", blockID)
			result[toastKey] = RelFileMetadata{
				RelNameMd5: r.relMd5,
				BlockID:    blockID,
				Kind:       FileKindToast,
			}
		}

		if visimapName != nil && *visimapName != "" {
			visimapKey := keyBase
			visimapKey.Filename = *visimapName
			result[visimapKey] = RelFileMetadata{
				RelNameMd5: r.relMd5,
				BlockID:    blockID,
				Kind:       FileKindVisimap,
			}
		}
	}
	return result, rows.Err()
}
