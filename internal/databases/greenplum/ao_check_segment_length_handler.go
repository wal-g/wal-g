package greenplum

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type DBInfo struct {
	DBName string
	Oid    pgtype.OID
}

type RelNames struct {
	FileName   pgtype.OID
	TableName  string
	SegRelName string
	Size       int64
}

type AOLengthCheckSegmentHandler struct {
	port   string
	segnum string
}

func NewAOLengthCheckSegmentHandler(port, segnum string) (*AOLengthCheckSegmentHandler, error) {
	return &AOLengthCheckSegmentHandler{
		port:   port,
		segnum: segnum}, nil
}

func (h *AOLengthCheckSegmentHandler) CheckAOTableLengthSegment() {
	initialConn, err := postgres.Connect(func(config *pgx.ConnConfig) error {
		a, err := strconv.Atoi(h.port)
		if err != nil {
			return err
		}
		config.Port = uint16(a)
		return nil
	})
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
	}

	DBNames, err := h.GetDatabasesInfo(initialConn)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to list databases %v", err)
	}

	//connection needs to be closed to prevent conflicts when opening another
	err = initialConn.Close()
	if err != nil {
		tracelog.WarningLogger.Println("failed close conn")
	}

	for _, db := range DBNames {
		tracelog.DebugLogger.Println(db.DBName)
		conn, err := postgres.Connect(func(config *pgx.ConnConfig) error {
			a, err := strconv.Atoi(h.port)
			if err != nil {
				return err
			}
			config.Port = uint16(a)
			config.Database = db.DBName
			return nil
		})
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
		}

		rows, err := conn.Query(`SELECT a.relfilenode file, a.relname tname, b.relname segname 
	FROM (SELECT relname, relid, segrelid, relpersistence, relfilenode FROM pg_class JOIN pg_appendonly ON oid = relid) a,
	(SELECT relname, segrelid FROM pg_class JOIN pg_appendonly ON oid = segrelid) b
	WHERE a.relpersistence = 'p' AND a.segrelid = b.segrelid;`)

		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to get ao/aocs tables %v", err)
		}
		defer rows.Close()

		AOTables := make([]RelNames, 0)
		for rows.Next() {
			row := RelNames{}
			if err := rows.Scan(&row.FileName, &row.TableName, &row.SegRelName); err != nil {
				tracelog.ErrorLogger.FatalfOnError("unable to parse query output %v", err)
			}
			AOTables = append(AOTables, row)
		}

		AOTablesSize := make(map[string]RelNames, 0)
		for _, table := range AOTables {
			table.Size, err = h.GetTableMetadataEOF(table, conn)
			if err != nil {
				tracelog.ErrorLogger.FatalfOnError("unable to get table metadata %v", err)
			}
			AOTablesSize[fmt.Sprintf("%d", table.FileName)] = table
			tracelog.DebugLogger.Printf("table: %s size: %d", table.TableName, table.Size)
		}

		tracelog.DebugLogger.Printf("AO/AOCS relations in db: %d", len(AOTablesSize))

		entries, err := os.ReadDir(fmt.Sprintf("/var/lib/greenplum/data1/primary/%s/base/%d/", fmt.Sprintf("gpseg%s", h.segnum), db.Oid))
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to list tables` file directory %v", err)
		}

		for _, file := range entries {
			parts := strings.Split(file.Name(), ".")
			f, err := file.Info()
			if err != nil {
				tracelog.ErrorLogger.FatalfOnError("unable to get file data %v", err)
			}
			if !f.IsDir() {
				tem, ok := AOTablesSize[parts[0]]
				if !ok {
					tracelog.DebugLogger.Printf("no metadata for file %s", parts[0])
					continue
				}
				tracelog.DebugLogger.Printf("found file for table: %s with size: %d", tem.TableName, f.Size())
				tem.Size -= f.Size()
				AOTablesSize[parts[0]] = tem
			}
		}

		errors := make([]string, 0)
		for _, v := range AOTablesSize {
			if v.Size > 0 {
				errors = append(errors, fmt.Sprintf("file for table %s is shorter than expected for %d", v.TableName, v.Size))
			}
		}
		if len(errors) > 0 {
			tracelog.ErrorLogger.Fatalf("check failed, tables files are too short:\n%s\n", strings.Join(errors, "\n"))
		}

		err = conn.Close()
		if err != nil {
			tracelog.WarningLogger.Println("failed to close connection")
		}
	}

}

func (h *AOLengthCheckSegmentHandler) GetDatabasesInfo(conn *pgx.Conn) ([]DBInfo, error) {
	rows, err := conn.Query("SELECT datname, oid FROM pg_database WHERE datallowconn")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make([]DBInfo, 0)
	for rows.Next() {
		tem := DBInfo{}
		if err = rows.Scan(&tem.DBName, &tem.Oid); err != nil {
			return nil, err
		}
		tracelog.DebugLogger.Printf("existing table: %s oid: %d", tem.DBName, tem.Oid)
		names = append(names, tem)
	}

	return names, nil
}

func (h *AOLengthCheckSegmentHandler) GetTableMetadataEOF(row RelNames, conn *pgx.Conn) (int64, error) {
	query := ""
	if !strings.Contains(row.SegRelName, "aocs") {
		query = fmt.Sprintf("SELECT sum(eofuncompressed) FROM pg_aoseg.%s", row.SegRelName)
	} else {
		query = fmt.Sprintf("SELECT sum(eof_uncompressed) FROM gp_toolkit.__gp_aocsseg('\"%s\"')", row.TableName)
	}

	// get expected size of table in metadata
	size, err := conn.Query(query)
	if err != nil {
		return 0, err
	}
	defer size.Close()
	var metaEOF int64
	for size.Next() {
		err = size.Scan(&metaEOF)
		if err != nil {
			metaEOF = int64(0)
		}
	}
	return metaEOF, nil
}
