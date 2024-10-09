package greenplum

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type dbInfo struct {
	DBName string
	Oid    pgtype.OID
}

type relNames struct {
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

func (checker *AOLengthCheckSegmentHandler) CheckAOTableLengthSegment() {
	DBNames, err := checker.getDatabasesInfo()
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to list databases %v", err)
	}

	for _, db := range DBNames {
		tracelog.DebugLogger.Println(db.DBName)
		conn, err := checker.connect(db.DBName)
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
		}

		AOTablesSize, err := checker.getTablesSizes(conn, db.Oid)
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to get metadata EOF %v", err)
		}
		tracelog.DebugLogger.Printf("AO/AOCS relations in db: %d", len(AOTablesSize))

		entries, err := os.ReadDir(fmt.Sprintf("/var/lib/greenplum/data1/primary/%s/base/%d/", fmt.Sprintf("gpseg%s", checker.segnum), db.Oid))
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to list tables` file directory %v", err)
		}

		for _, file := range entries {
			fileName := fmt.Sprintf("/base/%d/%s", db.Oid, strings.Split(file.Name(), ".")[0])
			fileInfo, err := file.Info()
			if err != nil {
				if os.IsNotExist(err) {
					tracelog.WarningLogger.Println(fileName, " deleted during filepath walk")
				} else {
					tracelog.ErrorLogger.FatalfOnError("unable to get file data %v", err)
				}
			}
			if !fileInfo.IsDir() {
				metaInfo, ok := AOTablesSize[fileName]
				if !ok {
					continue
				}
				tracelog.DebugLogger.Printf("found file for table: %s with size: %d", metaInfo.TableName, fileInfo.Size())
				metaInfo.Size -= fileInfo.Size()
				AOTablesSize[fileName] = metaInfo
			}
		}

		errors := checker.checkFileSizes(AOTablesSize)
		if len(errors) > 0 {
			tracelog.ErrorLogger.Fatalf("ao table length check failed, tables files are too short:\n%s\n", strings.Join(errors, "\n"))
		}

		err = conn.Close()
		if err != nil {
			tracelog.WarningLogger.Println("failed to close connection")
		}
	}
	tracelog.InfoLogger.Println("ao table length check passed")
}

func (checker *AOLengthCheckSegmentHandler) CheckAOBackupLengthSegment(backupName string) {
	DBNames, err := checker.getDatabasesInfo()
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to list databases %v", err)
	}

	s3Files, err := checker.getAOBackupFilesData()
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get files data from s3 %v", err)
	}

	backupFilesMetadata, err := checker.getAOMetadata(backupName)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get backup data %v", err)
	}
	tracelog.DebugLogger.Printf("AO/AOCS backupped files count: %d", len(backupFilesMetadata))

	errors := make([]string, 0)

	for _, db := range DBNames {
		entries, err := os.ReadDir(fmt.Sprintf("/var/lib/greenplum/data1/primary/%s/base/%d/", fmt.Sprintf("gpseg%s", checker.segnum), db.Oid))
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("unable to list tables` file directory %v", err)
		}

		for _, file := range entries {
			fileName := fmt.Sprintf("/base/%d/%s", db.Oid, file.Name())
			fileInfo, err := file.Info()
			if err != nil {
				if os.IsNotExist(err) {
					tracelog.WarningLogger.Println(fileName, " deleted during filepath walk")
				} else {
					tracelog.ErrorLogger.FatalfOnError("unable to get file data %v", err)
				}
			}
			backupFile, ok := backupFilesMetadata[fileName]
			if !ok {
				continue
			}
			tracelog.DebugLogger.Printf("found file : %s with size: %d", fileName, fileInfo.Size())
			if backupFile.EOF > fileInfo.Size() {
				errors = append(errors, fmt.Sprintf("table file %s is shorter than backup for %d", fileName, backupFile.EOF-fileInfo.Size()))
			}

			if backupFile.EOF > s3Files[backupFile.StoragePath] {
				errors = append(errors, fmt.Sprintf("s3 file %s is shorter than backup for %d",
					backupFile.StoragePath, backupFile.EOF-s3Files[backupFile.StoragePath]))
			}
		}
	}

	if len(errors) > 0 {
		tracelog.ErrorLogger.Fatalf("ao backup length check failed, backup is too long:\n%s\n", strings.Join(errors, "\n"))
	}
	tracelog.InfoLogger.Println("ao backup length check passed")
}

func (checker *AOLengthCheckSegmentHandler) checkFileSizes(AOTablesSize map[string]relNames) []string {
	errors := make([]string, 0)
	for _, v := range AOTablesSize {
		if v.Size > 0 {
			errors = append(errors, fmt.Sprintf("file for table %s is shorter than expected for %d", v.TableName, v.Size))
		}
	}
	return errors
}

func (checker *AOLengthCheckSegmentHandler) connect(db string) (*pgx.Conn, error) {
	return postgres.Connect(func(config *pgx.ConnConfig) error {
		a, err := strconv.Atoi(checker.port)
		if err != nil {
			return err
		}
		config.Port = uint16(a)
		if db != "" {
			config.Database = db
		}
		return nil
	})
}

func (checker *AOLengthCheckSegmentHandler) getTablesSizes(conn *pgx.Conn, dbOID pgtype.OID) (map[string]relNames, error) {
	rows, err := conn.Query(`SELECT a.relfilenode file, a.relname tname, b.relname segname 
	FROM (SELECT relname, relid, segrelid, relpersistence, relfilenode FROM pg_class JOIN pg_appendonly ON oid = relid) a,
	(SELECT relname, segrelid FROM pg_class JOIN pg_appendonly ON oid = segrelid) b
	WHERE a.relpersistence = 'p' AND a.segrelid = b.segrelid;`)
	if err != nil {
		return nil, fmt.Errorf("unable to get ao/aocs tables %v", err)
	}
	defer rows.Close()

	AOTables := make([]relNames, 0)
	for rows.Next() {
		row := relNames{}
		if err := rows.Scan(&row.FileName, &row.TableName, &row.SegRelName); err != nil {
			return nil, fmt.Errorf("unable to parse query output %v", err)
		}
		AOTables = append(AOTables, row)
	}

	AOTablesSize := make(map[string]relNames, 0)
	for _, table := range AOTables {
		table.Size, err = checker.getTableMetadataEOF(table, conn)
		if err != nil {
			return nil, fmt.Errorf("unable to get table metadata %v", err)
		}
		AOTablesSize[fmt.Sprintf("/base/%d/%d", dbOID, table.FileName)] = table
		tracelog.DebugLogger.Printf("table: %s size: %d", table.TableName, table.Size)
	}
	return AOTablesSize, nil
}

func (checker *AOLengthCheckSegmentHandler) getDatabasesInfo() ([]dbInfo, error) {
	conn, err := checker.connect("")
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			tracelog.WarningLogger.Println("failed close conn")
		}
	}()

	rows, err := conn.Query("SELECT datname, oid FROM pg_database WHERE datallowconn")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make([]dbInfo, 0)
	for rows.Next() {
		db := dbInfo{}
		if err = rows.Scan(&db.DBName, &db.Oid); err != nil {
			return nil, err
		}
		tracelog.DebugLogger.Printf("existing database: %s oid: %d", db.DBName, db.Oid)
		names = append(names, db)
	}

	return names, nil
}

func (checker *AOLengthCheckSegmentHandler) getTableMetadataEOF(row relNames, conn *pgx.Conn) (int64, error) {
	//query to get expected table size in metadata
	query := ""
	if !strings.Contains(row.SegRelName, "aocs") {
		query = fmt.Sprintf("SELECT sum(eof) FROM pg_aoseg.%s", row.SegRelName)
	} else {
		query = fmt.Sprintf("SELECT sum(eof) FROM gp_toolkit.__gp_aocsseg('\"%s\"')", row.TableName)
	}

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

func (checker *AOLengthCheckSegmentHandler) getAOMetadata(backupName string) (BackupAOFiles, error) {
	storage, err := internal.ConfigureStorage()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to configure folder")
		return nil, err
	}
	rootFolder := storage.RootFolder()

	var backup internal.Backup

	backup, err = internal.GetBackupByName(backupName, fmt.Sprintf("segments_005/seg%s/basebackups_005/", checker.segnum), rootFolder)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to get backup with name: %s", backupName)
		return nil, err
	}

	tracelog.DebugLogger.Printf("backup %s", backup.Name)
	files := NewAOFilesMetadataDTO()

	err = internal.FetchDto(backup.Folder, &files, fmt.Sprintf("%s/ao_files_metadata.json", backup.Name))
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to fetch file data")
		return nil, err
	}

	tracelog.DebugLogger.Printf("successfully loaded file metadata from backup")

	return files.Files, nil
}

func (checker *AOLengthCheckSegmentHandler) getAOBackupFilesData() (map[string]int64, error) {
	storage, err := internal.ConfigureStorage()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to configure folder")
		return nil, err
	}
	rootFolder := storage.RootFolder()
	aoFolder := rootFolder.GetSubFolder(fmt.Sprintf("segments_005/seg%s/basebackups_005/aosegments/", checker.segnum))

	files, _, err := aoFolder.ListFolder()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to list s3 objects: %v", err)
		return nil, err
	}

	fileData := make(map[string]int64, len(files))

	for _, file := range files {
		fileData[file.GetName()] = file.GetSize()
	}

	tracelog.DebugLogger.Printf("successfully loaded file data from backup")

	return fileData, nil
}
