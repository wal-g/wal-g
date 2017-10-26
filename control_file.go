package walg

/*
#include <inttypes.h>

typedef enum DBState
{
	DB_STARTUP = 0,
	DB_SHUTDOWNED,
	DB_SHUTDOWNED_IN_RECOVERY,
	DB_SHUTDOWNING,
	DB_IN_CRASH_RECOVERY,
	DB_IN_ARCHIVE_RECOVERY,
	DB_IN_PRODUCTION
} DBState;

#define bool char
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int64_t int64;
typedef uint64 XLogRecPtr;
typedef uint32 TimeLineID;
typedef uint32 TransactionId;
typedef unsigned int Oid;
typedef TransactionId MultiXactId;
typedef uint32 MultiXactOffset;
typedef uint32 CommandId;
typedef int64 pg_time_t;

typedef struct CheckPoint
{
XLogRecPtr	redo;

TimeLineID	ThisTimeLineID;
TimeLineID	PrevTimeLineID;

bool		fullPageWrites;
uint32		nextXidEpoch;
TransactionId nextXid;
Oid			nextOid;
MultiXactId nextMulti;
MultiXactOffset nextMultiOffset;
TransactionId oldestXid;
Oid			oldestXidDB;
MultiXactId oldestMulti;
Oid			oldestMultiDB;
pg_time_t	time;
TransactionId oldestCommitTsXid;
TransactionId newestCommitTsXid;

TransactionId oldestActiveXid;
} CheckPoint;


// Contents of pg_control.


typedef struct ControlFileData
{
uint64		system_identifier;

uint32		pg_control_version;
uint32		catalog_version_no;

DBState		state;
pg_time_t	time;
XLogRecPtr	checkPoint;
XLogRecPtr	prevCheckPoint;

CheckPoint	checkPointCopy;

XLogRecPtr	unloggedLSN;


XLogRecPtr	minRecoveryPoint;
TimeLineID	minRecoveryPointTLI;
XLogRecPtr	backupStartPoint;
XLogRecPtr	backupEndPoint;
bool		backupEndRequired;

int			wal_level;
bool		wal_log_hints;
int			MaxConnections;
int			max_worker_processes;
int			max_prepared_xacts;
int			max_locks_per_xact;
bool		track_commit_timestamp;


uint32		maxAlign;
double		floatFormat;
#define FLOATFORMAT_VALUE	1234567.0

uint32		blcksz;
uint32		relseg_size;

uint32		xlog_blcksz;
uint32		xlog_seg_size;

uint32		nameDataLen;
uint32		indexMaxKeys;

uint32		toast_max_chunk_size;
uint32		loblksize;


bool		float4ByVal;
bool		float8ByVal;

uint32		data_checksum_version;


//char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];

//pg_crc32c	crc;
} ControlFileData;

uint32 GetTimelineFromControlFileData(void* ptr)
 {
 	ControlFileData* data = (ControlFileData*) ptr;
 	return data->checkPointCopy.ThisTimeLineID;
}
*/
import "C"
import (
	"io/ioutil"
	"unsafe"
	"fmt"
	"strings"
	"strconv"
	"errors"
)

func readTimelineFromControlFile(fileName string) (uint32, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, err
	}
	timeline := C.GetTimelineFromControlFileData(unsafe.Pointer(&data[0]))
	return uint32(timeline), nil
}

func ParseLsn(lsnStr string) (lsn uint64, err error) {
	lsnArray := strings.SplitN(lsnStr, "/", 2)

	//Postgres format it's LSNs as two hex numbers separated by /
	const sizeofInt32 = 4
	highLsn, err := strconv.ParseUint(lsnArray[0], 0x10, sizeofInt32*8)
	lowLsn, err2 := strconv.ParseUint(lsnArray[1], 0x10, sizeofInt32*8)
	if err != nil || err2 != nil {
		err = errors.New("Unable to parse LSN " + lsnStr)
	}

	lsn = highLsn<<32 + lowLsn
	return
}

func WALFileName(lsnStr string, pgcontrol string) (string, error) {
	lsn, err := ParseLsn(lsnStr)
	if err != nil {
		return "", err
	}

	timeline, err := readTimelineFromControlFile(pgcontrol)
	if err != nil {
		return "", err
	}

	walSegSize := uint64(16 * 1024 * 1024)
	XLogSegmentsPerXLogId := 0x100000000 / walSegSize
	logSegNo := (lsn - uint64(1)) / walSegSize

	return fmt.Sprintf("%08X%08X%08X", timeline, logSegNo/XLogSegmentsPerXLogId, logSegNo%XLogSegmentsPerXLogId), nil
}
