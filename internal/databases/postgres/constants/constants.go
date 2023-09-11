package constants

// This package contents common constants to be used by other tools (i.e. WAL-G daemon client)

// Looking at sysexits.h, EX_IOERR (74) is defined as a generic exit code for input/output errors
// It is used in wal-fetch (and daemon-client wal-fetch) to signal that the requested file does not exist.
const ExIoError = 74
