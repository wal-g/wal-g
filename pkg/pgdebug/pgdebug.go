// Package pgdebug serves a minimal PostgreSQL wire protocol debug server.
// It answers simple SHOW queries so that regular clients (psql, drivers)
// can connect to wal-g tooling and inspect its state.
package pgdebug

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// maxMessageBodyLen bounds the body length of a single regular message,
// protecting the server from malicious or broken clients announcing
// huge message lengths (the default pgproto3 limit is unlimited)
const maxMessageBodyLen = 1 << 20

// BackupStats describes the latest upload wal-g is aware of.
type BackupStats struct {
	Name              string // backup name
	StartLSN          string // backup start LSN
	StartTime         string // backup start time
	FilesUploaded     int64  // number of uploaded files, -1 if unknown
	BytesUploaded     int64  // bytes uploaded to the storage (compressed)
	BytesUncompressed int64  // uncompressed data size
}

// Options configures the debug server.
type Options struct {
	// Version is reported by SHOW VERSION.
	Version string

	// BackupStatsProvider, when set, enables SHOW BACKUP_STATS which
	// reports how far the backup upload has progressed.
	BackupStatsProvider func() (BackupStats, error)
}

// Serve accepts connections on l until Accept fails.
func Serve(l net.Listener, opts Options) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer c.Close()
			h := &conn{c: c, opts: opts, backend: pgproto3.NewBackend(c, c)}
			h.backend.SetMaxBodyLen(maxMessageBodyLen)
			h.run()
		}()
	}
}

type conn struct {
	c       net.Conn
	opts    Options
	backend *pgproto3.Backend
}

// run reads the startup sequence, then serves queries until
// the client disconnects or a protocol violation occurs.
// A read error or a malformed packet closes the connection,
// never the whole process: all size validation is done by pgproto3.
func (c *conn) run() {
	err := c.handshake()
	if err != nil {
		return
	}
	for {
		msg, err := c.backend.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			c.handleQuery(m.String)
		case *pgproto3.Terminate:
			return
		default:
			// an unsupported message type terminates the connection
			return
		}
	}
}

// handshake reads the startup sequence: SSL/GSS encryption requests are
// declined, then the startup message is answered with AuthenticationOk.
func (c *conn) handshake() error {
	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			// encryption is not supported, reply with a plain 'N'
			// directly on the socket, as the protocol requires
			if _, err := c.c.Write([]byte{'N'}); err != nil {
				return err
			}
		case *pgproto3.CancelRequest:
			return fmt.Errorf("unexpected CancelRequest")
		case *pgproto3.StartupMessage:
			c.backend.Send(&pgproto3.AuthenticationOk{})
			c.backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: c.opts.Version})
			c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return c.backend.Flush()
		default:
			return fmt.Errorf("unexpected startup message %T", msg)
		}
	}
}

// handleQuery answers the supported SHOW queries,
// rejecting everything else with a normal protocolErrorResponse.
func (c *conn) handleQuery(query string) {
	q := normalizeQuery(query)
	switch q {
	case "show version", "show version;":
		c.sendRowset([]string{"version"}, []string{c.opts.Version})
	case "show backup_stats", "show backup_stats;":
		c.handleBackupStats()
	default:
		c.backend.Send(queryErrorResponse("42601",
			fmt.Sprintf("unsupported command %q", strings.TrimSpace(query))))
		c.sendReadyForQuery()
	}
}

// handleBackupStats implements SHOW BACKUP_STATS
func (c *conn) handleBackupStats() {
	if c.opts.BackupStatsProvider == nil {
		c.backend.Send(queryErrorResponse("0A000",
			"backup stats are not configured on this server"))
		c.sendReadyForQuery()
		return
	}
	stats, err := c.opts.BackupStatsProvider()
	if err != nil {
		c.backend.Send(queryErrorResponse("XX000",
			fmt.Sprintf("failed to collect backup stats: %v", err)))
		c.sendReadyForQuery()
		return
	}
	c.sendRowset(
		[]string{"name", "start_lsn", "start_time",
			"files_uploaded", "bytes_uploaded", "bytes_uncompressed"},
		[]string{
			stats.Name,
			stats.StartLSN,
			stats.StartTime,
			strconv.FormatInt(stats.FilesUploaded, 10),
			strconv.FormatInt(stats.BytesUploaded, 10),
			strconv.FormatInt(stats.BytesUncompressed, 10),
		})
}

// sendRowset sends a single-row result in the text format
func (c *conn) sendRowset(columns, values []string) {
	c.backend.Send(rowDescription(columns))
	c.backend.Send(dataRow(values))
	c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
	c.sendReadyForQuery()
}

func (c *conn) sendReadyForQuery() {
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = c.backend.Flush()
}

func queryErrorResponse(code, message string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	}
}

// normalizeQuery lowercases the query and drops a trailing semicolon,
// so that both psql's "SHOW VERSION;" and plain "show version" work
func normalizeQuery(q string) string {
	q = strings.TrimRight(q, "\x00 \t\n\r;")
	return strings.ToLower(strings.TrimSpace(q))
}

// textColumn describes a result column of the text type.
// The unspecified type modifier is -1, not 0.
func textColumn(name string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:        []byte(name),
		DataTypeOID: 25, // text
		// table OID, column number and format: unspecified, text
		TableAttributeNumber: 0,
		Format:               0,
		DataTypeSize:         -1,
		TypeModifier:         -1,
	}
}

func rowDescription(columns []string) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(columns))
	for i, col := range columns {
		fields[i] = textColumn(col)
	}
	return &pgproto3.RowDescription{Fields: fields}
}

func dataRow(values []string) *pgproto3.DataRow {
	cells := make([][]byte, len(values))
	for i, v := range values {
		cells[i] = []byte(v)
	}
	return &pgproto3.DataRow{Values: cells}
}
