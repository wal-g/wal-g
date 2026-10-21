package pgdebug

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// startTestServer starts the debug server on a random port
func startTestServer(t *testing.T, opts Options) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() { _ = Serve(l, opts) }()
	return l.Addr().String()
}

// dialAndStartup dials the server, declines SSL, sends the startup
// packet and returns a Frontend decoding the server messages
func dialAndStartup(t *testing.T, address string) (*net.TCPConn, *pgproto3.Frontend) {
	t.Helper()
	dialConn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dialConn.Close() })
	conn := dialConn.(*net.TCPConn)

	// SSLRequest -> expect a plain 'N'
	sslRequest := pgproto3.SSLRequest{}
	encoded, err := sslRequest.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(encoded); err != nil {
		t.Fatal(err)
	}
	reject := make([]byte, 1)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, reject); err != nil || reject[0] != 'N' {
		t.Fatalf("expected 'N' answer to SSLRequest, got %q err %v", reject[0], err)
	}

	frontend := pgproto3.NewFrontend(conn, conn)

	startup := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersion30,
		Parameters:      map[string]string{"user": "test"},
	}
	encoded, err = startup.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(encoded); err != nil {
		t.Fatal(err)
	}

	if _, ok := receive(t, conn, frontend).(*pgproto3.AuthenticationOk); !ok {
		t.Fatal("expected AuthenticationOk")
	}
	if got := receive(t, conn, frontend).(*pgproto3.ParameterStatus); got.Name != "server_version" {
		t.Fatalf("expected server_version ParameterStatus, got %q", got.Name)
	}
	if got := receive(t, conn, frontend).(*pgproto3.ReadyForQuery); got.TxStatus != 'I' {
		t.Fatalf("expected ReadyForQuery('I'), got %q", got.TxStatus)
	}
	return conn, frontend
}

// receive reads one message from the server, failing the test on errors
func receive(t *testing.T, conn *net.TCPConn, f *pgproto3.Frontend) pgproto3.BackendMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	msg, err := f.Receive()
	if err != nil {
		t.Fatalf("failed to receive message: %v", err)
	}
	return msg
}

// TestShowVersion checks the full response to SHOW VERSION,
// decoding the messages with the same pgproto3 parser psql uses
func TestShowVersion(t *testing.T) {
	address := startTestServer(t, Options{Version: "test-1.0"})
	conn, frontend := dialAndStartup(t, address)

	frontend.Send(&pgproto3.Query{String: "show version;"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	rowDesc := receive(t, conn, frontend).(*pgproto3.RowDescription)
	if len(rowDesc.Fields) != 1 {
		t.Fatalf("expected one column, got %d", len(rowDesc.Fields))
	}
	field := rowDesc.Fields[0]
	if string(field.Name) != "version" {
		t.Fatalf("expected column 'version', got %q", field.Name)
	}
	// the reviewer reproduced the OID written as {25, 0, 0, 0}:
	// decoding it must yield exactly the text OID, with -1 modifiers
	if field.DataTypeOID != 25 {
		t.Fatalf("expected text OID 25 in network byte order, got %d", field.DataTypeOID)
	}
	if field.TypeModifier != -1 {
		t.Fatalf("expected unspecified type modifier -1, got %d", field.TypeModifier)
	}
	if field.DataTypeSize != -1 {
		t.Fatalf("expected typlen -1, got %d", field.DataTypeSize)
	}
	if field.Format != 0 {
		t.Fatalf("expected text format, got %d", field.Format)
	}

	dataRow := receive(t, conn, frontend).(*pgproto3.DataRow)
	if len(dataRow.Values) != 1 || string(dataRow.Values[0]) != "test-1.0" {
		t.Fatalf("unexpected data row: %q", dataRow.Values)
	}
	if got := receive(t, conn, frontend).(*pgproto3.CommandComplete); string(got.CommandTag) != "SHOW" {
		t.Fatalf("expected CommandComplete 'SHOW', got %q", got.CommandTag)
	}
	if got := receive(t, conn, frontend).(*pgproto3.ReadyForQuery); got.TxStatus != 'I' {
		t.Fatalf("expected ReadyForQuery('I'), got %q", got.TxStatus)
	}
}

// TestShowBackupStats checks the SHOW BACKUP_STATS row: bytes and files
// uploaded so far plus the backup start position and time
func TestShowBackupStats(t *testing.T) {
	stats := BackupStats{
		Name:              "base_000000010000000000000002_01000000",
		StartLSN:          "0/2000028",
		StartTime:         "2026-10-21T12:34:56Z",
		FilesUploaded:     42,
		BytesUploaded:     12345,
		BytesUncompressed: 67890,
	}
	address := startTestServer(t, Options{
		Version: "test-1.0",
		BackupStatsProvider: func() (BackupStats, error) {
			return stats, nil
		},
	})
	conn, frontend := dialAndStartup(t, address)

	frontend.Send(&pgproto3.Query{String: "show backup_stats"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	rowDesc := receive(t, conn, frontend).(*pgproto3.RowDescription)
	wantColumns := []string{"name", "start_lsn", "start_time",
		"files_uploaded", "bytes_uploaded", "bytes_uncompressed"}
	if len(rowDesc.Fields) != len(wantColumns) {
		t.Fatalf("expected %d columns, got %d", len(wantColumns), len(rowDesc.Fields))
	}
	for i, col := range wantColumns {
		if string(rowDesc.Fields[i].Name) != col || rowDesc.Fields[i].DataTypeOID != 25 ||
			rowDesc.Fields[i].TypeModifier != -1 {
			t.Fatalf("column %d: expected text %q, got %+v", i, col, rowDesc.Fields[i])
		}
	}

	dataRow := receive(t, conn, frontend).(*pgproto3.DataRow)
	wantValues := []string{stats.Name, stats.StartLSN, stats.StartTime,
		"42", "12345", "67890"}
	if len(dataRow.Values) != len(wantValues) {
		t.Fatalf("expected %d values, got %d", len(wantValues), len(dataRow.Values))
	}
	for i, want := range wantValues {
		if string(dataRow.Values[i]) != want {
			t.Fatalf("value %d: expected %q, got %q", i, want, dataRow.Values[i])
		}
	}
}

// TestShowBackupStatsUnavailable checks the error response
// when the backup stats provider is not configured
func TestShowBackupStatsUnavailable(t *testing.T) {
	address := startTestServer(t, Options{Version: "test-1.0"})
	conn, frontend := dialAndStartup(t, address)

	frontend.Send(&pgproto3.Query{String: "show backup_stats"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	errResponse := receive(t, conn, frontend).(*pgproto3.ErrorResponse)
	if errResponse.Code != "0A000" {
		t.Fatalf("expected 0A000, got %q", errResponse.Code)
	}
	if got := receive(t, conn, frontend).(*pgproto3.ReadyForQuery); got.TxStatus != 'I' {
		t.Fatalf("expected ReadyForQuery('I'), got %q", got.TxStatus)
	}
}

// TestShowBackupStatsError checks the error response
// when the backup stats provider fails
func TestShowBackupStatsError(t *testing.T) {
	address := startTestServer(t, Options{
		Version: "test-1.0",
		BackupStatsProvider: func() (BackupStats, error) {
			return BackupStats{}, io.EOF
		},
	})
	conn, frontend := dialAndStartup(t, address)

	frontend.Send(&pgproto3.Query{String: "show backup_stats"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	errResponse := receive(t, conn, frontend).(*pgproto3.ErrorResponse)
	if errResponse.Code != "XX000" {
		t.Fatalf("expected XX000, got %q", errResponse.Code)
	}
}

// TestUnsupportedCommand checks that unknown queries get a proper
// ErrorResponse and the connection keeps serving afterwards
func TestUnsupportedCommand(t *testing.T) {
	address := startTestServer(t, Options{Version: "test-1.0"})
	conn, frontend := dialAndStartup(t, address)

	frontend.Send(&pgproto3.Query{String: "select 1"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	errResponse := receive(t, conn, frontend).(*pgproto3.ErrorResponse)
	if errResponse.Code != "42601" {
		t.Fatalf("expected 42601, got %q", errResponse.Code)
	}
	if got := receive(t, conn, frontend).(*pgproto3.ReadyForQuery); got.TxStatus != 'I' {
		t.Fatalf("expected ReadyForQuery('I'), got %q", got.TxStatus)
	}

	// the connection must still work after the error
	frontend.Send(&pgproto3.Query{String: "show version"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	rowDesc := receive(t, conn, frontend).(*pgproto3.RowDescription)
	if len(rowDesc.Fields) != 1 || string(rowDesc.Fields[0].Name) != "version" {
		t.Fatalf("unexpected row description after error: %+v", rowDesc.Fields)
	}
}

// TestMalformedStartupPackets checks that a startup packet with an
// invalid length is rejected without crashing the server. The reviewer
// reproduced a panic on a startup packet with length 4.
func TestMalformedStartupPackets(t *testing.T) {
	address := startTestServer(t, Options{Version: "test-1.0"})

	// lengths below 8 underflow/fail bounds checks, huge lengths
	// must be rejected before any allocation
	for _, msgLen := range []uint32{0, 1, 2, 3, 4, 5, 6, 7,
		0x7FFFFFFF, 0xFFFFFFF7, 999999999} {
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, msgLen)
		length = append(length, make([]byte, 16)...) // some body bytes
		if _, err := conn.Write(length); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		// the server must close the broken connection ...
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1)
		if n, err := conn.Read(buf); n > 0 || err == nil {
			_ = conn.Close()
			t.Fatalf("startup packet with length %d: expected the connection to be closed", msgLen)
		}
		_ = conn.Close()
	}

	// ... but keep serving well-formed connections
	conn, frontend := dialAndStartup(t, address)
	frontend.Send(&pgproto3.Query{String: "show version"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	rowDesc := receive(t, conn, frontend).(*pgproto3.RowDescription)
	if len(rowDesc.Fields) != 1 || rowDesc.Fields[0].DataTypeOID != 25 {
		t.Fatalf("server does not work after malformed startup packets: %+v", rowDesc.Fields)
	}
}

// TestMalformedRegularMessages checks that regular messages with invalid
// lengths are rejected by the length bound instead of being allocated
func TestMalformedRegularMessages(t *testing.T) {
	address := startTestServer(t, Options{Version: "test-1.0"})

	for _, msgLen := range []uint32{0, 1, 2, 3,
		50_000_000 /* exceeds the 1 MiB body limit */} {
		conn, frontend := dialAndStartup(t, address)

		header := make([]byte, 5)
		header[0] = 'Q'
		binary.BigEndian.PutUint32(header[1:], msgLen)
		if _, err := conn.Write(header); err != nil {
			t.Fatal(err)
		}
		_ = frontend // the raw header bypasses the frontend buffers

		// the server must close the broken connection
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1)
		if n, err := conn.Read(buf); n > 0 || err == nil {
			t.Fatalf("message with length %d: expected the connection to be closed", msgLen)
		}
	}

	// ... but keep serving well-formed connections
	conn, frontend := dialAndStartup(t, address)
	frontend.Send(&pgproto3.Query{String: "show version"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, ok := receive(t, conn, frontend).(*pgproto3.RowDescription); !ok {
		t.Fatal("expected RowDescription")
	}
	dataRow := receive(t, conn, frontend).(*pgproto3.DataRow)
	if len(dataRow.Values) != 1 || string(dataRow.Values[0]) != "test-1.0" {
		t.Fatalf("unexpected data row after malformed messages: %q", dataRow.Values)
	}
}
