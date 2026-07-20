package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const BinlogPath = "binlog_" + utility.VersionStr + "/"

const TimeMysqlFormat = "2006-01-02 15:04:05"

type BackupTool string

const (
	WalgUnspecifiedStreamBackupTool BackupTool = "WALG_UNSPECIFIED_STREAM_BACKUP_TOOL"
	WalgXtrabackupTool              BackupTool = "WALG_XTRABACKUP_TOOL"
)

func fetchMySQLVariable(conn *client.Conn, variable string) (string, error) {
	r, err := conn.Execute("SELECT @@" + variable)
	if err != nil {
		return "", err
	}
	defer r.Close()
	return r.GetString(0, 0)
}

func getMySQLVersion(conn *client.Conn) (string, error) {
	// e.g. '8.0.35-27'
	return fetchMySQLVariable(conn, "version")
}

//nolint:unused
func getMySQLArchitecture(conn *client.Conn) (string, error) {
	// e.g 'x86_64' / 'aarch64' / 'arm64'
	return fetchMySQLVariable(conn, "version_compile_machine")
}

//nolint:unused
func getMySQLOS(conn *client.Conn) (string, error) {
	// e.g. 'Linux' / 'macos14.2'
	return fetchMySQLVariable(conn, "version_compile_os")
}

func getMySQLFlavor(conn *client.Conn) (string, error) {
	version, err := getMySQLVersion(conn)
	if err != nil {
		return "", err
	}
	// example: '10.6.4-MariaDB-1:10.6.4+maria~focal'
	if strings.Contains(version, "MariaDB") {
		return gomysql.MariaDBFlavor, nil
	}
	// It is possible to distinguish Percona & MySQL by checking 'version_comment',
	// however usually we can expect that there is no difference between these distributions
	return gomysql.MySQLFlavor, nil
}

func getMySQLGTIDExecuted(conn *client.Conn, flavor string) (gomysql.GTIDSet, error) {
	query := ""
	switch flavor {
	case gomysql.MySQLFlavor:
		query = "SELECT @@global.gtid_executed"
	case gomysql.MariaDBFlavor:
		query = "SELECT @@global.gtid_current_pos"
	default:
		return nil, fmt.Errorf("unknown MySQL flavor: %s", flavor)
	}

	r, err := conn.Execute(query)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	gtidStr, err := r.GetString(0, 0)
	if err != nil {
		return nil, err
	}

	return gomysql.ParseGTIDSet(flavor, gtidStr)
}

func getServerUUID(conn *client.Conn, flavor string) (string, error) {
	query := ""
	switch flavor {
	case gomysql.MySQLFlavor:
		query = "SELECT @@server_uuid"
	case gomysql.MariaDBFlavor:
		// MariaDB doesn't support `server_uuid`
		return "", nil
	default:
		return "", fmt.Errorf("unknown MySQL flavor: %s", flavor)
	}

	r, err := conn.Execute(query)
	if err != nil {
		return "", err
	}
	defer r.Close()
	return r.GetString(0, 0)
}

func getLastUploadedBinlog(ctx context.Context, folder storage.Folder) (string, error) {
	logFiles, _, err := folder.GetSubFolder(BinlogPath).ListFolder(ctx)
	if err != nil {
		return "", err
	}
	slices.SortFunc(logFiles, func(a, b storage.Object) int {
		return a.GetLastModified().Compare(b.GetLastModified())
	})
	if len(logFiles) == 0 {
		return "", nil
	}
	name := logFiles[len(logFiles)-1].GetName()
	if ext := path.Ext(name); compression.FindDecompressor(ext) != nil {
		// remove archive extension (like .br)
		name = strings.TrimSuffix(name, ext)
	}
	return name, nil
}

func getLastUploadedBinlogBeforeGTID(ctx context.Context, folder storage.Folder, gtid gomysql.GTIDSet, flavor string) (string, error) {
	folder = folder.GetSubFolder(BinlogPath)
	logFiles, _, err := folder.ListFolder(ctx)
	if err != nil {
		return "", err
	}
	slices.SortFunc(logFiles, func(a, b storage.Object) int {
		return a.GetLastModified().Compare(b.GetLastModified())
	})
	if len(logFiles) == 0 {
		return "", nil
	}
	for i := len(logFiles) - 1; i > 0; i-- {
		prevGtid, err := GetBinlogPreviousGTIDsRemote(ctx, folder, logFiles[i].GetName(), flavor)
		if err != nil {
			return "", err
		}
		if gtid.Contain(prevGtid) {
			return utility.TrimFileExtension(logFiles[i].GetName()), nil
		}
	}
	tracelog.WarningLogger.Printf("failed to find uploaded binlog behind %s", gtid)
	return "", nil
}

// mysqlDatasource holds the fields parsed from a go-sql-driver style DSN.
type mysqlDatasource struct {
	user     string
	password string
	network  string // tcp, tcp4, tcp6 or unix
	addr     string // host:port for tcp, socket path for unix
	dbName   string
	params   url.Values
}

// parseMySQLDatasource parses a go-sql-driver style DSN, matching its address,
// database-name and parameter normalization:
//
//	[user[:password]@][net[(addr)]]/dbname[?param=value&...]
//
// https://github.com/go-sql-driver/mysql#dsn-data-source-name
// Errors avoid echoing the DSN since it commonly carries credentials.
func parseMySQLDatasource(dsn string) (mysqlDatasource, error) {
	var d mysqlDatasource

	slash := strings.LastIndex(dsn, "/")
	if slash < 0 {
		return d, errors.New("invalid mysql datasource: missing '/' before database name")
	}
	endpoint, dbAndParams := dsn[:slash], dsn[slash+1:]

	if at := strings.LastIndex(endpoint, "@"); at >= 0 {
		credentials := endpoint[:at]
		if colon := strings.IndexByte(credentials, ':'); colon >= 0 {
			d.user, d.password = credentials[:colon], credentials[colon+1:]
		} else {
			d.user = credentials
		}
		endpoint = endpoint[at+1:]
	}

	if open := strings.IndexByte(endpoint, '('); open >= 0 {
		if !strings.HasSuffix(endpoint, ")") {
			return d, errors.New("invalid mysql datasource: network address not terminated (missing ')')")
		}
		d.network = endpoint[:open]
		d.addr = endpoint[open+1 : len(endpoint)-1]
	} else {
		d.addr = endpoint
	}

	rawDBName := dbAndParams
	if q := strings.IndexByte(dbAndParams, '?'); q >= 0 {
		rawDBName = dbAndParams[:q]
		params, err := url.ParseQuery(dbAndParams[q+1:])
		if err != nil {
			return d, errors.New("invalid mysql datasource parameters")
		}
		d.params = params
	} else {
		d.params = url.Values{}
	}
	dbName, err := url.PathUnescape(rawDBName)
	if err != nil {
		return d, fmt.Errorf("invalid mysql dbname %q: %w", rawDBName, err)
	}
	d.dbName = dbName

	d.network, d.addr, err = defaultMySQLAddr(d.network, d.addr)
	return d, err
}

// defaultMySQLAddr applies go-sql-driver's network/address defaults: empty net
// becomes tcp, an empty addr resolves to 127.0.0.1:3306 (tcp) or /tmp/mysql.sock
// (unix), and a tcp addr without a port gets :3306. go-mysql fills in none of these.
func defaultMySQLAddr(network, addr string) (string, string, error) {
	if network == "" {
		network = "tcp"
	}
	if addr == "" {
		switch network {
		case "tcp":
			return network, "127.0.0.1:3306", nil
		case "unix":
			return network, "/tmp/mysql.sock", nil
		default:
			return network, "", fmt.Errorf("default addr for mysql network %q unknown", network)
		}
	}
	if network == "tcp" {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "3306")
		}
	}
	return network, addr, nil
}

// getMySQLConnection connects using WALG_MYSQL_DATASOURCE_NAME, falling back to
// localhost when the configured host is unreachable.
func getMySQLConnection(ctx context.Context) (*client.Conn, error) {
	datasourceName, err := conf.GetRequiredSetting(conf.MysqlDatasourceNameSetting)
	if err != nil {
		return nil, err
	}
	dsn, err := parseMySQLDatasource(datasourceName)
	if err != nil {
		return nil, err
	}

	caFile, hasCA := conf.GetSetting(conf.MysqlSslCaSetting)
	if hasCA && dsn.params.Has("tls") {
		return nil, fmt.Errorf("mysql datasource contains tls option, it can't be used with %v option",
			conf.MysqlSslCaSetting)
	}

	conn, err := connectMySQL(ctx, dsn, caFile)
	if err != nil {
		if fallback := replaceHost(dsn.addr, "localhost"); fallback != dsn.addr {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided host, trying localhost")
			dsn.addr = fallback
			conn, err = connectMySQL(ctx, dsn, caFile)
		}
	}
	return conn, err
}

// connectMySQL opens a go-mysql client connection. When caFile is set a custom
// CA is used; otherwise TLS follows the DSN 'tls' parameter.
func connectMySQL(ctx context.Context, dsn mysqlDatasource, caFile string) (*client.Conn, error) {
	host, _, err := net.SplitHostPort(dsn.addr)
	if err != nil {
		host = dsn.addr // unix socket or portless addr
	}

	tlsConfig, allowFallback, err := dsn.resolveTLS(host, caFile)
	if err != nil {
		return nil, err
	}

	conn, err := dsn.dial(ctx, tlsConfig)
	// tls=preferred connects plaintext when the server lacks TLS support;
	// go-mysql rejects a configured TLS against such a server (client/auth.go)
	if err != nil && allowFallback && strings.Contains(err.Error(), "does not support TLS") {
		conn, err = dsn.dial(ctx, nil)
	}
	return conn, err
}

func (d mysqlDatasource) dial(ctx context.Context, tlsConfig *tls.Config) (*client.Conn, error) {
	options, timeout, err := d.connectParams()
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		options = append(options, func(conn *client.Conn) error {
			conn.SetTLSConfig(tlsConfig)
			return nil
		})
	}
	dialer := net.Dialer{Timeout: timeout}
	return client.ConnectWithDialer(ctx, d.network, d.addr, d.user, d.password, d.dbName, dialer.DialContext, options...)
}

// connectParams maps the supported go-sql-driver DSN options to go-mysql client
// settings plus the dial timeout (go-sql-driver default 0 = no timeout). tls is
// handled by resolveTLS; database/sql-layer options (parseTime, loc, ...) are ignored.
func (d mysqlDatasource) connectParams() ([]client.Option, time.Duration, error) {
	var (
		options []client.Option
		timeout time.Duration
	)
	for key := range d.params {
		value := d.params.Get(key)
		switch key {
		case "tls": // handled by resolveTLS
		case "timeout":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return nil, 0, fmt.Errorf("invalid mysql timeout option: %w", err)
			}
			timeout = dur
		case "readTimeout":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return nil, 0, fmt.Errorf("invalid mysql readTimeout option: %w", err)
			}
			options = append(options, func(c *client.Conn) error { c.ReadTimeout = dur; return nil })
		case "writeTimeout":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return nil, 0, fmt.Errorf("invalid mysql writeTimeout option: %w", err)
			}
			options = append(options, func(c *client.Conn) error { c.WriteTimeout = dur; return nil })
		case "collation":
			collation := value
			options = append(options, func(c *client.Conn) error { return c.SetCollation(collation) })
		case "compress":
			option, err := compressOption(value)
			if err != nil {
				return nil, 0, err
			}
			if option != nil {
				options = append(options, option)
			}
		}
	}
	return options, timeout, nil
}

// compressOption maps the compress DSN value to a capability. It accepts
// go-mysql algorithm names (zlib, zstd) and go-sql-driver booleans, where a
// truthy value selects zlib. A nil option disables compression.
func compressOption(value string) (client.Option, error) {
	switch value {
	case "zlib":
		return capabilityOption(gomysql.CLIENT_COMPRESS), nil
	case "zstd":
		return capabilityOption(gomysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM), nil
	}
	on, ok := readBool(value)
	if !ok {
		return nil, fmt.Errorf("invalid mysql compress option %q (want zlib, zstd, true or false)", value)
	}
	if on {
		return capabilityOption(gomysql.CLIENT_COMPRESS), nil
	}
	return nil, nil
}

func capabilityOption(capability uint32) client.Option {
	return func(c *client.Conn) error {
		return c.SetCapability(capability)
	}
}

// resolveTLS mirrors go-sql-driver's 'tls' parameter semantics; a non-empty
// caFile (WALG_MYSQL_SSL_CA) takes precedence as a custom verified CA.
func (d mysqlDatasource) resolveTLS(host, caFile string) (config *tls.Config, allowFallback bool, err error) {
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, false, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, false, fmt.Errorf("failed to load certificate from %s", caFile)
		}
		return &tls.Config{RootCAs: pool, ServerName: host}, false, nil
	}

	switch mode := tlsConfigName(d.params.Get("tls")); mode {
	case "", "false":
		return nil, false, nil
	case "true":
		return &tls.Config{ServerName: host}, false, nil
	case "skip-verify":
		return &tls.Config{InsecureSkipVerify: true}, false, nil
	case "preferred":
		return &tls.Config{InsecureSkipVerify: true}, true, nil
	default:
		return nil, false, fmt.Errorf("unknown mysql tls config name %q", mode)
	}
}

// tlsConfigName maps a DSN 'tls' value to go-sql-driver's canonical form,
// accepting its boolean aliases (1/true/TRUE/True, 0/false/FALSE/False).
func tlsConfigName(value string) string {
	if b, ok := readBool(value); ok {
		if b {
			return "true"
		}
		return "false"
	}
	return strings.ToLower(value)
}

func readBool(input string) (value, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}
	return false, false
}

func replaceHost(addr, newHost string) string {
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return net.JoinHostPort(newHost, port)
	}
	return addr
}

type StreamSentinelDto struct {
	Tool        BackupTool `json:"Tool,omitempty"`
	BinLogStart string     `json:"BinLogStart,omitempty"`
	// BinLogEnd field is for debug purpose only.
	// As we can not guarantee that transactions in BinLogEnd file happened before or after backup
	BinLogEnd      string    `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time `json:"StartLocalTime,omitempty"`
	StopLocalTime  time.Time `json:"StopLocalTime,omitempty"`

	UncompressedSize int64  `json:"UncompressedSize,omitempty"`
	CompressedSize   int64  `json:"CompressedSize,omitempty"`
	Hostname         string `json:"Hostname,omitempty"`
	ServerUUID       string `json:"ServerUUID,omitempty"`
	ServerVersion    string `json:"ServerVersion,omitempty"` // e.g. '8.0.35-27'
	ServerArch       string `json:"ServerArch,omitempty"`    // e.g '386' / 'amd64' / 'arm64' / 'arm'
	ServerOS         string `json:"ServerOS,omitempty"`      // e.g. 'linux' / 'darwin' / 'windows'

	IsPermanent   bool `json:"IsPermanent"`
	IsIncremental bool `json:"IsIncremental"`

	UserData interface{} `json:"UserData,omitempty"`

	LSN               *LSN    `json:"LSN"`
	IncrementFromLSN  *LSN    `json:"DeltaLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`
	//todo: add other fields from internal.GenericMetadata
}

func (s *StreamSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

type binlogHandler interface {
	handleBinlog(binlogPath string) error
}

func fetchLogs(ctx context.Context,
	folder storage.Folder, dstDir string, startTS, endTS, endBinlogTS time.Time, handler binlogHandler) error {
	logFolder := folder.GetSubFolder(BinlogPath)
	includeStart := true
outer:
	for {
		logsToFetch, err := getLogsCoveringInterval(ctx, logFolder, startTS, includeStart, endBinlogTS)
		includeStart = false
		if err != nil {
			return err
		}
		for _, logFile := range logsToFetch {
			startTS = logFile.GetLastModified()
			binlogName := utility.TrimFileExtension(logFile.GetName())
			binlogPath := path.Join(dstDir, binlogName)
			tracelog.InfoLogger.Printf("downloading %s into %s", binlogName, binlogPath)
			if err = internal.DownloadFileTo(ctx, internal.NewFolderReader(logFolder), binlogName, binlogPath); err != nil {
				tracelog.ErrorLogger.Printf("failed to download %s: %v", binlogName, err)
				return err
			}
			timestamp, err := GetBinlogStartTimestamp(binlogPath, gomysql.MySQLFlavor)
			if err != nil {
				return err
			}
			err = handler.handleBinlog(binlogPath)
			if err != nil {
				return err
			}
			if timestamp.After(endTS) {
				break outer
			}
		}
		if len(logsToFetch) == 0 {
			break
		}
	}
	return nil
}

func getBinlogSinceTS(ctx context.Context, folder storage.Folder, backup internal.Backup) (time.Time, error) {
	startTS := utility.MaxTime // far future
	var streamSentinel StreamSentinelDto
	err := backup.FetchSentinel(ctx, &streamSentinel)
	if err != nil {
		return time.Time{}, err
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", streamSentinel.String())

	// case when backup was uploaded before first binlog
	sentinels, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder(ctx)
	if err != nil {
		return time.Time{}, err
	}
	for _, sentinel := range sentinels {
		if strings.HasPrefix(sentinel.GetName(), backup.Name) {
			tracelog.InfoLogger.Printf("Backup sentinel file: %s (%s)", sentinel.GetName(), sentinel.GetLastModified())
			if sentinel.GetLastModified().Before(startTS) {
				startTS = sentinel.GetLastModified()
			}
		}
	}
	// case when binlog was uploaded before backup
	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder(ctx)
	if err != nil {
		return time.Time{}, err
	}
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			tracelog.InfoLogger.Printf("Backup start binlog: %s (%s)", binlog.GetName(), binlog.GetLastModified())
			if binlog.GetLastModified().Before(startTS) {
				startTS = binlog.GetLastModified()
			}
		}
	}
	return startTS, nil
}

// getLogsCoveringInterval lists the operation logs that cover the interval
func getLogsCoveringInterval(ctx context.Context, folder storage.Folder,
	start time.Time, includeStart bool, endBinlogTS time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(logFiles, func(a, b storage.Object) int {
		return a.GetLastModified().Compare(b.GetLastModified())
	})
	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		if logFile.GetLastModified().After(endBinlogTS) {
			continue // don't fetch binlogs from future
		}
		if start.Before(logFile.GetLastModified()) || includeStart && start.Equal(logFile.GetLastModified()) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}
