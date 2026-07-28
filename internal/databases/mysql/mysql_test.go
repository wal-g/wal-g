package mysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMySQLDatasource(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		user     string
		password string
		network  string
		addr     string
		dbName   string
	}{
		{
			// form used by docker/mysql, docker/mariadb export_common.sh
			name:    "empty tcp addr defaults to 127.0.0.1:3306",
			dsn:     "sbtest:@/sbtest",
			user:    "sbtest",
			network: "tcp",
			addr:    "127.0.0.1:3306",
			dbName:  "sbtest",
		},
		{
			name:     "tcp with host and port",
			dsn:      "user:pass@tcp(localhost:3306)/mysql",
			user:     "user",
			password: "pass",
			network:  "tcp",
			addr:     "localhost:3306",
			dbName:   "mysql",
		},
		{
			name:     "tcp host without port gets :3306",
			dsn:      "user:pass@tcp(localhost)/mysql",
			user:     "user",
			password: "pass",
			network:  "tcp",
			addr:     "localhost:3306",
			dbName:   "mysql",
		},
		{
			name:    "unix socket",
			dsn:     "user@unix(/var/run/mysqld/mysqld.sock)/db",
			user:    "user",
			network: "unix",
			addr:    "/var/run/mysqld/mysqld.sock",
			dbName:  "db",
		},
		{
			name:    "empty unix addr defaults to /tmp/mysql.sock",
			dsn:     "user@unix()/db",
			user:    "user",
			network: "unix",
			addr:    "/tmp/mysql.sock",
			dbName:  "db",
		},
		{
			name:    "no credentials, empty addr",
			dsn:     "/db",
			network: "tcp",
			addr:    "127.0.0.1:3306",
			dbName:  "db",
		},
		{
			name:    "percent-encoded database name is unescaped",
			dsn:     "user@/foo%2Fbar",
			user:    "user",
			network: "tcp",
			addr:    "127.0.0.1:3306",
			dbName:  "foo/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := parseMySQLDatasource(tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.user, d.user)
			assert.Equal(t, tt.password, d.password)
			assert.Equal(t, tt.network, d.network)
			assert.Equal(t, tt.addr, d.addr)
			assert.Equal(t, tt.dbName, d.dbName)
		})
	}
}

func TestParseMySQLDatasource_Params(t *testing.T) {
	d, err := parseMySQLDatasource("user:pass@tcp(host:3306)/db?tls=skip-verify&timeout=30s")
	require.NoError(t, err)
	assert.Equal(t, "skip-verify", d.params.Get("tls"))
	assert.Equal(t, "30s", d.params.Get("timeout"))
}

func TestParseMySQLDatasource_Errors(t *testing.T) {
	// errors must not echo the DSN (commonly carries credentials)
	for _, dsn := range []string{"user:secret@tcp(host:3306)", "user:secret@tcp(host:3306/db"} {
		_, err := parseMySQLDatasource(dsn)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "secret")
	}
}

func TestTLSConfigName(t *testing.T) {
	cases := map[string]string{
		"1": "true", "true": "true", "TRUE": "true", "True": "true",
		"0": "false", "false": "false", "FALSE": "false", "False": "false",
		"skip-verify": "skip-verify",
		"preferred":   "preferred",
		"PREFERRED":   "preferred",
		"":            "",
		"custom":      "custom",
	}
	for value, want := range cases {
		assert.Equalf(t, want, tlsConfigName(value), "tls=%q", value)
	}
}

func TestResolveTLS(t *testing.T) {
	mk := func(tls string) mysqlDatasource {
		d, err := parseMySQLDatasource("u@tcp(host:3306)/db?tls=" + tls)
		require.NoError(t, err)
		return d
	}

	cfg, fallback, err := mk("true").resolveTLS("host", "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "host", cfg.ServerName)
	assert.False(t, cfg.InsecureSkipVerify)
	assert.False(t, fallback)

	cfg, fallback, err = mk("skip-verify").resolveTLS("host", "")
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.False(t, fallback)

	cfg, fallback, err = mk("preferred").resolveTLS("host", "")
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.True(t, fallback)

	cfg, _, err = mk("0").resolveTLS("host", "")
	require.NoError(t, err)
	assert.Nil(t, cfg)

	_, _, err = mk("bogus").resolveTLS("host", "")
	assert.Error(t, err)
}

func TestConnectParams(t *testing.T) {
	mk := func(query string) mysqlDatasource {
		d, err := parseMySQLDatasource("u@tcp(host:3306)/db?" + query)
		require.NoError(t, err)
		return d
	}

	opts, timeout, err := mk("timeout=30s&readTimeout=5s&writeTimeout=7s&collation=utf8mb4_general_ci&compress=true").connectParams()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, timeout)
	assert.Len(t, opts, 4) // readTimeout, writeTimeout, collation, compress (timeout is not an option)

	_, timeout, err = mk("readTimeout=5s").connectParams()
	require.NoError(t, err)
	assert.Zero(t, timeout) // go-sql-driver default: no dial timeout

	for _, bad := range []string{"timeout=xyz", "readTimeout=nope", "compress=bogus"} {
		_, _, err := mk(bad).connectParams()
		assert.Errorf(t, err, "expected error for %q", bad)
	}
}

func TestConnectParams_Apply(t *testing.T) {
	apply := func(query string) *client.Conn {
		d, err := parseMySQLDatasource("u@tcp(host:3306)/db?" + query)
		require.NoError(t, err)
		opts, _, err := d.connectParams()
		require.NoError(t, err)
		c := &client.Conn{}
		for _, opt := range opts {
			require.NoError(t, opt(c))
		}
		return c
	}

	assert.Equal(t, 5*time.Second, apply("readTimeout=5s").ReadTimeout)
	assert.Equal(t, 7*time.Second, apply("writeTimeout=7s").WriteTimeout)
	assert.Equal(t, "utf8mb4_unicode_ci", apply("collation=utf8mb4_unicode_ci").GetCollation())
	assert.True(t, apply("compress=true").HasCapability(gomysql.CLIENT_COMPRESS))
	assert.True(t, apply("compress=zlib").HasCapability(gomysql.CLIENT_COMPRESS))
	assert.True(t, apply("compress=zstd").HasCapability(gomysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM))
	assert.False(t, apply("compress=false").HasCapability(gomysql.CLIENT_COMPRESS))
}
