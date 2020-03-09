package helpers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/tests_func/utils"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	LocalDB   = "local"
	OplogColl = "oplog.rs"
	AdminDB   = "admin"
)

type DatabaseRecord struct {
	Timestamp int64
	IntNum    int
	Str       string
}

func generateRecord(rowNum int, strLen int, strPrefix string) DatabaseRecord {
	return DatabaseRecord{
		Timestamp: time.Now().Unix(),
		IntNum:    rowNum,
		Str:       fmt.Sprintf("%s_%s", strPrefix, utils.RandSeq(strLen)),
	}
}

type NsSnapshot struct {
	NS      string
	Docs    []bson.M
	Indexes []bson.M
}

func isSystemCollection(collectionName string) bool {
	return strings.HasPrefix(collectionName, "system.")
}

var (
	SystemDatabases = []string{"local", "config", "admin"}
)

func isSystemDatabase(db string) bool {
	for _, sysdb := range SystemDatabases {
		if db == sysdb {
			return true
		}
	}
	return false
}

type CmdResponse struct {
	Ok       int    `bson:"ok"`
	ErrMsg   string `bson:"errmsg, omitempty"`
	CodeName string `bson:"codeName, omitempty"`
}

// Optime ...
type OpTime struct {
	TS   primitive.Timestamp `bson:"ts" json:"ts"`
	Term int64               `bson:"t" json:"t"`
}

// IsMasterLastWrite ...
type IsMasterLastWrite struct {
	OpTime         OpTime `bson:"opTime"`
	MajorityOpTime OpTime `bson:"majorityOpTime"`
}

// IsMaster ...
type IsMaster struct {
	IsMaster  bool              `bson:"ismaster"`
	LastWrite IsMasterLastWrite `bson:"lastWrite"`
	SetName   string            `bson:"setName"`
}

// AuthCreds ...
type AuthCreds struct {
	Username string
	Password string
	Database string
}

func AdminCredsFromEnv(env map[string]string) AuthCreds {
	return AuthCreds{
		Username: env["MONGO_ADMIN_USERNAME"],
		Password: env["MONGO_ADMIN_PASSWORD"],
		Database: env["MONGO_ADMIN_DB_NAME"],
	}
}

type MongoCtl struct {
	ctx        context.Context
	host       string
	port       int
	expHost    string
	expPort    int
	adminCreds AuthCreds
	adminConn  *mongo.Client
}

type MongoCtlOpt func(*MongoCtl)

func AdminCreds(creds AuthCreds) MongoCtlOpt {
	return func(mc *MongoCtl) {
		mc.adminCreds = creds
	}
}

func Port(port int) MongoCtlOpt {
	return func(mc *MongoCtl) {
		mc.port = port
	}
}

func NewMongoCtl(ctx context.Context, host string, setters ...MongoCtlOpt) (*MongoCtl, error) {
	mc := &MongoCtl{
		ctx:  ctx,
		host: host,
		port: 27018,
	}
	for _, setter := range setters {
		setter(mc)
	}
	expHost, expPort, err := ExposedHostPort(ctx, mc.host, mc.port)
	if err != nil {
		return nil, err
	}
	mc.expHost = expHost
	mc.expPort = expPort

	return mc, nil
}

func (mc *MongoCtl) Connect(creds *AuthCreds) (*mongo.Client, error) {
	return mc.connect(creds)
}

func (mc *MongoCtl) AdminConnect() (*mongo.Client, error) {
	if mc.adminConn == nil {
		conn, err := mc.connect(&mc.adminCreds)
		if err != nil {
			return nil, err
		}
		mc.adminConn = conn
	}

	return mc.adminConn, nil
}

func (mc *MongoCtl) connect(creds *AuthCreds) (*mongo.Client, error) {
	auth := ""
	dbase := AdminDB
	if creds != nil {
		auth = fmt.Sprintf("%s:%s@", creds.Username, creds.Password)
		dbase = creds.Database
	}
	uri := fmt.Sprintf("mongodb://%s%s:%d/%s?connect=direct&w=majority&socketTimeoutMS=3000&connectTimeoutMS=3000", auth, mc.expHost, mc.expPort, dbase)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("can not create mongo client: %v", err)
	}
	err = client.Connect(mc.ctx)
	if err != nil {
		return nil, fmt.Errorf("can not connect via mongo client: %v", err)
	}
	return client, nil
}

func (mc *MongoCtl) WriteTestData(mark string) error {
	conn, err := mc.AdminConnect()
	if err != nil {
		return err
	}
	docsCount := 3
	for _, dbName := range []string{"test_db_01", "test_db_02"} {
		for _, tableName := range []string{"test_table_01", "test_table_02"} {
			var rows []interface{}
			for k := 1; k <= docsCount; k++ {
				rows = append(rows, generateRecord(k, 5, mark))
			}
			if _, err := conn.Database(dbName).Collection(tableName).InsertMany(mc.ctx, rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *MongoCtl) Snapshot() ([]NsSnapshot, error) {
	var snapshot []NsSnapshot

	conn, err := mc.AdminConnect()
	if err != nil {
		return nil, err
	}

	databases, err := conn.ListDatabaseNames(mc.ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("can not list databases: %v", err)
	}
	sort.Strings(databases)
	for _, database := range databases {
		if isSystemDatabase(database) {
			continue
		}

		collsInfo, err := ListCollections(mc.ctx, conn, database)
		if err != nil {
			return nil, err
		}

		for _, collInfo := range collsInfo {
			coll := collInfo["name"].(string)
			if isSystemCollection(coll) {
				continue
			}
			docs, err := FetchNsDocs(mc.ctx, conn, database, coll)
			if err != nil {
				return nil, err
			}

			indexes, err := ListNsIndexes(mc.ctx, conn, database, coll)
			if err != nil {
				return nil, err
			}

			snapshot = append(snapshot, NsSnapshot{
				NS:      fmt.Sprintf("%s.%s", database, coll),
				Docs:    docs,
				Indexes: indexes,
			})
		}
	}

	return snapshot, nil
}

func ListCollections(ctx context.Context, conn *mongo.Client, database string) ([]bson.M, error) {
	// TODO: filter system.*
	cur, err := conn.Database(database, nil).ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("can not create listCollections cursor: %v", err)
	}
	colls, err := FetchAllDocs(ctx, cur)
	if err != nil {
		return nil, err
	}
	sort.Slice(colls, func(i, j int) bool {
		return colls[i]["name"].(string) < colls[j]["name"].(string)
	})
	return colls, nil
}

func FetchNsDocs(ctx context.Context, conn *mongo.Client, database, table string) ([]bson.M, error) {
	ns := fmt.Sprintf("%s.%s", database, table)

	cur, err := conn.
		Database(database, nil).
		Collection(table).
		Find(ctx, bson.M{}, options.Find().SetSort(map[string]int{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("can not create cursor to dump docs from ns '%s': %v", ns, err)
	}

	nsData, err := FetchAllDocs(ctx, cur)
	if err != nil {
		return nil, fmt.Errorf("can not fetch docs from ns '%s': %v", ns, err)
	}

	if err := cur.Close(ctx); err != nil {
		return nil, fmt.Errorf("can not close cursor for ns '%s': %v", ns, err)
	}

	return nsData, nil
}

func ListNsIndexes(ctx context.Context, conn *mongo.Client, database, table string) ([]bson.M, error) {
	ns := fmt.Sprintf("%s.%s", database, table)

	indexes := conn.Database(database, nil).Collection(table).Indexes()

	cur, err := indexes.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("can not create cursor to list indexes on ns '%s': %v", ns, err)
	}

	nsData, err := FetchAllDocs(ctx, cur)
	if err != nil {
		return nil, fmt.Errorf("can not list indexes on ns '%s': %v", ns, err)
	}

	if err := cur.Close(ctx); err != nil {
		return nil, fmt.Errorf("can not close cursor for ns '%s': %v", ns, err)
	}

	return nsData, nil
}

func FetchAllDocs(ctx context.Context, cur *mongo.Cursor) ([]bson.M, error) {
	var results []bson.M
	err := cur.All(ctx, &results)
	return results, err
}

func (mc *MongoCtl) runIsMaster() (IsMaster, error) {
	conn, err := mc.Connect(nil)
	if err != nil {
		return IsMaster{}, err
	}
	im := IsMaster{}
	err = conn.Database(AdminDB).RunCommand(mc.ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&im)

	return im, err
}

func (mc *MongoCtl) IsMaster() (bool, error) {
	im, err := mc.runIsMaster()
	if err != nil {
		return false, err
	}

	return im.IsMaster, err
}

func (mc *MongoCtl) LastMajTS() (OpTimestamp, error) {
	im, err := mc.runIsMaster()
	if err != nil {
		return OpTimestamp{}, err
	}
	ts := im.LastWrite.MajorityOpTime.TS

	return OpTimestamp{TS: ts.T, Inc: ts.I}, nil
}

func (mc *MongoCtl) LastTS() (OpTimestamp, error) {
	conn, err := mc.AdminConnect()
	if err != nil {
		return OpTimestamp{}, err
	}
	var op db.Oplog
	opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	err = conn.Database(LocalDB).Collection(OplogColl).
		FindOne(mc.ctx, bson.D{}, opts).Decode(&op)

	if err != nil {
		return OpTimestamp{}, err
	}
	ts := op.Timestamp

	return OpTimestamp{TS: ts.T, Inc: ts.I}, nil
}

func (mc *MongoCtl) InitReplSet() error {
	im, err := mc.runIsMaster()
	if err != nil {
		return err
	}
	if im.SetName != "" {
		return nil
	}
	_, err = mc.runCmd([]string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval", "rs.initiate()"})
	time.Sleep(3 * time.Second) // TODO: wait until rs initiated

	return err
}

func (mc *MongoCtl) EnableAuth() error {
	cmd := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018",
		"--eval", fmt.Sprintf("db.createUser({user: '%s', pwd: '%s', roles: ['root']})",
			mc.adminCreds.Username,
			mc.adminCreds.Password,
		), AdminDB}
	response, err := RunCommand(mc.ctx, mc.host, cmd)
	if err != nil {
		return err
	}

	if strings.Contains(response.Combined(), "command createUser requires authentication") ||
		strings.Contains(response.Combined(), "couldn't add user: not authorized on admin to execute command") ||
		strings.Contains(response.Combined(), "there are no users authenticated") {
		return nil
	}
	if !strings.Contains(response.Combined(), "Successfully added user") {
		tracelog.ErrorLogger.Printf("can not create admin user: %s", response.Combined())
		return fmt.Errorf("can not initialize auth")
	}

	conn, err := mc.AdminConnect()
	if err != nil {
		return err
	}

	err = conn.Database(AdminDB).RunCommand(mc.ctx,
		bson.D{
			{Key: "createRole", Value: "anything"},
			{Key: "privileges", Value: bson.A{
				bson.M{
					"resource": bson.M{"anyResource": true},
					"actions":  bson.A{"anyAction"},
				}}},
			{Key: "roles", Value: bson.A{}},
		}).Err()

	if err != nil {
		return err
	}

	err = conn.Database(AdminDB).RunCommand(mc.ctx,
		bson.D{
			{Key: "grantRolesToUser", Value: mc.adminCreds.Username},
			{Key: "roles", Value: bson.A{"anything"}},
		}).Err()
	if err != nil {
		return err
	}

	return nil
}

func (mc *MongoCtl) runCmd(run []string) (ExecResult, error) {
	exc, err := RunCommandStrict(mc.ctx, mc.host, run)
	cmdLine := strings.Join(run, " ")

	if err != nil {
		tracelog.ErrorLogger.Printf("Command failed '%s' failed: %v", cmdLine, exc.String())
		return exc, err
	}
	return exc, err
}

func (mc *MongoCtl) PurgeDatadir() error {
	_, err := mc.runCmd([]string{"supervisorctl", "stop", "mongodb"})
	if err != nil {
		return err
	}

	_, err = mc.runCmd([]string{"rm", "-rf", "/var/lib/mongodb/*"})
	if err != nil {
		return err
	}

	_, err = mc.runCmd([]string{"supervisorctl", "start", "mongodb"})
	if err != nil {
		return err
	}

	return nil
}
