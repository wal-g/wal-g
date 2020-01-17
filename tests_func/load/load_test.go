package mongoload

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func prepareTestLoad(configFile, patronFile, mongoUri string, ctx context.Context) (*mongo.Client, *os.File, error) {
	err := generatePatronsFromFile(configFile)
	if err != nil {
		return nil, nil, err
	}
	cli, err := mongo.NewClient(options.Client().ApplyURI(mongoUri))
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create client to mongodb: %v", err)
	}
	err = cli.Connect(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot connect to mongodb: %v", err)
	}
	err = cli.Ping(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot ping mongodb basse: %v", err)
	}
	f, err := os.Open(patronFile)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read patron: %v", err)
	}
	return cli, f, nil

}

func TestLoadRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	cli, f, err := prepareTestLoad("mongo_load_config.json",
		"mongo_load_patron_1.json",
		"mongodb://localhost:27017", ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var errcList []<-chan error
	roc, errc0, err := ReadRawMongoOps(ctx, f, 3)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	errcList = append(errcList, errc0)
	cmdc, errc1 := MakeMongoOps(ctx, cli, roc)
	errcList = append(errcList, errc1)
	rsc := RunMongoOpFuncs(ctx, cmdc, 3, 3)
	PrintMongoOpRes(ctx, rsc)
	err = WaitForPipeline(errcList...)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1 * time.Second)
	err = os.Remove("mongo_load_patron_1.json")
	if err != nil {
		t.Error(err)
		return
	}
}
