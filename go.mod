module github.com/wal-g/wal-g

go 1.18

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.4.1
	github.com/Azure/go-autorest/autorest v0.11.21
	github.com/DataDog/zstd v1.4.4
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/aws/aws-sdk-go v1.44.7
	github.com/blang/semver v3.5.1+incompatible
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cucumber/godog v0.12.5
	github.com/cyberdelia/lzo v0.0.0-20171006181345-d85071271a6f
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/docker/docker v1.13.1
	github.com/go-mysql-org/go-mysql v1.4.1-0.20220126055159-3566d1e608ea
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gofrs/flock v0.8.0
	github.com/golang/mock v1.4.4
	github.com/google/uuid v1.2.0
	github.com/greenplum-db/gp-common-go-libs v1.0.4
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jackc/pgconn v1.6.5-0.20200823013804-5db484908cf7
	github.com/jackc/pglogrepl v0.0.0-20210109153808-a78a685a0bff
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx v3.6.0+incompatible
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/magiconair/properties v1.8.1
	github.com/minio/sio v0.2.0
	github.com/mongodb/mongo-tools-common v2.0.1+incompatible
	github.com/ncw/swift v1.0.49
	github.com/pierrec/lz4/v4 v4.1.11
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.11.0
	github.com/spf13/cobra v1.4.0
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.7.1
	github.com/ulikunitz/xz v0.5.8
	github.com/wal-g/json v0.3.1
	github.com/wal-g/tracelog v0.0.0-20190824100002-0ab2b054ff30
	github.com/yandex-cloud/go-genproto v0.0.0-20201102102956-0c505728b6f0
	github.com/yandex-cloud/go-sdk v0.0.0-20201109103511-a86298d3fea5
	go.mongodb.org/mongo-driver v1.9.1
	golang.org/x/crypto v0.0.0-20220511200225-c6db032c6c88
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/api v0.30.0
	gopkg.in/ini.v1 v1.51.0
)

require (
	github.com/cactus/go-statsd-client/v5 v5.0.0
	github.com/google/brotli/go/cbrotli v0.0.0-20220110100810-f4153a09f87c
	github.com/pkg/profile v1.6.0
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
)

require (
	cloud.google.com/go v0.65.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.14 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.5.1 // indirect
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cucumber/gherkin-go/v19 v19.0.3 // indirect
	github.com/cucumber/messages-go/v16 v16.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20190901134440-81cf024a9e0a // indirect
	github.com/go-openapi/errors v0.19.3 // indirect
	github.com/go-openapi/strfmt v0.19.4 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.2-0.20190904063534-ff6b7dc882cf // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-memdb v1.3.0 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jmoiron/sqlx v1.3.3 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.8 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3 // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20200227202807-02e2044944cc // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.0 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2 // indirect
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/mod v0.3.0 // indirect
	golang.org/x/net v0.0.0-20220425223048-2871e0cb64e4 // indirect
	golang.org/x/oauth2 v0.0.0-20210514164344-f6687ab2804c // indirect
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.0.0-20201125231158-b5590deeca9b // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/genproto v0.0.0-20200825200019-8632dd797987 // indirect
	google.golang.org/grpc v1.31.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
