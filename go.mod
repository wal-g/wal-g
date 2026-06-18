module github.com/wal-g/wal-g

go 1.25.8

toolchain go1.25.9

require (
	cloud.google.com/go/storage v1.62.3
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.22.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.14.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.0
	github.com/aws/aws-sdk-go v1.55.7
	github.com/cenkalti/backoff/v5 v5.0.3
	github.com/cucumber/godog v0.15.1
	github.com/cyberdelia/lzo v1.0.0
	github.com/go-mysql-org/go-mysql v1.14.1-0.20260227075927-498f8104b8ff
	github.com/go-sql-driver/mysql v1.10.0
	github.com/gofrs/flock v0.13.0
	github.com/google/uuid v1.6.0
	github.com/greenplum-db/gp-common-go-libs v1.0.22
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/jackc/pglogrepl v0.0.0-20260401131349-e37c41485510
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/microsoft/go-mssqldb v1.10.0
	github.com/minio/sio v0.2.0
	github.com/moby/moby/api v1.54.2
	github.com/moby/moby/client v0.4.1
	github.com/pierrec/lz4/v4 v4.1.27
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.1
	github.com/spf13/cobra v1.7.0
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.7
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.11.1
	github.com/ulikunitz/xz v0.5.15
	github.com/wal-g/tracelog v0.1.1
	github.com/yandex-cloud/go-genproto v0.0.0-20230918115514-93a99045c9de
	github.com/yandex-cloud/go-sdk v0.0.0-20230918120620-9e95f0816d79
	go.mongodb.org/mongo-driver v1.17.1
	golang.org/x/crypto v0.53.0
	golang.org/x/sync v0.21.0
	golang.org/x/time v0.15.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
	google.golang.org/api v0.284.0
	gopkg.in/ini.v1 v1.67.0
)

require (
	github.com/ProtonMail/go-crypto v1.3.0
	github.com/RoaringBitmap/roaring/v2 v2.18.0
	github.com/aliyun/alibabacloud-oss-go-sdk-v2 v1.2.3
	github.com/aliyun/credentials-go v1.4.5
	github.com/cactus/go-statsd-client/v5 v5.0.0
	github.com/google/brotli/go/cbrotli v1.1.0
	github.com/klauspost/compress v1.18.5
	github.com/mongodb/mongo-tools v0.0.0-20240724183527-6d4f001be3fc
	github.com/ncw/directio v1.0.5
	github.com/ncw/swift/v2 v2.0.5
	github.com/pkg/profile v1.7.0
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.6.2
	github.com/redis/go-redis/v9 v9.20.1
	go.uber.org/mock v0.6.0
	golang.org/x/mod v0.36.0
	golang.org/x/net v0.56.0
	golang.org/x/sys v0.46.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.7.0 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.31.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.55.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.55.0 // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.2 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/cucumber/gherkin/go/v26 v26.2.0 // indirect
	github.com/cucumber/messages/go/v21 v21.0.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/fgprof v0.9.3 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/pprof v0.0.0-20211214055906-6f57359322fd // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.16 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/pgx/v4 v4.18.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pingcap/log v1.1.1-0.20241212030209-7e3ff8601a2a // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260219190905-9b9281fa8d6d // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.42.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

require (
	cloud.google.com/go v0.123.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.7.2 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudflare/circl v1.6.3 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/docker/go-connections v0.7.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-openapi/errors v0.19.3 // indirect
	github.com/go-openapi/strfmt v0.19.4 // indirect
	github.com/gofrs/uuid v4.3.1+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/googleapis/gax-go/v2 v2.22.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.4 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-version v1.9.0
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/term v0.44.0 // indirect
	golang.org/x/text v0.38.0
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/grpc v1.81.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
