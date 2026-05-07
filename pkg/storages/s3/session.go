package s3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/wal-g/tracelog"
	"gopkg.in/yaml.v3"
)

// loadAWSConfig builds the v2 aws.Config from wal-g's S3 Config and returns
// the per-S3-client functional options that customize endpoint, path-style etc.
// In v1 these were knobs on session.Config; in v2 they live on s3.Options and
// are applied per service client at construction time.
func loadAWSConfig(cfg *Config) (aws.Config, []func(*s3.Options), error) {
	httpClient, err := buildHTTPClient(cfg)
	if err != nil {
		return aws.Config{}, nil, err
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithHTTPClient(httpClient),
	}

	if cfg.DualStack {
		loadOpts = append(loadOpts, config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled))
	}

	if cfg.LogLevel == "DEVEL" {
		loadOpts = append(loadOpts, config.WithClientLogMode(aws.LogRequest|aws.LogResponse|aws.LogRetries))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return aws.Config{}, nil, fmt.Errorf("load default AWS config: %w", err)
	}

	if err := configureCredentials(&awsCfg, cfg); err != nil {
		return aws.Config{}, nil, err
	}

	region, err := resolveRegion(&awsCfg, cfg)
	if err != nil {
		return aws.Config{}, nil, err
	}
	awsCfg.Region = region

	awsCfg.Retryer = newRetryerFunc(cfg)

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = cfg.ForcePathStyle
		},
	}
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	if cfg.EndpointSource != "" {
		s3Opts = append(s3Opts, withDynamicEndpoint(cfg.EndpointSource, cfg.EndpointPort, cfg.Endpoint))
	}

	if cfg.RequestAdditionalHeaders != "" {
		headers, err := decodeHeaders(cfg.RequestAdditionalHeaders)
		if err != nil {
			return aws.Config{}, nil, fmt.Errorf("decode additional headers for S3 requests: %w", err)
		}
		s3Opts = append(s3Opts, withAdditionalHeaders(headers))
	}

	if cfg.Disable100Continue {
		s3Opts = append(s3Opts, withDisable100Continue())
	}

	if cfg.UseYCSessionToken != "" {
		useYC, err := strconv.ParseBool(cfg.UseYCSessionToken)
		if err != nil {
			return aws.Config{}, nil, fmt.Errorf("invalid YC session token: %w", err)
		}
		if useYC {
			// Yandex Cloud mimics the EC2 metadata service. Override default credentials
			// with the IMDS provider, then copy X-Amz-Security-Token to X-YaCloud-SubjectToken
			// after AWS signing has stamped the former onto the request.
			awsCfg.Credentials = aws.NewCredentialsCache(ec2rolecreds.New())
			s3Opts = append(s3Opts, withYCSubjectToken())
		}
	}

	return awsCfg, s3Opts, nil
}

func buildHTTPClient(cfg *Config) (aws.HTTPClient, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	if cfg.CACertFile != "" {
		certs, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read S3 CA cert file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(certs) {
			return nil, fmt.Errorf("no PEM certs found in %q", cfg.CACertFile)
		}
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		transport.TLSClientConfig.RootCAs = pool
	}

	return &http.Client{Transport: NewRoundTripperWithLogging(transport)}, nil
}

func configureCredentials(awsCfg *aws.Config, cfg *Config) error {
	accessKey := cfg.AccessKey
	secretKey := cfg.Secrets.SecretKey
	sessionToken := cfg.SessionToken

	if cfg.RoleARN != "" {
		if os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" && os.Getenv("AWS_ROLE_ARN") != "" {
			tracelog.InfoLogger.Printf("Running with IRSA, skipping explicit role assumption")
		} else {
			stsClient := sts.NewFromConfig(*awsCfg)
			out, err := stsClient.AssumeRole(context.Background(), &sts.AssumeRoleInput{
				RoleArn:         aws.String(cfg.RoleARN),
				RoleSessionName: aws.String(cfg.SessionName),
			})
			if err != nil {
				return fmt.Errorf("assume role by ARN: %w", err)
			}
			accessKey = aws.ToString(out.Credentials.AccessKeyId)
			secretKey = aws.ToString(out.Credentials.SecretAccessKey)
			sessionToken = aws.ToString(out.Credentials.SessionToken)
		}
	}

	if accessKey != "" && secretKey != "" {
		awsCfg.Credentials = aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		)
	}
	return nil
}

func resolveRegion(awsCfg *aws.Config, cfg *Config) (string, error) {
	if cfg.Region != "" {
		return cfg.Region, nil
	}
	if cfg.Endpoint == "" || strings.HasSuffix(cfg.Endpoint, ".amazonaws.com") {
		region, err := detectAWSRegionByBucket(awsCfg, cfg.Bucket, cfg.Endpoint)
		if err != nil {
			return "", fmt.Errorf("AWS region isn't configured explicitly: detect region: %w", err)
		}
		return region, nil
	}
	// S3-compatible services (Minio, Ceph, etc.) accept us-east-1 as a stand-in.
	// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
	return "us-east-1", nil
}

func detectAWSRegionByBucket(awsCfg *aws.Config, bucket, endpoint string) (string, error) {
	probe := *awsCfg
	probe.Region = "us-east-1"
	client := s3.NewFromConfig(probe, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	out, err := client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}
	if out.LocationConstraint == "" {
		// "US Standard" buckets (us-east-1) return an empty constraint.
		return "us-east-1", nil
	}
	return string(out.LocationConstraint), nil
}

// withDynamicEndpoint rewrites the request URL host using a value fetched from
// EndpointSource on each request. It runs as a Build middleware so the rewrite
// happens before signing, matching v1's Validate handler timing.
func withDynamicEndpoint(endpointSource, port, staticEndpoint string) func(*s3.Options) {
	mw := &dynamicEndpointMiddleware{
		source:         endpointSource,
		port:           port,
		staticEndpoint: staticEndpoint,
	}
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Build.Add(mw, middleware.After)
		})
	}
}

type dynamicEndpointMiddleware struct {
	source         string
	port           string
	staticEndpoint string
}

func (*dynamicEndpointMiddleware) ID() string { return "walgDynamicEndpoint" }

func (m *dynamicEndpointMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleBuild(ctx, in)
	}
	endpoint := requestEndpointFromSource(m.source, m.port)
	if endpoint != nil {
		tracelog.DebugLogger.Printf("using S3 endpoint %s", *endpoint)
		host := strings.TrimPrefix(m.staticEndpoint, "https://")
		req.Host = host
		req.URL.Host = *endpoint
		req.URL.Scheme = "http"
	} else {
		tracelog.DebugLogger.Printf("using S3 endpoint %s", m.staticEndpoint)
	}
	return next.HandleBuild(ctx, in)
}

func withAdditionalHeaders(headers map[string]string) func(*s3.Options) {
	mw := &additionalHeadersMiddleware{headers: headers}
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Build.Add(mw, middleware.After)
		})
	}
}

type additionalHeadersMiddleware struct {
	headers map[string]string
}

func (*additionalHeadersMiddleware) ID() string { return "walgAdditionalHeaders" }

func (m *additionalHeadersMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleBuild(ctx, in)
	}
	for k, v := range m.headers {
		req.Header.Add(k, v)
	}
	return next.HandleBuild(ctx, in)
}

func withDisable100Continue() func(*s3.Options) {
	mw := disable100ContinueMiddleware{}
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Build.Add(mw, middleware.After)
		})
	}
}

type disable100ContinueMiddleware struct{}

func (disable100ContinueMiddleware) ID() string { return "walgDisable100Continue" }

func (disable100ContinueMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		req.Header.Del("Expect")
	}
	return next.HandleBuild(ctx, in)
}

// withYCSubjectToken copies the X-Amz-Security-Token header (set by the AWS
// SigV4 signer) to X-YaCloud-SubjectToken. Yandex Cloud's S3-compatible API
// reads the YaCloud header instead of the AWS one. Must run AFTER the Sign
// middleware in the Finalize step.
func withYCSubjectToken() func(*s3.Options) {
	mw := ycSubjectTokenMiddleware{}
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Finalize.Add(mw, middleware.After)
		})
	}
}

type ycSubjectTokenMiddleware struct{}

func (ycSubjectTokenMiddleware) ID() string { return "walgYCSubjectToken" }

func (ycSubjectTokenMiddleware) HandleFinalize(
	ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
) (middleware.FinalizeOutput, middleware.Metadata, error) {
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		if token := req.Header.Get("X-Amz-Security-Token"); token != "" {
			req.Header.Set("X-YaCloud-SubjectToken", token)
		}
	}
	return next.HandleFinalize(ctx, in)
}

func requestEndpointFromSource(endpointSource, port string) *string {
	t := http.DefaultTransport
	c := http.DefaultClient
	if tr, ok := t.(*http.Transport); ok {
		tr.DisableKeepAlives = true
		c = &http.Client{Transport: tr}
	}
	resp, err := c.Get(endpointSource)
	if err != nil {
		tracelog.ErrorLogger.Printf("Endpoint source error: %v ", err)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		tracelog.ErrorLogger.Printf("Endpoint source bad status code: %v ", resp.StatusCode)
		return nil
	}
	bytes, err := io.ReadAll(resp.Body)
	if err == nil {
		return aws.String(net.JoinHostPort(string(bytes), port))
	}
	tracelog.ErrorLogger.Println("Endpoint source reading error:", err)
	return nil
}

func decodeHeaders(encodedHeaders string) (map[string]string, error) {
	var data interface{}
	err := yaml.Unmarshal([]byte(encodedHeaders), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML headers: %w", err)
	}

	interfaces, ok := data.(map[string]interface{})
	if !ok {
		headerList, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("headers expected to be a list in YAML: %w", err)
		}
		interfaces = reformHeaderListToMap(headerList)
	}

	headers := map[string]string{}

	for k, v := range interfaces {
		headers[k] = v.(string)
	}

	return headers, nil
}

func reformHeaderListToMap(headerList []interface{}) map[string]interface{} {
	headers := map[string]interface{}{}
	for _, header := range headerList {
		ma := header.(map[string]interface{})
		for k, v := range ma {
			headers[k] = v
		}
	}
	return headers
}
