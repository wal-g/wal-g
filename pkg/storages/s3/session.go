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

func loadAWSConfig(ctx context.Context, cfg *Config) (aws.Config, []func(*s3.Options), error) {
	// Connect to a node from endpoint source, but keep configured endpoint as request host
	// Use configured endpoint protocol for connection
	scheme, host := endpointSchemeAndHost(cfg.Endpoint)
	if cfg.EndpointProtocol != "" {
		scheme = cfg.EndpointProtocol
	}

	// Verify TLS against configured endpoint, not node address
	serverName := ""
	if cfg.EndpointSource != "" && scheme == "https" {
		serverName = tlsServerName(host)
	}

	httpClient, err := buildHTTPClient(cfg, serverName)
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

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, nil, fmt.Errorf("load default AWS config: %w", err)
	}

	if err := configureCredentials(ctx, &awsCfg, cfg); err != nil {
		return aws.Config{}, nil, err
	}

	region, err := resolveRegion(ctx, &awsCfg, cfg)
	if err != nil {
		return aws.Config{}, nil, err
	}
	awsCfg.Region = region

	awsCfg.Retryer = newRetryerFunc(cfg)

	s3Opts, err := buildS3Options(cfg, scheme, host)
	if err != nil {
		return aws.Config{}, nil, err
	}

	ycOpts, err := applyYCSessionToken(&awsCfg, cfg)
	if err != nil {
		return aws.Config{}, nil, err
	}

	return awsCfg, append(s3Opts, ycOpts...), nil
}

func buildS3Options(cfg *Config, scheme, host string) ([]func(*s3.Options), error) {
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
		s3Opts = append(s3Opts, withDynamicEndpoint(cfg.EndpointSource, cfg.EndpointPort, cfg.Endpoint, scheme, host))
	}

	if cfg.RequestAdditionalHeaders != "" {
		headers, err := decodeHeaders(cfg.RequestAdditionalHeaders)
		if err != nil {
			return nil, fmt.Errorf("decode additional headers for S3 requests: %w", err)
		}
		s3Opts = append(s3Opts, withAdditionalHeaders(headers))
	}

	if cfg.Disable100Continue {
		s3Opts = append(s3Opts, withDisable100Continue())
	}

	return s3Opts, nil
}

func applyYCSessionToken(awsCfg *aws.Config, cfg *Config) ([]func(*s3.Options), error) {
	if cfg.UseYCSessionToken == "" {
		return nil, nil
	}
	useYC, err := strconv.ParseBool(cfg.UseYCSessionToken)
	if err != nil {
		return nil, fmt.Errorf("invalid YC session token: %w", err)
	}
	if !useYC {
		return nil, nil
	}
	// Yandex Cloud mimics the EC2 metadata service. Override default credentials
	// with the IMDS provider, then copy X-Amz-Security-Token to X-YaCloud-SubjectToken
	// after AWS signing has stamped the former onto the request.
	awsCfg.Credentials = aws.NewCredentialsCache(ec2rolecreds.New())
	return []func(*s3.Options){withYCSubjectToken()}, nil
}

func buildHTTPClient(cfg *Config, tlsServerName string) (aws.HTTPClient, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	if tlsServerName != "" {
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		transport.TLSClientConfig.ServerName = tlsServerName
	}

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

func configureCredentials(ctx context.Context, awsCfg *aws.Config, cfg *Config) error {
	accessKey := cfg.AccessKey
	secretKey := cfg.Secrets.SecretKey
	sessionToken := cfg.SessionToken

	if cfg.RoleARN != "" {
		if os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" && os.Getenv("AWS_ROLE_ARN") != "" {
			tracelog.InfoLogger.Printf("Running with IRSA, skipping explicit role assumption")
		} else {
			stsClient := sts.NewFromConfig(*awsCfg)
			out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
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

func resolveRegion(ctx context.Context, awsCfg *aws.Config, cfg *Config) (string, error) {
	if cfg.Region != "" {
		return cfg.Region, nil
	}
	if cfg.Endpoint == "" || strings.HasSuffix(cfg.Endpoint, ".amazonaws.com") {
		region, err := detectAWSRegionByBucket(ctx, awsCfg, cfg.Bucket, cfg.Endpoint)
		if err != nil {
			return "", fmt.Errorf("AWS region isn't configured explicitly: detect region: %w", err)
		}
		return region, nil
	}
	// S3-compatible services (Minio, Ceph, etc.) accept us-east-1 as a stand-in.
	// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
	return "us-east-1", nil
}

func detectAWSRegionByBucket(ctx context.Context, awsCfg *aws.Config, bucket, endpoint string) (string, error) {
	probe := *awsCfg
	probe.Region = "us-east-1"
	client := s3.NewFromConfig(probe, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	out, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
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
func withDynamicEndpoint(endpointSource, port, staticEndpoint, scheme, host string) func(*s3.Options) {
	mw := &dynamicEndpointMiddleware{
		source:         endpointSource,
		port:           port,
		staticEndpoint: staticEndpoint,
		scheme:         scheme,
		host:           host,
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
	scheme         string
	host           string
}

func (*dynamicEndpointMiddleware) ID() string { return "walgDynamicEndpoint" }

func (m *dynamicEndpointMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (middleware.BuildOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleBuild(ctx, in)
	}
	endpoint := requestEndpointFromSource(ctx, m.source, m.port)
	if endpoint != nil {
		tracelog.DebugLogger.Printf("using S3 endpoint %s", *endpoint)
		req.Host = m.host
		req.URL.Host = *endpoint
		req.URL.Scheme = m.scheme
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

// withContentMD5 makes DeleteObjects send Content-Md5 instead of
// x-amz-checksum-crc32. MinIO and other S3 compatible stores reject
// multi-object delete without it. Workaround published by AWS in
// https://github.com/aws/aws-sdk-go-v2/discussions/2960
func withContentMD5(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		// Removes report an error when an operation never registered the middleware
		_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
		_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
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

// endpointSchemeAndHost splits the configured endpoint into a connection scheme and a host.
// A scheme-less endpoint defaults to https, matching the AWS SDK behavior.
func endpointSchemeAndHost(endpoint string) (scheme, host string) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		return "https", strings.TrimPrefix(endpoint, "https://")
	case strings.HasPrefix(endpoint, "http://"):
		return "http", strings.TrimPrefix(endpoint, "http://")
	default:
		return "https", endpoint
	}
}

// tlsServerName strips an optional port from the host so it can be used as a TLS SNI value.
func tlsServerName(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func requestEndpointFromSource(ctx context.Context, endpointSource, port string) *string {
	t := http.DefaultTransport
	c := http.DefaultClient
	if tr, ok := t.(*http.Transport); ok {
		tr.DisableKeepAlives = true
		c = &http.Client{Transport: tr}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointSource, nil)
	if err != nil {
		tracelog.ErrorLogger.Printf("Endpoint source request error: %v ", err)
		return nil
	}
	resp, err := c.Do(req)
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
