package s3

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"gopkg.in/yaml.v3"
)

func createSession(config *Config) (*session.Session, error) {
	sessOpts := session.Options{}
	if config.CACertFile != "" {
		file, err := os.Open(config.CACertFile)
		if err != nil {
			return nil, err
		}
		defer utility.LoggedClose(file, "S3 CA cert file")
		sessOpts.CustomCABundle = file
	}

	sess, err := session.NewSessionWithOptions(sessOpts)
	if err != nil {
		return nil, fmt.Errorf("init new session: %w", err)
	}

	err = configureSession(sess, config)
	if err != nil {
		return nil, fmt.Errorf("configure session: %w", err)
	}

	if config.UseYCSessionToken != "" {
		useYcSessionToken, err := strconv.ParseBool(config.UseYCSessionToken)
		if err != nil {
			return nil, fmt.Errorf("invalid YC session token: %w", err)
		}
		if useYcSessionToken {
			// Yandex Cloud mimic metadata service, so we can use default AWS credentials, but set token to another header
			cred := credentials.NewCredentials(defaults.RemoteCredProvider(*defaults.Config(), defaults.Handlers()))
			sess.Config.WithCredentials(cred)
			sess.Handlers.Send.PushFront(func(r *request.Request) {
				token := r.HTTPRequest.Header.Get("X-Amz-Security-Token")
				r.HTTPRequest.Header.Add("X-YaCloud-SubjectToken", token)
			})
		}
	}

	if config.EndpointSource != "" {
		sess.Handlers.Validate.PushBack(func(request *request.Request) {
			endpoint := requestEndpointFromSource(config.EndpointSource, config.EndpointPort)
			if endpoint != nil {
				tracelog.DebugLogger.Printf("using S3 endpoint %s", *endpoint)
				host := strings.TrimPrefix(*sess.Config.Endpoint, "https://")
				request.HTTPRequest.Host = host
				request.HTTPRequest.Header.Add("Host", host)
				request.HTTPRequest.URL.Host = *endpoint
				request.HTTPRequest.URL.Scheme = "http"
			} else {
				tracelog.DebugLogger.Printf("using S3 endpoint %s", *sess.Config.Endpoint)
			}
		})
	}

	if config.RequestAdditionalHeaders != "" {
		headers, err := decodeHeaders(config.RequestAdditionalHeaders)
		if err != nil {
			return nil, fmt.Errorf("decode additional headers for S3 requests: %w", err)
		}

		sess.Handlers.Validate.PushBack(func(request *request.Request) {
			for k, v := range headers {
				request.HTTPRequest.Header.Add(k, v)
			}
		})
	}

	return sess, err
}

func configureSession(sess *session.Session, config *Config) error {
	awsConfig := sess.Config

	// DefaultRetryer implements basic retry logic using exponential backoff for
	// most services. If you want to implement custom retry logic, you can implement the
	// request.Retryer interface.
	awsConfig = request.WithRetryer(awsConfig, NewConnResetRetryer(
		client.DefaultRetryer{
			NumMaxRetries:    config.MaxRetries,
			MinThrottleDelay: config.MinThrottlingRetryDelay,
			MaxThrottleDelay: config.MaxThrottlingRetryDelay,
		}))

	awsConfig.HTTPClient.Transport = NewRoundTripperWithLogging(awsConfig.HTTPClient.Transport)
	accessKey := config.AccessKey
	secretKey := config.Secrets.SecretKey
	sessionToken := config.SessionToken

	if config.RoleARN != "" {
		stsSession := sts.New(sess)
		assumedRole, err := stsSession.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         aws.String(config.RoleARN),
			RoleSessionName: aws.String(config.SessionName),
		})
		if err != nil {
			return fmt.Errorf("assume role by ARN: %w", err)
		}
		accessKey = *assumedRole.Credentials.AccessKeyId
		secretKey = *assumedRole.Credentials.SecretAccessKey
		sessionToken = *assumedRole.Credentials.SessionToken
	}

	if accessKey != "" && secretKey != "" {
		provider := &credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
		}}
		providers := make([]credentials.Provider, 0)
		providers = append(providers, provider)
		providers = append(providers, defaults.CredProviders(awsConfig, defaults.Handlers())...)
		newCredentials := credentials.NewCredentials(&credentials.ChainProvider{
			VerboseErrors: aws.BoolValue(awsConfig.CredentialsChainVerboseErrors),
			Providers:     providers,
		})

		awsConfig = awsConfig.WithCredentials(newCredentials)
	}

	if config.LogLevel != "" {
		awsConfig = awsConfig.WithLogLevel(func(s string) aws.LogLevelType {
			switch s {
			case "DEVEL":
				return aws.LogDebug
			default:
				return aws.LogOff
			}
		}(config.LogLevel))
	}

	if config.Endpoint != "" {
		awsConfig = awsConfig.WithEndpoint(config.Endpoint)
	}

	awsConfig.S3ForcePathStyle = &config.ForcePathStyle

	if config.Region == "" {
		region, err := detectAWSRegion(config.Bucket, awsConfig)
		if err != nil {
			return fmt.Errorf("AWS region isn't configured explicitly: detect region: %w", err)
		}
		awsConfig = awsConfig.WithRegion(region)
	} else {
		awsConfig = awsConfig.WithRegion(config.Region)
	}

	sess.Config = awsConfig
	return nil
}

func detectAWSRegion(bucket string, awsConfig *aws.Config) (string, error) {
	if awsConfig.Endpoint == nil ||
		*awsConfig.Endpoint == "" ||
		strings.HasSuffix(*awsConfig.Endpoint, ".amazonaws.com") {
		region, err := detectAWSRegionByBucket(bucket, awsConfig)
		if err != nil {
			return "", fmt.Errorf("detect region by bucket: %w", err)
		}
		return region, nil
	}
	// For S3 compatible services like Minio, Ceph etc. use `us-east-1` as region
	// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
	return "us-east-1", nil
}

// detectAWSRegionByBucket attempts to detect the AWS region by the bucket name
func detectAWSRegionByBucket(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

func requestEndpointFromSource(endpointSource, port string) *string {
	resp, err := http.Get(endpointSource)
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
