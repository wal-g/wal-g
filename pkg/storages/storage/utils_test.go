package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePrefixAsURL_URLInvalid(t *testing.T) {
	_, _, err := ParsePrefixAsURL("\r\n")
	assert.Error(t, err)
}

func TestParsePrefixAsURL_NoHostOrSchemaFound(t *testing.T) {
	urls := []string{
		"host.com/path",
		"/relative-path",
		"",
	}

	for _, url := range urls {
		_, _, err := ParsePrefixAsURL(url)
		assert.Error(t, err)
	}
}

func TestParsePrefixAsURL(t *testing.T) {
	testcases := []struct{ url, bucket, server string }{
		{url: "http://host.com", bucket: "host.com", server: ""},
		{url: "http://host.com/", bucket: "host.com", server: "/"},
		{url: "http://admin:pass@www.host.com:8080/path?v=query#anchor", bucket: "www.host.com:8080", server: "/path"},
	}

	for _, tc := range testcases {
		bucket, server, err := ParsePrefixAsURL(tc.url)
		assert.Nil(t, err)
		assert.Equal(t, tc.bucket, bucket)
		assert.Equal(t, tc.server, server)
	}
}

func TestGetPathFromPrefix(t *testing.T) {
	testcases := []struct{ url, bucket, server string }{
		{url: "http://host.com", bucket: "host.com", server: ""},
		{url: "http://host.com/", bucket: "host.com", server: ""},
		{url: "http://admin:pass@www.host.com:8080/path?v=query#anchor", bucket: "www.host.com:8080", server: "path"},
	}

	for _, tc := range testcases {
		bucket, server, err := GetPathFromPrefix(tc.url)
		assert.Nil(t, err)
		assert.Equal(t, tc.bucket, bucket)
		assert.Equal(t, tc.server, server)
	}
}
