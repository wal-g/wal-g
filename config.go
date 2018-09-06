package walg

import (
	"path/filepath"
	"io/ioutil"
	"encoding/json"
	"os/user"
	"log"
	"github.com/go-yaml/yaml"
	"os"
	"strings"
)

var (
	WalgConfig        *map[string]string
	allowedConfigKeys = map[string]*string{
		"WALG_S3_PREFIX":               nil,
		"WALE_S3_PREFIX":               nil,
		"AWS_REGION":                   nil,
		"WALG_DOWNLOAD_CONCURRENCY":    nil,
		"WALG_UPLOAD_CONCURRENCY":      nil,
		"WALG_UPLOAD_DISK_CONCURRENCY": nil,
		"WALG_SENTINEL_USER_DATA":      nil,
		"WALG_PREVENT_WAL_OVERWRITE":   nil,
		"AWS_ENDPOINT":                 nil,
		"AWS_S3_FORCE_PATH_STYLE":      nil,
		"WALG_S3_STORAGE_CLASS":        nil,
		"WALG_S3_SSE":                  nil,
		"WALG_S3_SSE_KMS_ID":           nil,
		"WALG_GPG_KEY_ID":              nil,
		"WALE_GPG_KEY_ID":              nil,
		"WALG_DELTA_MAX_STEPS":         nil,
		"WALG_DELTA_ORIGIN":            nil,
		"WALG_COMPRESSION_METHOD":      nil,
		"WALG_DISK_RATE_LIMIT":         nil,
		"WALG_NETWORK_RATE_LIMIT":      nil,
		"WALG_USE_WAL_DELTA":      nil,
	}
)

func init() {
	readConfig()
	verifyConfig()
}

func verifyConfig() {
	if WalgConfig == nil {
		return
	}
	for k, _ := range *WalgConfig {
		if _, ok := allowedConfigKeys[k]; !ok {
			log.Panic("Settings " + k + " is unknown")
		}
	}
}

func readConfig() {
	usr, err := user.Current()
	if err == nil {
		cacheFilename := filepath.Join(usr.HomeDir, ".walg.json")
		file, err := ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			err = json.Unmarshal(file, &WalgConfig)
			if err != nil {
				log.Panic(err)
			}
			return
		} else if !os.IsNotExist(err) {
			log.Panic(err)
		}

		cacheFilename = filepath.Join(usr.HomeDir, ".walg.json")
		file, err = ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			err := yaml.Unmarshal(file, &WalgConfig)
			if err != nil {
				log.Panic(err)
			}
			return
		} else if !os.IsNotExist(err) {
			log.Panic(err)
		}
	}
}

func LookupConfigValue(key string) (value string, ok bool) {
	if WalgConfig != nil {
		if val, ok := (*WalgConfig)[key]; ok {
			return val, true
		}
	}
	return os.LookupEnv(key)
}

func getSettingValue(key string) string {
	if strings.HasPrefix(key, "WALE") {
		walgKey := "WALG" + strings.TrimPrefix(key, "WALE")
		if val, ok := LookupConfigValue(walgKey); ok && len(val) > 0 {
			return val
		}
	}

	value, ok := LookupConfigValue(key)
	if ok {
		return value
	}
	return ""
}
