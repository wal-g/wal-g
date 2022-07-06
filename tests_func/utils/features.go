package utils

import (
	"fmt"
	"os"
	"path"
	"strings"
)

const (
	featuresDir = "features"
	featureExt  = ".feature"
)

func FindFeaturePaths(database, featurePrefix string) ([]string, error) {
	environ := ParseEnvLines(os.Environ())
	requestedFeatureName := environ["FEATURE"]

	databaseFeaturesPath := path.Join(featuresDir, database)
	foundFeatures, err := scanDirs(databaseFeaturesPath, func(fileName string) bool {
		if requestedFeatureName != "" && fileName != requestedFeatureName+featureExt {
			return false
		}
		if featurePrefix != "" && !strings.HasPrefix(fileName, featurePrefix) {
			return false
		}
		if !strings.HasSuffix(fileName, featureExt) {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	if len(foundFeatures) == 0 {
		return nil, fmt.Errorf("no features found")
	}

	return foundFeatures, nil
}

func scanDirs(dirPath string, fileFilter func(fileName string) bool) ([]string, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var foundFeatures []string
	for _, f := range files {
		if fileFilter(f.Name()) {
			featurePath := path.Join(dirPath, f.Name())
			foundFeatures = append(foundFeatures, featurePath)
		}
	}

	return foundFeatures, nil
}
