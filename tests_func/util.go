package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func mergeEnvs(env []string, values []string) []string {
	envMap := make(map[string]string, 0)
	for _, line := range append(env, values...) {
		name, value := SplitEnvLine(line)
		envMap[name] = value
	}
	updatedEnv := make([]string, 0)
	for name, value := range envMap {
		updatedEnv = append(updatedEnv, fmt.Sprintf("%s=%s", name, value))
	}
	return updatedEnv
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func SplitEnvLine(line string) (string, string) {
	values := strings.Split(line, "=")
	return values[0], values[1]
}

func GetVarFromEnvList(env []string, name string) string {
	for _, value := range env {
		currentName, currentValue := SplitEnvLine(value)
		if currentName == name {
			return currentValue
		}
	}
	return ""
}

func getTestEnv(testContext *TestContextType) []string {
	if testContext.Env != nil {
		return testContext.Env
	}
	env := make([]string, 0)
	envFile := Env["ENV_FILE"]
	file, err := os.OpenFile(envFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		key, value := SplitEnvLine(line)
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return env
}

func generateSecrets(testContext *TestContextType) map[string]string {
	return map[string]string{
		"MINIO_ACCESS_KEY": RandSeq(20),
		"MINIO_SECRET_KEY": RandSeq(40),
	}
}

func updateFileValues(filepath string, subs map[string]string) {
	minioDockerfile, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	lines := strings.Split(string(minioDockerfile), "\n")

	for i, line := range lines {
		for prefix, value := range subs {
			if strings.HasPrefix(line, prefix) {
				lines[i] = prefix + value
			}
		}
	}

	err = ioutil.WriteFile(filepath, []byte(strings.Join(lines, "\n")), 0644)
	if err != nil {
		panic(err)
	}
}
