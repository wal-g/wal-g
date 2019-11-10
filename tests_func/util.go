package main

import (
	"bufio"
	"fmt"
	"os"
)

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

func SetupStaging(testContext *TestContextType) {
	stagingDir := Env["STAGING_DIR"]
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		err = os.Mkdir(stagingDir, os.ModeDir)
		if err != nil {
			panic(err)
		}
	}
	envFile := Env["ENV_FILE"]
	if _, err := os.Stat(envFile); os.IsNotExist(err) {
		file, err := os.OpenFile(envFile, os.O_CREATE | os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		for key, value := range Env {
			_, err = fmt.Fprintf(file, "%s=%s\n", key, value)
			if err != nil {
				panic(err)
			}
		}
	}
	testContext.Env = getTestEnv(testContext)
}
