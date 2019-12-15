package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func MapEnvToListEnv(mpEnv map[string]string) []string {
	var result []string
	for key, value := range mpEnv {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	return result
}

func MergeEnvs(env []string, values []string) []string {
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

func StringInSlice(a string, list []string) bool {
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

func GenerateSecrets() map[string]string {
	return map[string]string{
		"MINIO_ACCESS_KEY": RandSeq(20),
		"MINIO_SECRET_KEY": RandSeq(40),
	}
}

func UpdateFileValues(filepath string, subs map[string]string) {
	minioDockerfile, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	lines := strings.Split(string(minioDockerfile), "\n")

	for i, _ := range lines {
		for key, value := range subs {
			lines[i] = strings.Replace(lines[i], "{{"+key+"}}", value, -1)
		}
	}

	err = ioutil.WriteFile(filepath, []byte(strings.Join(lines, "\n")), 0644)
	if err != nil {
		panic(err)
	}
}

func CopyDirectory(scrDir, dest string) error {
	entries, err := ioutil.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		stat, ok := fileInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("failed to get raw syscall.Stat_t data for '%s'", sourcePath)
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err := CreateIfNotExists(destPath, 0755); err != nil {
				return err
			}
			if err := CopyDirectory(sourcePath, destPath); err != nil {
				return err
			}
		case os.ModeSymlink:
			if err := CopySymLink(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err := Copy(sourcePath, destPath); err != nil {
				return err
			}
		}

		if err := os.Lchown(destPath, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}

		isSymlink := entry.Mode()&os.ModeSymlink != 0
		if !isSymlink {
			if err := os.Chmod(destPath, entry.Mode()); err != nil {
				return err
			}
		}
	}
	return nil
}

func Copy(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	defer out.Close()
	if err != nil {
		return err
	}

	in, err := os.Open(srcFile)
	defer in.Close()
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

func Exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}

func CreateIfNotExists(dir string, perm os.FileMode) error {
	if Exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func CopySymLink(source, dest string) error {
	link, err := os.Readlink(source)
	if err != nil {
		return err
	}
	return os.Symlink(link, dest)
}

func WriteEnvFile(envLines []string, envFile string) error {
	_, err := os.Stat(envFile)
	file, err := os.OpenFile(envFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error in setuping staging: %v", err)
	}
	defer file.Close()
	for _, envLine:= range envLines {
		key, value := SplitEnvLine(envLine)
		_, err = fmt.Fprintf(file, "%s=%s\n", key, value)
		if err != nil {
			return fmt.Errorf("error in setuping staging: %v", err)
		}
	}
	return nil
}