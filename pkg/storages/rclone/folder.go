package rclone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Folder struct {
	remotePath string
	subPath    string
	config     *Config
}

type rcloneListItem struct {
	Path     string    `json:"Path"`
	Name     string    `json:"Name"`
	Size     int64     `json:"Size"`
	MimeType string    `json:"MimeType"`
	ModTime  time.Time `json:"ModTime"`
	IsDir    bool      `json:"IsDir"`
}

func NewFolder(remotePath string, subPath string, config *Config) *Folder {
	return &Folder{
		remotePath: remotePath,
		subPath:    subPath,
		config:     config,
	}
}

func (f *Folder) GetPath() string {
	return f.subPath + "/"
}

func (f *Folder) getFullPath() string {
	if f.subPath == "" {
		return f.remotePath
	}
	return path.Join(f.remotePath, f.subPath)
}

func (f *Folder) ListFolder(_ context.Context) (objects []storage.Object, subFolders []storage.Folder, err error) {
	fullPath := f.getFullPath()

	args := f.buildBaseArgs()
	args = append(args, "lsjson", "--recursive=false", fullPath)

	output, err := f.runRcloneCommand(args...)
	if err != nil {
		if isNotFoundError(err) {
			return []storage.Object{}, []storage.Folder{}, nil
		}
		return nil, nil, fmt.Errorf("list folder %q: %w", fullPath, err)
	}

	var items []rcloneListItem
	if err := json.Unmarshal(output, &items); err != nil {
		return nil, nil, fmt.Errorf("parse rclone output: %w", err)
	}

	objects = make([]storage.Object, 0)
	subFolders = make([]storage.Folder, 0)

	for _, item := range items {
		if item.IsDir {
			subFolderPath := path.Join(f.subPath, item.Name)
			subFolders = append(subFolders, NewFolder(f.remotePath, subFolderPath, f.config))
		} else {
			objects = append(objects, storage.NewLocalObject(
				item.Name,
				item.ModTime,
				item.Size,
			))
		}
	}

	return objects, subFolders, nil
}

func (f *Folder) DeleteObjects(_ context.Context, objects []storage.Object) error {
	if len(objects) == 0 {
		return nil
	}

	for _, obj := range objects {
		objPath := obj.GetName()
		fullPath := path.Join(f.getFullPath(), objPath)

		args := f.buildBaseArgs()
		args = append(args, "deletefile", fullPath)

		if _, err := f.runRcloneCommand(args...); err != nil {
			if !isNotFoundError(err) {
				return fmt.Errorf("delete object %q: %w", objPath, err)
			}
			tracelog.WarningLogger.Printf("Object %q does not exist, skipping deletion", objPath)
		}
	}

	return nil
}

func (f *Folder) Exists(_ context.Context, objectRelativePath string) (bool, error) {
	fullPath := path.Join(f.getFullPath(), objectRelativePath)

	args := f.buildBaseArgs()
	args = append(args, "lsjson", fullPath)

	output, err := f.runRcloneCommand(args...)
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("check existence of %q: %w", objectRelativePath, err)
	}

	var items []rcloneListItem
	if err := json.Unmarshal(output, &items); err != nil {
		return false, fmt.Errorf("parse rclone output: %w", err)
	}

	return len(items) > 0, nil
}

func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		f.remotePath,
		path.Join(f.subPath, subFolderRelativePath),
		f.config,
	)
}

func (f *Folder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	fullPath := path.Join(f.getFullPath(), objectRelativePath)

	args := f.buildBaseArgs()
	args = append(args, "cat", fullPath)

	cmd := exec.CommandContext(ctx, f.config.RcloneBinary, args...)
	if f.config.ConfigPath != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("RCLONE_CONFIG=%s", f.config.ConfigPath))
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create stdout pipe: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start rclone cat command: %w", err)
	}

	return &rcloneReader{
		reader: stdout,
		cmd:    cmd,
		stderr: stderr,
		path:   objectRelativePath,
	}, nil
}

func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	fullPath := path.Join(f.getFullPath(), name)

	args := f.buildBaseArgs()
	args = append(args, "rcat", fullPath)

	cmd := exec.CommandContext(ctx, f.config.RcloneBinary, args...)
	if f.config.ConfigPath != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("RCLONE_CONFIG=%s", f.config.ConfigPath))
	}

	cmd.Stdin = content

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload object %q: %w: %s", name, err, stderr.String())
	}

	return nil
}

func (f *Folder) CopyObject(_ context.Context, srcPath string, dstPath string) error {
	srcFullPath := path.Join(f.getFullPath(), srcPath)
	dstFullPath := path.Join(f.getFullPath(), dstPath)

	args := f.buildBaseArgs()
	args = append(args, "copyto", srcFullPath, dstFullPath)

	if _, err := f.runRcloneCommand(args...); err != nil {
		return fmt.Errorf("copy object from %q to %q: %w", srcPath, dstPath, err)
	}

	return nil
}

func (f *Folder) Validate(_ context.Context) error {
	args := f.buildBaseArgs()
	args = append(args, "about", f.remotePath)

	if _, err := f.runRcloneCommand(args...); err != nil {
		return fmt.Errorf("validate rclone remote %q: %w", f.remotePath, err)
	}

	return nil
}

func (f *Folder) SetVersioningEnabled(_ context.Context, _ bool) {}

func (f *Folder) GetVersioningEnabled(_ context.Context) bool {
	return false
}

func (f *Folder) buildBaseArgs() []string {
	args := []string{}

	if f.config.ConfigPath != "" {
		args = append(args, "--config", f.config.ConfigPath)
	}

	args = append(args,
		"--transfers", fmt.Sprintf("%d", f.config.Transfers),
		"--buffer-size", fmt.Sprintf("%dM", f.config.BufferSize/(1024*1024)),
		"--timeout", fmt.Sprintf("%ds", f.config.Timeout),
		"--retries", fmt.Sprintf("%d", f.config.Retries),
		"--low-level-retries", fmt.Sprintf("%d", f.config.LowLevelRetries),
	)

	if f.config.S3ChunkSize > 0 {
		args = append(args, "--s3-chunk-size", fmt.Sprintf("%dM", f.config.S3ChunkSize/(1024*1024)))
	}

	if f.config.UploadConcurrency > 0 {
		args = append(args, "--s3-upload-concurrency", fmt.Sprintf("%d", f.config.UploadConcurrency))
	}

	if f.config.ExtraArgs != "" {
		extraArgs := strings.Fields(f.config.ExtraArgs)
		args = append(args, extraArgs...)
	}

	return args
}

func (f *Folder) runRcloneCommand(args ...string) ([]byte, error) {
	cmd := exec.Command(f.config.RcloneBinary, args...)
	if f.config.ConfigPath != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("RCLONE_CONFIG=%s", f.config.ConfigPath))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, string(output))
	}

	return output, nil
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "no such file") ||
		strings.Contains(errStr, "doesn't exist") ||
		strings.Contains(errStr, "directory not found")
}

type rcloneReader struct {
	reader io.Reader
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	path   string
}

func (r *rcloneReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *rcloneReader) Close() error {
	if err := r.cmd.Wait(); err != nil {
		stderrContent := r.stderr.String()
		if stderrContent != "" {
			return fmt.Errorf("rclone read %q failed: %w: %s", r.path, err, stderrContent)
		}
		return fmt.Errorf("rclone read %q failed: %w", r.path, err)
	}
	return nil
}
