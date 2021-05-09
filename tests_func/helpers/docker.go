package helpers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/utils"
)

var Docker *client.Client

const envDockerMachineName = "DOCKER_MACHINE_NAME"

type ExecResult struct {
	ExitCode     int
	stdoutBuffer *bytes.Buffer
	stderrBuffer *bytes.Buffer
}

func (res *ExecResult) Stdout() string {
	return res.stdoutBuffer.String()
}

func (res *ExecResult) Stderr() string {
	return res.stderrBuffer.String()
}

func (res *ExecResult) Combined() string {
	return res.stdoutBuffer.String() + res.stderrBuffer.String()
}

func (res *ExecResult) String() string {
	return fmt.Sprintf("code: %d\nstdout:\n%s\nstderr:\n%s\n", res.ExitCode, res.Stdout(), res.Stderr())
}

type RunOptions struct {
	user string
}

type RunOption func(*RunOptions)

func User(user string) RunOption {
	return func(args *RunOptions) {
		args.user = user
	}
}

func TimeInContainer(ctx context.Context, container string) (time.Time, error) {
	exc, err := RunCommandStrict(ctx, container, []string{"date", "+%s"})
	if err != nil {
		return time.Time{}, err
	}
	ts, err := strconv.ParseInt(strings.Trim(exc.Stdout(), "\n"), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0), nil
}

func RunCommandStrict(ctx context.Context, container string, command []string) (ExecResult, error) {
	cmdLine := strings.Join(command, " ")

	exc, err := RunCommand(ctx, container, command)
	if err != nil {
		return exc, err
	}
	if exc.ExitCode != 0 {
		tracelog.ErrorLogger.Printf("'%s' failed with %d\nstdout:\n%s\nstderr:\n%s\n",
			cmdLine, exc.ExitCode, exc.Stdout(), exc.Stderr())
		return exc, fmt.Errorf("%s exit code: %d", cmdLine, exc.ExitCode)
	}
	return exc, nil

}

func RunCommand(ctx context.Context,
	container string, cmd []string, setters ...RunOption) (ExecResult, error) {
	args := &RunOptions{}
	for _, setter := range setters {
		setter(args)
	}

	execConfig := types.ExecConfig{
		AttachStdout: true,
		AttachStderr: true,
		User:         args.user,
		Cmd:          cmd,
	}

	containerExec, err := Docker.ContainerExecCreate(ctx, container, execConfig)
	if err != nil {
		return ExecResult{}, err
	}

	attach, err := Docker.ContainerExecAttach(ctx, containerExec.ID, types.ExecConfig{})
	if err != nil {
		return ExecResult{}, err
	}
	defer attach.Close()

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Running command on %s: %v", container, cmdLine)

	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)

	go func() {
		_, err = stdcopy.StdCopy(&outBuf, &errBuf, attach.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return ExecResult{}, err
		}
	case <-ctx.Done():
		return ExecResult{}, ctx.Err()
	}

	execInspect, err := Docker.ContainerExecInspect(ctx, containerExec.ID)
	if err != nil {
		return ExecResult{}, err
	}
	exc := ExecResult{ExitCode: execInspect.ExitCode, stdoutBuffer: &outBuf, stderrBuffer: &errBuf}
	tracelog.DebugLogger.Printf("'%s' exit code %d\nstdout:\n%s\nstderr:\n%s\n",
		cmdLine, exc.ExitCode, exc.Stdout(), exc.Stderr())

	return exc, nil
}

func ContainerWithPrefix(containers []types.Container, name string) (*types.Container, error) {
	for _, container := range containers {
		if utils.StringInSlice(name, container.Names) {
			return &container, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("cannot find container with name %s", name))
}

func DockerContainer(ctx context.Context, prefix string) (*types.Container, error) {
	containers, err := Docker.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error in getting docker container: %v", err)
	}
	containerWithPrefixPointer, err := ContainerWithPrefix(containers, fmt.Sprintf("/%s", prefix))
	if err != nil {
		return nil, fmt.Errorf("error in getting docker container: %v", err)
	}
	return containerWithPrefixPointer, nil
}

func ExposedPort(container types.Container, port int) (string, int, error) {
	machineName, hasMachineName := os.LookupEnv(envDockerMachineName)
	host := "localhost"
	if hasMachineName {
		hostBytes, err := exec.Command("docker-machine", "ip", machineName).Output()
		if err != nil {
			return "", 0, fmt.Errorf("error in getting exposed port: %v", err)
		}
		host = string(hostBytes)
	}

	bindings := container.Ports
	for _, value := range bindings {
		if value.Type != "tcp" {
			continue
		}
		if int(value.PrivatePort) == port {
			return host, int(value.PublicPort), nil
		}
	}
	return "", 0, fmt.Errorf("error in getting exposed port")
}

func ListNets(ctx context.Context, name string) ([]types.NetworkResource, error) {
	networkFilters := filters.NewArgs()
	networkResources, err := Docker.NetworkList(ctx, types.NetworkListOptions{
		Filters: networkFilters,
	})
	var result []types.NetworkResource
	for _, value := range networkResources {
		if value.Name == name {
			result = append(result, value)
		}
	}
	if err != nil {
		return []types.NetworkResource{}, fmt.Errorf("error in getting network list with name: %v", err)
	}
	return result, nil
}

func CreateNet(ctx context.Context, netName string) error {
	networkList, err := ListNets(ctx, netName)
	if err != nil {
		return fmt.Errorf("error in creating network: %v", err)
	}
	if len(networkList) != 0 {
		return nil
	}
	ipam := &network.IPAM{
		Config: []network.IPAMConfig{{Subnet: fmt.Sprintf("10.0.%d.0/24", rand.Intn(255))}},
	}
	netOpts := map[string]string{
		"com.docker.network.bridge.enable_ip_masquerade": "true",
		"com.docker.network.bridge.enable_icc":           "true",
		"com.docker.network.bridge.netName":              netName,
	}
	config := types.NetworkCreate{
		IPAM:    ipam,
		Options: netOpts,
	}
	_, err = Docker.NetworkCreate(ctx, netName, config)
	if err != nil {
		return fmt.Errorf("error in creating network: %v", err)
	}
	return nil
}

func RemoveNet(ctx context.Context, netName string) error {
	nets, err := ListNets(ctx, netName)
	if err != nil {
		return fmt.Errorf("error im removing network %s: %v", netName, err)
	}
	for _, net := range nets {
		if err := Docker.NetworkRemove(ctx, net.ID); err != nil {
			panic(err)
		}
	}
	return nil
}

func ExposedHostPort(ctx context.Context, fqdn string, port int) (string, int, error) {
	dockerContainer, err := DockerContainer(ctx, fqdn)
	if err != nil {
		return "", 0, fmt.Errorf("can not get docker container: %v", err)
	}
	return ExposedPort(*dockerContainer, port)
}

func BuildImage(ctx context.Context, tag string, path string) error {
	cmd := exec.CommandContext(ctx, "docker", "build", "-t", tag, path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error in building base: %v", err)
	}
	return nil
}

type BaseImage struct {
	Path string
	Tag  string
}

type Infra struct {
	ctx       context.Context
	config    string
	env       map[string]string
	net       string
	baseImage BaseImage
}

func NewInfra(ctx context.Context,
	config string, env map[string]string, net string, base BaseImage) *Infra {
	return &Infra{
		ctx:       ctx,
		config:    config,
		env:       env,
		net:       net,
		baseImage: base,
	}
}

func (inf *Infra) Setup() error {
	if err := CreateNet(inf.ctx, inf.net); err != nil {
		return fmt.Errorf("can not create network: %v", err)
	}

	if err := BuildImage(inf.ctx, inf.baseImage.Tag, inf.baseImage.Path); err != nil {
		return fmt.Errorf("can not build base image: %v", err)
	}

	actions := []string{"--verbose", "--log-level", "WARNING", "build"}
	if err := inf.callCompose(actions); err != nil {
		return fmt.Errorf("can not build images: %v", err)
	}

	return nil
}

func (inf *Infra) RecreateContainers() error {
	actions := []string{"--verbose", "--log-level", "WARNING", "down", "--volumes", "--timeout", "0"}
	if err := inf.callCompose(actions); err != nil {
		return err
	}
	return inf.callCompose([]string{"--verbose", "--log-level", "WARNING", "up", "--detach"})
}

func (inf *Infra) Shutdown() error {
	actions := []string{"down", "--rmi", "local", "--remove-orphans", "--timeout", "0"}
	if err := inf.callCompose(actions); err != nil {
		return fmt.Errorf("can not shutdown containers: %v", err)
	}

	if err := Docker.NetworkRemove(inf.ctx, inf.net); err != nil {
		return fmt.Errorf("error in shutting down network: %v", err)
	}
	return nil
}

func (inf *Infra) callCompose(actions []string) error {
	baseArgs := []string{"--file", inf.config, "-p", "test"}
	baseArgs = append(baseArgs, actions...)
	// lookup the full path before exec.CommandContext call
	// (fixes https://github.com/docker/compose/issues/1135)
	fullPath, err := exec.LookPath("docker-compose")
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(inf.ctx, fullPath, baseArgs...)
	for _, line := range utils.EnvToList(inf.env) {
		cmd.Env = append(cmd.Env, line)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("can not start command: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("error when calling compose: %v", err)
	}

	return nil
}

func init() {
	var err error
	Docker, err = client.NewEnvClient()
	tracelog.ErrorLogger.FatalOnError(err)
}
