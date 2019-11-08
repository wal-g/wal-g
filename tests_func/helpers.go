package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"unsafe"
)

const envDockerMachineName = "DOCKER_MACHINE_NAME"
const composeFile = "./staging/docker-compose.yml"

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func getContainerWithPrefix(containers []types.Container, name string) (*types.Container, error) {
	for _, container := range containers {
		if stringInSlice(name, container.Names){
			return &container, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Cannot find container with name %s", name))
}

func GetDockerContainer(testContext *TestContextType, prefix string) *types.Container {
	dockerClient := testContext.DockerClient
	containers, err := dockerClient.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		panic(err)
	}
	containerWithPrefixPointer, err := getContainerWithPrefix(containers, fmt.Sprintf("/%s", prefix))
	if err != nil {
		panic(err)
	}
	return containerWithPrefixPointer
}

func getExposedPort(container types.Container, port uint16) (string, uint16){
	machineName, hasMachineName := os.LookupEnv(envDockerMachineName)
	host := "127.0.0.1"
	if hasMachineName {
		hostBytes, err := exec.Command("docker-machine", "ip", machineName).Output()
		if err != nil {
			panic(err)
		}
		host = string(hostBytes)
	}
	bindings := container.Ports
	for _, value := range bindings {
		if value.Type != "tcp" {
			continue
		}
		if value.PrivatePort == port {
			return host, value.PublicPort
		}
	}
	panic("cannot get port")
}


func callCompose(testContext  *TestContextType, actions []string) {
	baseArgs := []string{"--file", composeFile, "-p", "test"}
	baseArgs = append(baseArgs, actions...)
	cmd := exec.Command("docker-compose", baseArgs...)
	for _, line:= range testContext.Env {
		cmd.Env = append(cmd.Env, line)
	}
	stdout, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(stdout)
	if err != nil {
		panic(err)
	}
	b := buf.Bytes()
	log := *(*string)(unsafe.Pointer(&b))
	fmt.Printf("\n%+v\n", log)
}

func getNetworkListWithName(testContext *TestContextType, name string) []types.NetworkResource {
	networkFilters := filters.NewArgs()
	networkResources, err := testContext.DockerClient.NetworkList(context.Background(), types.NetworkListOptions{
		Filters: networkFilters,
	})
	var result []types.NetworkResource
	for _, value := range networkResources {
		if value.Name == name {
			result = append(result, value)
		}
	}
	if err != nil {
		panic(err)
	}
	return result
}

func createNet(testContext *TestContextType, name string) {
	dockerClient := testContext.DockerClient
	name = testContext.Configuration.NetworkName
	if len(getNetworkListWithName(testContext, name)) != 0 {
		return
	}
	ipam := &network.IPAM{
		Config:  []network.IPAMConfig{{Subnet: fmt.Sprintf("10.0.%d.0/24", rand.Intn(255))}},
	}
	netOpts := map[string]string{
		"com.docker.network.bridge.enable_ip_masquerade": "true",
		"com.docker.network.bridge.enable_icc": "true",
		"com.docker.network.bridge.name": name,
	}
	config := types.NetworkCreate{
		IPAM: ipam,
		Options: netOpts,
		//EnableIPv6: true,
	}
	_, err := dockerClient.NetworkCreate(context.Background(), name, config)
	if err != nil {
		panic(err)
	}
}

func RemoveNet(testContext *TestContextType, name string) {
	nets := getNetworkListWithName(testContext, name)
	for _, net := range nets {
		err := testContext.DockerClient.NetworkRemove(context.Background(), net.ID)
		if err != nil {
			panic(err)
		}
	}
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

type SafeStorageType struct {
	CreatedBackupNames []string
	NometaBackupNames  []string
}

type TestContextType struct {
	DockerClient  *client.Client
	Env           []string
	SafeStorage   SafeStorageType
	Configuration ConfigurationType
	TestData      map[string]map[string]map[string][]DatabaseRecord
}

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

func ShutdownContainers(testContext *TestContextType) {
	callCompose(testContext, []string{"down"})
}

func ShutdownNetwork(testContext *TestContextType) {
	networkName := testContext.Configuration.NetworkName
	err := testContext.DockerClient.NetworkRemove(context.Background(), networkName)
	if err != nil {
		panic(err)
	}
}

func Start(testContext *TestContextType) {
	testContext.Env = mergeEnvs(os.Environ(), testContext.Env)
	createNet(testContext, GetVarFromEnvList(testContext.Env, "TEST_ID"))
	callCompose(testContext, []string{"build"})
	callCompose(testContext, []string{"up", "--detach"})
}

func Stop(testContext *TestContextType) {
	callCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func BuildBase(testContext *TestContextType) {
	var err error
	testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.40"))
	if err != nil {
		panic(err)
	}
	conf := getConfiguration(testContext)
	opts := types.ImageBuildOptions{
		Tags:           []string{conf.BaseImages["mongodb-backup-base"].tag},
	}
	buildContext, err := archive.TarWithOptions(conf.BaseImages["mongodb-backup-base"].path, &archive.TarOptions{})
	if err != nil {
		panic(err)
	}
	_, err = testContext.DockerClient.ImageBuild(context.Background(), buildContext, opts)
	if err != nil {
		panic(err)
	}
	testContext.Configuration = conf
}
