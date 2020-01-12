package helpers

import (
	"context"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

const envDockerMachineName = "DOCKER_MACHINE_NAME"

func GetContainerWithPrefix(containers []types.Container, name string) (*types.Container, error) {
	for _, container := range containers {
		if testUtils.StringInSlice(name, container.Names) {
			return &container, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("cannot find container with name %s", name))
}

func GetDockerContainer(testContext *TestContextType, prefix string) (*types.Container, error) {
	dockerClient := testContext.DockerClient
	containers, err := dockerClient.ContainerList(testContext.Context, types.ContainerListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error in getting docker container: %v", err)
	}
	containerWithPrefixPointer, err := GetContainerWithPrefix(containers, fmt.Sprintf("/%s", prefix))
	if err != nil {
		return nil, fmt.Errorf("error in getting docker container: %v", err)
	}
	return containerWithPrefixPointer, nil
}

func GetExposedPort(container types.Container, port uint16) (string, uint16, error) {
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
		if value.PrivatePort == port {
			return host, value.PublicPort, nil
		}
	}
	return "", 0, fmt.Errorf("error in getting exposed port")
}

func CallCompose(testContext *TestContextType, actions []string) error {
	composeFile := testUtils.GetVarFromEnvList(testContext.Env, "COMPOSE_FILE")
	baseArgs := []string{"--file", composeFile, "-p", "test"}
	baseArgs = append(baseArgs, actions...)
	cmd := exec.Command("docker-compose", baseArgs...)
	for _, line := range testContext.Env {
		cmd.Env = append(cmd.Env, line)
	}
	stdout, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("error when calling compose: %v", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error when calling compose: %v", err)
	}
	buf, err := ioutil.ReadAll(stdout)
	if err != nil {
		return fmt.Errorf("error when calling compose: %v", err)
	}
	fmt.Printf("\n%+v\n", string(buf))
	return nil
}

func getNetworkListWithName(testContext *TestContextType, name string) ([]types.NetworkResource, error) {
	networkFilters := filters.NewArgs()
	networkResources, err := testContext.DockerClient.NetworkList(testContext.Context, types.NetworkListOptions{
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

func CreateNet(testContext *TestContextType, name string) error {
	dockerClient := testContext.DockerClient
	name = testUtils.GetVarFromEnvList(testContext.Env, "NETWORK_NAME")
	networkList, err := getNetworkListWithName(testContext, name)
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
		"com.docker.network.bridge.name":                 name,
	}
	config := types.NetworkCreate{
		IPAM:    ipam,
		Options: netOpts,
	}
	_, err = dockerClient.NetworkCreate(testContext.Context, name, config)
	if err != nil {
		return fmt.Errorf("error in creating network: %v", err)
	}
	return nil
}

func RemoveNet(testContext *TestContextType, name string) error {
	nets, err := getNetworkListWithName(testContext, name)
	if err != nil {
		return fmt.Errorf("error im removing network %s: %v", name, err)
	}
	for _, net := range nets {
		err := testContext.DockerClient.NetworkRemove(testContext.Context, net.ID)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

type SafeStorageType struct {
	CreatedBackupNames []string
	NometaBackupNames  []string
}

type AuxData struct {
	Timestamps map[int]time.Time
}

type TestContextType struct {
	DockerClient *client.Client
	Env          []string
	SafeStorage  SafeStorageType
	TestData     map[string]map[string]map[string][]DatabaseRecord
	Context      context.Context
	AuxData      AuxData
}

func ShutdownContainers(testContext *TestContextType) error {
	return CallCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func ShutdownNetwork(testContext *TestContextType) error {
	networkName := testUtils.GetVarFromEnvList(testContext.Env, "NETWORK_NAME")
	err := testContext.DockerClient.NetworkRemove(testContext.Context, networkName)
	if err != nil {
		return fmt.Errorf("error in shutting down network: %v", err)
	}
	return nil
}
