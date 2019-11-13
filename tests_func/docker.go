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
	"log"
	"math/rand"
	"os"
	"os/exec"
	"unsafe"
)

const envDockerMachineName = "DOCKER_MACHINE_NAME"
const composeFile = "./staging/docker-compose.yml"

func getContainerWithPrefix(containers []types.Container, name string) (*types.Container, error) {
	for _, container := range containers {
		if stringInSlice(name, container.Names) {
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

func getExposedPort(container types.Container, port uint16) (string, uint16) {
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

func callCompose(testContext *TestContextType, actions []string) {
	baseArgs := []string{"--file", composeFile, "-p", "test"}
	baseArgs = append(baseArgs, actions...)
	cmd := exec.Command("docker-compose", baseArgs...)
	for _, line := range testContext.Env {
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

func ShutdownContainers(testContext *TestContextType) {
	callCompose(testContext, []string{"down", "--rmi", "local", "--remove-orphans"})
}

func ShutdownNetwork(testContext *TestContextType) {
	networkName := testContext.Configuration.NetworkName
	err := testContext.DockerClient.NetworkRemove(context.Background(), networkName)
	if err != nil {
		panic(err)
	}
}
