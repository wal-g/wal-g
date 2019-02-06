package internal

import (
	"bytes"
	"fmt"
	"github.com/ncw/swift"
	"github.com/pkg/errors"
	"github.com/tamalsaha/wal-g-demo/tracelog"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type SwiftFolderError struct {
	error
}

func NewSwiftFolderError(err error, format string, args ...interface{}) SwiftFolderError {
	return SwiftFolderError{errors.Wrapf(err, format, args...)}
}

func (err SwiftFolderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func NewSwiftFolder(connection *swift.Connection, container swift.Container, path string) *SwiftFolder {
	return &SwiftFolder{connection,container, path}
}

func ConfigureSwiftFolder(prefix string) (StorageFolder, error) {
	swiftCredentials := getSwiftCredentials()
	username := swiftCredentials["user"]
	key := swiftCredentials["key"]
	authURL := swiftCredentials["authURL"]
	authType := swiftCredentials["authType"]
	if swiftCredentials["exists"] != "yes" {
		return nil, NewSwiftFolderError(errors.New("Credential error"),
			"Either the OS_USERNAME, OS_PASSWORD or OS_AUTH_URL environment variable is not set")
	}
	var region,tenantID,tenantName string
	// v2 and v3 authentication require tenant ID or name as well
	if authType != "v1"{
		tenantID = swiftCredentials["tenantID"]
		tenantName = swiftCredentials["tenantName"]
		region = swiftCredentials["region"]
	}
	//v3 authentication requires tenant id, domain name, domain
	if authType!= "v2" && authType != "v1"{
		//domain name, id, and such
	}
	var connection *swift.Connection

	if swiftCredentials["authType"] == "v1"{
		// Create a v1 auth connection
		connection = &swift.Connection{
			UserName: username,
			ApiKey: key,
			AuthUrl: authURL,
			Region:region,
		}
	}else if swiftCredentials["authType"] == "v2"{
		// Create a v2 auth connection
		connection = &swift.Connection{
			UserName: username,
			ApiKey: key,
			AuthUrl: authURL,
			Tenant: tenantName,
			TenantId:tenantID,
			Region:region,
		}
	}
	// Authenticate
	err := connection.Authenticate()
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to authenticate Swift connection")
	}

	containerName, path, err := getPathFromPrefix(prefix)
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to get container name and path from prefix %v",prefix)
	}
	path = addDelimiterToSwiftPath(path)

	container,_,err := connection.Container(containerName)
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to fetch Swift container from name %v",containerName)
	}

	return NewSwiftFolder(connection,container, path), nil
}

func addDelimiterToSwiftPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
}

type SwiftFolder struct {
	connection *swift.Connection
	container swift.Container
	path         string
}

func (folder *SwiftFolder) GetPath() string {
	return folder.path
}

func (folder *SwiftFolder) Exists(objectRelativePath string) (bool, error) {
	path := JoinS3Path(folder.path, objectRelativePath)
	_,_,err  := folder.connection.Object(folder.container.Name,path)
	if err == swift.ObjectNotFound{
		return false,nil
	}
	if err != nil {
		return false, NewSwiftFolderError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *SwiftFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	//Iterate
	err = folder.connection.ObjectsWalk(folder.container.Name, &swift.ObjectsOpts{Delimiter:int32('/'),Prefix:folder.path}, func(opts *swift.ObjectsOpts) (interface{}, error) {

		objectNames, err := folder.connection.ObjectNames(folder.container.Name, opts)
		for _,objectName := range objectNames {
			if strings.HasSuffix(objectName,"/"){
				subFolders = append(subFolders, NewSwiftFolder(folder.connection,folder.container, objectName))
			}else{
				obj,_,err := folder.connection.Object(folder.container.Name,objectName)
				if err != nil{
					return nil,err
				}
				objects = append(objects,&SwiftStorageObject{name:obj.Name,updated:obj.LastModified})
			}
		}

		return objectNames, err
	})
	if err != nil{
		return nil,nil, NewSwiftFolderError(err, "Unable to iterate %v",folder.path)
	}
	return
}

func (folder *SwiftFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return NewSwiftFolder(folder.connection,folder.container, addDelimiterToAzPath(JoinS3Path(folder.path, subFolderRelativePath)))
}

func (folder *SwiftFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := JoinS3Path(folder.path, objectRelativePath)
	contentBytes, err := folder.connection.ObjectGetBytes(folder.container.Name,path)
	if err != nil{
		return nil, NewSwiftFolderError(err, "Unable to fetch content %v",path)
	}
	readContents := bytes.NewReader(contentBytes)
	return ioutil.NopCloser(readContents), nil
}

func (folder *SwiftFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	path := JoinS3Path(folder.path, name)
	_,err := folder.connection.ObjectPut(folder.container.Name,path,content,false,"","",nil)
	if err != nil {
		return NewSwiftFolderError(err, "Unable to write content.")
	}
	return nil
}

func (folder *SwiftFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := JoinS3Path(folder.path, objectRelativePath)
		tracelog.DebugLogger.Printf("Delete object %v\n",path)
		err := folder.connection.ObjectDelete(folder.container.Name,path)
		if err == swift.ObjectNotFound{
			continue
		}
		if err != nil {
			return NewSwiftFolderError(err,"Unable to delete object %v", path)
		}
	}
	return nil
}

func getSwiftCredentials() map[string]string {
	credentials := make(map[string]string)
	userName := os.Getenv("OS_USERNAME")
	password := os.Getenv("OS_PASSWORD")
	authURL := os.Getenv("OS_AUTH_URL")

	//swiftKey := os.Getenv("SWIFT_API_KEY")
	//swiftUser := os.Getenv("SWIFT_API_USER")
	//swiftAuthURL := os.Getenv("SWIFT_AUTH_URL")

	region := os.Getenv("OS_REGION_NAME")
	tenantName := os.Getenv("OS_TENANT_NAME")
	tenantID := os.Getenv("OS_TENANT_ID")
	if userName != "" && password != "" && authURL != ""{
		credentials["authType"] = "v1"
		credentials["exists"] = "yes"
	}

	if tenantID != "" || tenantName != ""{
		credentials["authType"] = "v2"
	}
	credentials["user"] = userName
	credentials["key"] = password
	credentials["authURL"]= authURL
	credentials["region"] = region
	credentials["tenantID"] = tenantID
	credentials["tenantName"] = tenantName

	return credentials
}
