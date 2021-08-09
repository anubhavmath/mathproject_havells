package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"../../../../modules/message"
	"../../../../modules/mlclient"
	"../../../../vsystemclient"
)

func Setup() {
	gVsystemInfo.Endpoint = vsystemclient.GetvSystemEndpoint()
	Logf("%s: %s: using vSystemEndpoint: %q", getOperatorName(), "Setup", gVsystemInfo.Endpoint)
	if err := CheckMandatoryUIParameter(); nil != err {
		ProcessErrorSetup("MandatoryParameters", err)
		return
	}
}

func InArtifact(msg interface{}) {
	Logf("%s: incoming Message...", getOperatorName())
	artifactID, err := ExtractArtifactIDFromMessage(msg)
	if nil != err {
		ProcessErrorInArtifact("incoming message", err)
		return
	}
	Logf("%s: found artifactID: %s", getOperatorName(), artifactID)

	connectionID, path, err := ReceiveArtifactInformation(artifactID)
	if nil != err {
		ProcessErrorInArtifact("ML-API", err)
		return
	}
	Logf("%s: found protocol: %s and filePath: %s", getOperatorName(), connectionID, path)

	fileMap := AppendFileInformationToMessage(msg, connectionID, path)
	Logf("%s: sends: %v", getOperatorName(), fileMap)

	if OutArtifact != nil {
		OutArtifact(msg)
	}
}
func ExtractArtifactIDFromMessage(msg interface{}) (string, error) {
	artifact, err := message.GetArtifactAttributes(msg)
	if nil != err {
		return "", err
	}
	artifactID, err := GetArtifactID(artifact)
	if nil != err {
		return "", err
	}
	return artifactID, nil
}

func ReceiveArtifactInformation(inArtifactID string) (string, string, error) {
	inAPIVersion := "v1"
	artifactsEndpoint := mlclient.CreateArtifactEndpoint(gVsystemInfo.Endpoint, inAPIVersion, fmt.Sprintf("artifacts/%s", inArtifactID))

	m, err := mlclient.GetArifactInformation(artifactsEndpoint, getTimeout())
	if nil != err {
		return "", "", err
	}
	protocol, URI, err := GetPath(m.URI)
	if nil != err {
		return "", "", err
	}
	Logf("%s: protocol: %s, filePath: %s", getOperatorName(), protocol, URI)

	connectionID, path := SplitConnectionIDAndPath(URI)
	return connectionID, path, nil
}

func AppendFileInformationToMessage(msg interface{}, connectionID string, path string) interface{} {
	fileMap := CreateFileMessage(connectionID, path)
	// tested before via GetArtifactAttributes
	attributes, _ := message.GetAttributes(msg)
	attributes["file"] = fileMap
	return fileMap
}

func CreateFileMessage(connectionID string, path string) interface{} {
	connection := make(map[string]interface{}, 2)
	connection["configurationType"] = "Connection Management"
	connection["connectionID"] = connectionID

	file := make(map[string]interface{}, 2)
	file["connection"] = connection
	file["path"] = path

	return file
}

func GetArtifactID(artifactMap map[string]interface{}) (string, error) {
	artifactID, ok := artifactMap["id"].(string)
	if !ok {
		return "", errors.New("artifactID is not a string")
	}
	if len(artifactID) == 0 {
		return "", errors.New("artifactID is empty")
	}
	return artifactID, nil
}

func GetPath(uri string) (protocol string, path string, err error) {
	if strings.Contains(uri, "://") {
		token := strings.Split(uri, "://")
		if strings.Contains(token[0], "dh-dl") {
			path = token[1]
			protocol = token[0]
		} else {
			return "", "", fmt.Errorf("unspecified protocol %q", token[0])
		}
	} else {
		path = uri
	}
	return protocol, path, nil
}

func SplitConnectionIDAndPath(uri string) (string, string) {
	token := strings.Split(uri, "/")
	path := strings.Replace(uri, token[0], "", 1)
	return token[0], path
}

func CheckMandatoryUIParameter() error {
	const currentAPIVersion = "v1"

	var value string
	if err := CheckMandatoryParameter(&value, "apiVersion", GetString); nil != err {
		return err
	}
	if value != currentAPIVersion {
		return fmt.Errorf("apiVersion should be %q, got %q", currentAPIVersion, value)
	}
	return nil
}

func CheckMandatoryParameter(mandatoryValue *string, key string, getFunc func(string) string) error {
	value := getFunc(key)
	if len(value) == 0 {
		return fmt.Errorf("mandatory parameter %q is not set", key)
	}
	*mandatoryValue = value
	return nil
}

func getOperatorName() string {
	return "ArtifactConsumer"
}

func getTimeout() time.Duration {
	return 15 * time.Second
}

func getOperatorConfig() *vsystemclient.OperatorConfig {
	return &vsystemclient.OperatorConfig{
		OperatorName: getOperatorName(),
		Operation:    "artifact consumption",
		OperatorPath: "com.sap.ml.artifact.consumer.v2",
	}
}

func ProcessErrorSetup(operation string, err error) {
	config := getOperatorConfig()
	config.Operation = operation
	vsystemclient.ProcessError(err, Errorf, OutError, *config, GetString("processName"), "Setup")
}

func ProcessErrorInArtifact(operation string, err error) {
	config := getOperatorConfig()
	config.Operation = operation
	vsystemclient.ProcessError(err, Errorf, OutError, *config, GetString("processName"), "InArtifact")
}

var (
	gVsystemInfo vsystemclient.VSystemInfo

	Logf      func(string, ...interface{})
	Errorf    func(string, ...interface{})
	GetString func(string) string

	OutArtifact func(interface{})
	OutError    func(interface{})
)
