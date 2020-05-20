package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"time"
)

// monitor pulsar function trigger and verify successful response

// PulsarFunction triggers a function and verify the response
func PulsarFunction(clusterName, clusterURL, token, testStr string) error {

	buffer := new(bytes.Buffer)

	writer := multipart.NewWriter(buffer)
	dataField, err := writer.CreateFormField("data")
	if err != nil {
		return err
	}
	dataField.Write([]byte(testStr))

	writer.Close()
	req, err := http.NewRequest(http.MethodPost, clusterURL, buffer)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", writer.FormDataContentType())
	req.Header.Add("Expect", "100-continue")

	client := &http.Client{
		Timeout: 5 * time.Second, //because the function also times out at 5 seconds by default
	}

	sentTime := time.Now()
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}
	PromLatencySum(FuncLatencyGaugeOpt(), clusterName, time.Now().Sub(sentTime))

	if resp.StatusCode != 200 {
		return fmt.Errorf("failure statusCode %d", resp.StatusCode)
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	expected := fmt.Sprintf("%s!", testStr)
	if expected != string(bodyBytes) {
		return fmt.Errorf("received %s but expected %s", string(bodyBytes), expected)
	}

	return nil
}

// PulsarFunctions get a list of tenants on each cluster
func PulsarFunctions() {
	cfg := GetConfig()
	clusters := cfg.PulsarFunctionsConfig.Clusters
	token := AssignString(Trim(cfg.PulsarFunctionsConfig.MasterToken), Trim(cfg.PulsarOpsConfig.MasterToken))

	for _, cluster := range clusters {
		name := Trim(cluster.Name)
		clusterURL := Trim(cluster.TriggerURL)
		log.Printf("trigger function %s\n", clusterURL)
		err := PulsarFunction(name, clusterURL, token, "flyingmama")
		if err != nil {
			errMsg := fmt.Sprintf("trigger-function failed on cluster %s error: %v", name, err)
			Alert(errMsg)
			ReportIncident(name, name, "persisted trigger-function failure", errMsg, &cluster.AlertPolicy, false)
		} else {
			ClearIncident(name)
		}
	}
}
