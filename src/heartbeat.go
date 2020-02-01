package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

var heatbeatDuration = 60 * time.Second

// StartHeartBeat starts heartbeat monitoring the program by OpsGenie
func StartHeartBeat() {
	genieKey := GetConfig().OpsGenieConfig.Key
	err := HeartBeatToOpsGenie(genieKey)
	if err != nil {
		Alert(fmt.Sprintf("OpsGenie error %v", err))
	}
}

// HeartBeatToOpsGenie send heart beat to ops genie
func HeartBeatToOpsGenie(genieKey string) error {

	opsGenieURL := "https://api.opsgenie.com/v2/heartbeats/latency-monitor/ping"
	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	req, err := retryablehttp.NewRequest(http.MethodGet, opsGenieURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", genieKey)

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Println(err)
		Alert(fmt.Sprintf("Opsgenie returns error %v", err))
		return err
	}

	log.Print("opsgenie status code ", resp.StatusCode)
	if resp.StatusCode > 300 {
		msg := fmt.Sprintf("Opsgenie returns incorrect status code %d", resp.StatusCode)
		Alert(msg)
		return errors.New(msg)
	}

	return nil
}
