package cfg

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// StartHeartBeat starts heartbeat monitoring the program by OpsGenie
func StartHeartBeat() {
	// opsgenie url in the format of "https://api.opsgenie.com/v2/heartbeats/<component>/ping"
	genieURL := GetConfig().OpsGenieConfig.HeartBeatURL
	genieKey := GetConfig().OpsGenieConfig.HeartbeatKey
	if genieURL != "" {
		err := HeartBeatToOpsGenie(genieURL, genieKey)
		if err != nil {
			Alert(fmt.Sprintf("OpsGenie error %v", err))
		}
	}
}

// UptimeHeartBeat sends heartbeat to uptime counter
func UptimeHeartBeat() {
	AnalyticsHeartbeat(GetConfig().Name)
	PromCounter(HeartbeatCounterOpt(), GetConfig().Name)
}

// HeartBeatToOpsGenie send heart beat to ops genie
func HeartBeatToOpsGenie(genieURL, genieKey string) error {

	name := GetConfig().Name

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	req, err := retryablehttp.NewRequest(http.MethodGet, genieURL, nil)
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
		Alert(fmt.Sprintf("from %s Opsgenie returns error %v", name, err))
		return err
	}

	log.Print("opsgenie status code ", resp.StatusCode)
	if resp.StatusCode > 300 {
		msg := fmt.Sprintf("from %s Opsgenie returns incorrect status code %d", name, resp.StatusCode)
		Alert(msg)
		return errors.New(msg)
	}

	return nil
}
