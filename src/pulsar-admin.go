package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// PulsarAdminTenant probes the tenant endpoint to get a list of tenants
// returns the number of tenants on the cluster
func PulsarAdminTenant(clusterURL, token string) (int, error) {

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(10) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	req, err := retryablehttp.NewRequest(http.MethodGet, clusterURL, nil)
	if err != nil {
		return 0, err
	}

	req.Header.Add("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return 0, err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var tenants []string

	err = json.Unmarshal(bodyBytes, &tenants)
	if err != nil {
		return 0, err
	}

	return len(tenants), nil
}

// PulsarTenants get a list of tenants on each cluster
func PulsarTenants() {
	clusters := GetConfig().PulsarOpsConfig.Clusters
	token := GetConfig().PulsarOpsConfig.MasterToken

	for _, cluster := range clusters {
		clusterURL := "https://kafkaesque.io/api/v1/" + cluster.Name + "/tenants/"
		tenantSize, err := PulsarAdminTenant(clusterURL, token)
		if err != nil {
			errMsg := fmt.Sprintf("tenant-test failed on cluster %s error: %v", cluster.Name, err)
			Alert(errMsg)
			ReportIncident(cluster.Name, "persisted cluster tenants test failure", errMsg, &cluster.AlertPolicy)
		} else {
			PromGaugeInt(TenantsGaugeOpt(), cluster.Name, tenantSize)
			ClearIncident(cluster.Name)
			if tenantSize == 0 {
				Alert(fmt.Sprintf("%s has incorrect number of tenants 0", cluster.Name))
			}
		}
	}
}

// TODO: add broker stats "https://kafkaesque.io/api/v1/" + cluster + "/broker-stats/load-report/"
