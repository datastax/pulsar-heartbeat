package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
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
	if err != nil {
		return 0, err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

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
	log.Println(clusters)

	for _, cluster := range clusters {
		clusterURL := "https://kafkaesque.io/api/v1/" + cluster + "/tenants/"
		tenantSize, err := PulsarAdminTenant(clusterURL, token)
		if err != nil {
			Alert("failed to contact " + cluster)
		} else if tenantSize == 0 {
			Alert(cluster + " tenant size is 0")
		}
	}
}
