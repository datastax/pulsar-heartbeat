package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
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
	clusters := GetConfig().PulsarAdminConfig.Clusters
	token := util.AssignString(GetConfig().PulsarAdminConfig.Token, GetConfig().Token)

	for _, cluster := range clusters {
		adminURL, err := url.ParseRequestURI(cluster.URL)
		if err != nil {
			panic(err) //panic because this is a showstopper
		}
		clusterName := adminURL.Hostname()
		queryURL := util.SingleSlashJoin(cluster.URL, "/admin/v2/tenants")
		tenantSize, err := PulsarAdminTenant(queryURL, token)
		if err != nil {
			errMsg := fmt.Sprintf("tenant-test failed on cluster %s error: %v", queryURL, err)
			VerboseAlert(clusterName+"-pulsar-admin", errMsg, 3*time.Minute)
			ReportIncident(cluster.Name, clusterName, "persisted cluster tenants test failure", errMsg, &cluster.AlertPolicy)
		} else {
			PromGaugeInt(TenantsGaugeOpt(), cluster.Name, tenantSize)
			ClearIncident(cluster.Name)
			if tenantSize == 0 {
				VerboseAlert(clusterName+"-pulsar-admin", fmt.Sprintf("%s has incorrect number of tenants 0", cluster.Name), 3*time.Minute)
			} else {
				log.Printf("cluster %s has %d numbers of tenants", clusterName, tenantSize)
			}
		}
	}
}
