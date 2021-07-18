//
//  Copyright (c) 2020-2021 Datastax, Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
//

package cfg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	log "github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
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
			log.Errorf(clusterName + "-pulsar-admin " + errMsg)
			ReportIncident(cluster.Name, clusterName, "persisted cluster tenants test failure", errMsg, &cluster.AlertPolicy)
		} else {
			PromGaugeInt(TenantsGaugeOpt(), cluster.Name, tenantSize)
			ClearIncident(cluster.Name)
			if tenantSize == 0 {
				log.Errorf("cluster %s pulsar-admin has incorrect number of tenants 0", cluster.Name)
			} else {
				log.Infof("cluster %s has %d numbers of tenants", clusterName, tenantSize)
			}
		}
	}
}
