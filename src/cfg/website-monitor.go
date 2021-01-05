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
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/antonmedv/expr"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"github.com/hashicorp/go-retryablehttp"
)

func monitorSite(site SiteCfg) error {

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(site.ResponseSeconds) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = site.Retries

	req, err := retryablehttp.NewRequest(http.MethodGet, site.URL, nil)
	if err != nil {
		return err
	}

	for k, v := range site.Headers {
		req.Header.Add(k, v)
	}

	sentTime := time.Now()
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}
	PromLatencySum(SiteLatencyGaugeOpt(), site.Name, time.Now().Sub(sentTime))

	if site.StatusCode > 0 && resp.StatusCode != site.StatusCode {
		return fmt.Errorf("Response statusCode %d does not match the expected code %d", resp.StatusCode, site.StatusCode)
	}

	if site.StatusCodeExpr != "" {
		// the literal statusCode must be used in the expression for evaluation
		env := map[string]interface{}{
			"statusCode": resp.StatusCode,
		}

		result, err := expr.Eval(site.StatusCodeExpr, env)
		if err != nil {
			return fmt.Errorf("Response code %d does not satisfy expression evaluation %s, error %v",
				resp.StatusCode, site.StatusCodeExpr, err)
		}
		rc, ok := result.(bool)
		if !ok {
			return fmt.Errorf("Response code %d evaluation against %s failed to reach a boolean verdict",
				resp.StatusCode, site.StatusCodeExpr)
		} else if !rc {
			return fmt.Errorf("Response code %d evaluation againt %s failed",
				resp.StatusCode, site.StatusCodeExpr)
		}
	}

	return nil
}

func mon(site SiteCfg) {
	err := monitorSite(site)
	if err != nil {
		errMsg := fmt.Sprintf("site monitoring %s error: %v", site.URL, err)
		title := fmt.Sprintf("persisted %s endpoint failure", site.Name)
		VerboseAlert(site.Name+"-site-monitor", errMsg, 3*time.Minute)
		ReportIncident(site.Name, site.Name, title, errMsg, &site.AlertPolicy)
	} else {
		ClearIncident(site.Name)
	}
}

// MonitorSites monitors a list of sites
func MonitorSites() {
	sites := GetConfig().SitesConfig.Sites
	log.Println(sites)

	for _, site := range sites {
		log.Println(site.URL)
		go func(s SiteCfg) {
			interval := util.TimeDuration(s.IntervalSeconds, 120, time.Second)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			mon(s)
			for {
				select {
				case <-ticker.C:
					mon(s)
				}
			}
		}(site)
	}
}
