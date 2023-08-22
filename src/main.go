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

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/cfg"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"github.com/google/gops/agent"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	cfgFile = flag.String("config", "../config/runtime.yml", "config file for monitoring")
)

func main() {
	// runtime.GOMAXPROCS does not the container's CPU quota in Kubernetes
	// therefore, it requires to be set explicitly
	runtime.GOMAXPROCS(util.StrToInt(os.Getenv("GOMAXPROCS"), 1))

	// gops debug instrument
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(fmt.Sprintf("gops instrument error %v", err))
	}

	flag.Parse()
	effectiveCfgFile := util.FirstNonEmptyString(os.Getenv("PULSAR_OPS_MONITOR_CFG"), *cfgFile)
	log.Infof("config file %s", effectiveCfgFile)
	cfg.ReadConfigFile(effectiveCfgFile)

	config := cfg.GetConfig()

	cfg.MonitorK8sPulsarCluster()
	cfg.RunInterval(cfg.PulsarTenants, util.TimeDuration(config.PulsarAdminConfig.IntervalSeconds, 120, time.Second))
	cfg.RunInterval(cfg.StartHeartBeat, util.TimeDuration(config.OpsGenieConfig.IntervalSeconds, 240, time.Second))
	cfg.RunInterval(cfg.UptimeHeartBeat, 30*time.Second) // fixed 30 seconds for heartbeat
	cfg.MonitorSites()
	cfg.TopicLatencyTestThread()
	cfg.WebSocketTopicLatencyTestThread()
	cfg.PushToPrometheusProxyThread()

	if config.PrometheusConfig.ExposeMetrics {
		log.Infof("serving metrics on port %s", config.PrometheusConfig.Port)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(util.FirstNonEmptyString(config.PrometheusConfig.Port, ":8089"), nil)
	}
	exit := make(chan *struct{})
	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
