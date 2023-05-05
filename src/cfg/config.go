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
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"time"
	"unicode"

	"golang.org/x/oauth2/clientcredentials"

	"github.com/datastax/pulsar-heartbeat/src/util"

	"github.com/apex/log"
	"github.com/ghodss/yaml"
)

// PrometheusCfg configures Premetheus set up
type PrometheusCfg struct {
	Port                  string `json:"port"`
	ExposeMetrics         bool   `json:"exposeMetrics"`
	PrometheusProxyURL    string `json:"prometheusProxyURL"`
	PrometheusProxyAPIKey string `json:"prometheusProxyAPIKey"`
}

// SlackCfg is slack configuration
type SlackCfg struct {
	AlertURL string `json:"alertUrl"`
	Verbose  bool   `json:"verbose"`
}

// OpsGenieCfg is opsGenie configuration
type OpsGenieCfg struct {
	HeartBeatURL    string `json:"heartbeatUrl"`
	HeartbeatKey    string `json:"heartbeatKey"`
	AlertKey        string `json:"alertKey"`
	IntervalSeconds int    `json:"intervalSeconds"`
}

// PagerDutyCfg is opsGenie configuration
type PagerDutyCfg struct {
	IntegrationKey string `json:"integrationKey"`
}

// AnalyticsCfg is analytics usage and statistucs tracking configuration
type AnalyticsCfg struct {
	APIKey            string `json:"apiKey"`
	IngestionURL      string `json:"ingestionUrl"`
	InsightsWriteKey  string `json:"insightsWriteKey"`
	InsightsAccountID string `json:"insightsAccountId"`
}

// SiteCfg configures general website
type SiteCfg struct {
	Headers         map[string]string `json:"headers"`
	URL             string            `json:"url"`
	Name            string            `json:"name"`
	IntervalSeconds int               `json:"intervalSeconds"`
	ResponseSeconds int               `json:"responseSeconds"`
	StatusCode      int               `json:"statusCode"`
	StatusCodeExpr  string            `json:"statusCodeExpr"`
	Retries         int               `json:"retries"`
	AlertPolicy     AlertPolicyCfg    `json:"alertPolicy"`
}

// SitesCfg configures a list of website`
type SitesCfg struct {
	Sites []SiteCfg `json:"sites"`
}

// OpsClusterCfg is each cluster's configuration
type OpsClusterCfg struct {
	Name        string         `json:"name"`
	URL         string         `json:"url"`
	AlertPolicy AlertPolicyCfg `json:"alertPolicy"`
}

// PulsarAdminRESTCfg is for monitor a list of Pulsar cluster
type PulsarAdminRESTCfg struct {
	Token           string          `json:"Token"`
	Clusters        []OpsClusterCfg `json:"clusters"`
	IntervalSeconds int             `json:"intervalSeconds"`
}

// TopicCfg is topic configuration
type TopicCfg struct {
	Name                    string         `json:"name"`
	ClusterName             string         `json:"clusterName"` // used for broker monitoring if specified
	Token                   string         `json:"token"`
	TrustStore              string         `json:"trustStore"`
	NumberOfPartitions      int            `json:"numberOfPartitions"`
	LatencyBudgetMs         int            `json:"latencyBudgetMs"`
	PulsarURL               string         `json:"pulsarUrl"`
	AdminURL                string         `json:"adminUrl"`
	TopicName               string         `json:"topicName"`
	OutputTopic             string         `json:"outputTopic"`
	IntervalSeconds         int            `json:"intervalSeconds"`
	ExpectedMsg             string         `json:"expectedMsg"`
	PayloadSizes            []string       `json:"payloadSizes"`
	NumOfMessages           int            `json:"numberOfMessages"`
	AlertPolicy             AlertPolicyCfg `json:"AlertPolicy"`
	DowntimeTrackerDisabled bool           `json:"downtimeTrackerDisabled"`
}

// WsConfig is configuration to monitor WebSocket pub sub latency
type WsConfig struct {
	Name            string         `json:"name"`
	Token           string         `json:"token"`
	Cluster         string         `json:"cluster"` // can be used for alert de-dupe
	LatencyBudgetMs int            `json:"latencyBudgetMs"`
	ProducerURL     string         `json:"producerUrl"`
	ConsumerURL     string         `json:"consumerUrl"`
	TopicName       string         `json:"topicName"`
	IntervalSeconds int            `json:"intervalSeconds"`
	Scheme          string         `json:"scheme"`
	Port            string         `json:"port"`
	Subscription    string         `json:"subscription"`
	URLQueryParams  string         `json:"urlQueryParams"`
	AlertPolicy     AlertPolicyCfg `json:"AlertPolicy"`
}

// K8sClusterCfg is configuration to monitor kubernete cluster
// only to be enabled in-cluster monitoring
type K8sClusterCfg struct {
	Enabled         bool           `json:"enabled"`
	PulsarNamespace string         `json:"pulsarNamespace"`
	KubeConfigDir   string         `json:"kubeConfigDir"`
	AlertPolicy     AlertPolicyCfg `json:"AlertPolicy"`
}

// BrokersCfg monitors all brokers in the cluster
type BrokersCfg struct {
	BrokerTestRequired bool           `json:"brokerTestRequired"`
	InClusterRESTURL   string         `json:"inclusterRestURL"`
	IntervalSeconds    int            `json:"intervalSeconds"`
	AlertPolicy        AlertPolicyCfg `json:"AlertPolicy"`
}

// TenantUsageCfg tenant usage reporting and monitoring
type TenantUsageCfg struct {
	OutBytesLimit        uint64 `json:"outBytesLimit"`
	AlertIntervalMinutes int    `json:"alertIntervalMinutes"`
}

// Configuration - this server's configuration
type Configuration struct {
	// Name is the Pulsar cluster name, it is mandatory
	Name string `json:"name"`
	// ClusterName is the Pulsar cluster name if the Name cannot be used as the Pulsar cluster name, optional
	ClusterName      string                    `json:"clusterName"`
	TokenOAuthConfig *clientcredentials.Config `json:"tokenOAuthConfig"`
	// TokenFilePath is the file path to Pulsar JWT. It takes precedence of the token attribute.
	TokenFilePath string `json:"tokenFilePath"`
	// Token is a Pulsar JWT can be used for both client or http admin client
	Token             string             `json:"token"`
	BrokersConfig     BrokersCfg         `json:"brokersConfig"`
	TrustStore        string             `json:"trustStore"`
	K8sConfig         K8sClusterCfg      `json:"k8sConfig"`
	AnalyticsConfig   AnalyticsCfg       `json:"analyticsConfig"`
	PrometheusConfig  PrometheusCfg      `json:"prometheusConfig"`
	SlackConfig       SlackCfg           `json:"slackConfig"`
	OpsGenieConfig    OpsGenieCfg        `json:"opsGenieConfig"`
	PagerDutyConfig   PagerDutyCfg       `json:"pagerDutyConfig"`
	PulsarAdminConfig PulsarAdminRESTCfg `json:"pulsarAdminRestConfig"`
	PulsarTopicConfig []TopicCfg         `json:"pulsarTopicConfig"`
	SitesConfig       SitesCfg           `json:"sitesConfig"`
	WebSocketConfig   []WsConfig         `json:"webSocketConfig"`
	TenantUsageConfig TenantUsageCfg     `json:"tenantUsageConfig"`

	tokenFunc func() (string, error)
}

func (c *Configuration) Init() {
	if len(c.Name) < 1 {
		panic("a valid `name` in Configuration must be specified")
	}

	if c.TokenOAuthConfig != nil {
		tokenSrc := c.TokenOAuthConfig.TokenSource(context.Background())
		c.tokenFunc = func() (string, error) {
			ot, err := tokenSrc.Token()
			if err != nil {
				return "", err
			}
			return ot.AccessToken, nil
		}
	} else if len(c.TokenFilePath) > 1 {
		// In the case of Kubernetes, the token file can be updated, so this reads it from the file every time.
		c.tokenFunc = func() (string, error) {
			tokenBytes, err := ioutil.ReadFile(c.TokenFilePath)
			if err != nil {
				return "", err
			}
			return string(tokenBytes), nil
		}
	} else if c.Token != "" {
		c.Token = strings.TrimSuffix(os.Getenv("PulsarToken"), "\n")
		c.tokenFunc = func() (string, error) {
			return c.Token, nil
		}
	}
}

func (c *Configuration) TokenSupplier() func() (string, error) {
	return c.tokenFunc
}

// AlertPolicyCfg is a set of criteria to evaluation triggers for incident alert
type AlertPolicyCfg struct {
	// first evaluation to count continuous failure
	Ceiling int `json:"ceiling"`
	// Second evaluation for moving window
	MovingWindowSeconds   int `json:"movingWindowSeconds"`
	CeilingInMovingWindow int `json:"ceilingInMovingWindow"`
}

// Config - this server's configuration instance
var Config Configuration

// ReadConfigFile reads configuration file.
func ReadConfigFile(configFile string) {

	fileBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Errorf("failed to load configuration file %s", configFile)
		panic(err)
	}

	if hasJSONPrefix(fileBytes) {
		err = json.Unmarshal(fileBytes, &Config)
		if err != nil {
			panic(err)
		}
	} else {
		err = yaml.Unmarshal(fileBytes, &Config)
		if err != nil {
			panic(err)
		}
	}
	Config.Init()
	log.Infof("config %v", Config)
}

var jsonPrefix = []byte("{")

func hasJSONPrefix(buf []byte) bool {
	return hasPrefix(buf, jsonPrefix)
}

// Return true if the first non-whitespace bytes in buf is prefix.
func hasPrefix(buf []byte, prefix []byte) bool {
	trim := bytes.TrimLeftFunc(buf, unicode.IsSpace)
	return bytes.HasPrefix(trim, prefix)
}

// GetConfig returns a reference to the Configuration
func GetConfig() *Configuration {
	return &Config
}

type monitorFunc func()

// RunInterval runs interval
func RunInterval(fn monitorFunc, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		fn()
		for {
			select {
			case <-ticker.C:
				fn()
			}
		}

	}()
}
