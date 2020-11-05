package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/apex/log"
	"github.com/google/gops/agent"
	"github.com/kafkaesque-io/pulsar-monitor/src/cfg"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	cfgFile = flag.String("config", "../config/runtime.yml", "config file for monitoring")
)

type complete struct{} //emptry struct as a single for channel

func main() {
	// runtime.GOMAXPROCS does not the container's CPU quota in Kubernetes
	// therefore, it requires to be set explicitly
	runtime.GOMAXPROCS(util.StrToInt(os.Getenv("GOMAXPROCS"), 1))

	// gops debug instrument
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(fmt.Sprintf("gops instrument error %v", err))
	}

	flag.Parse()
	effectiveCfgFile := util.AssignString(os.Getenv("PULSAR_OPS_MONITOR_CFG"), *cfgFile)
	log.Infof("config file %s", effectiveCfgFile)
	cfg.ReadConfigFile(effectiveCfgFile)

	exit := make(chan *complete)
	config := cfg.GetConfig()

	cfg.SetupAnalytics()

	cfg.AnalyticsAppStart(util.AssignString(config.Name, "dev"))
	cfg.MonitorK8sPulsarCluster()
	cfg.MonitorBrokers()
	cfg.RunInterval(cfg.PulsarTenants, util.TimeDuration(config.PulsarAdminConfig.IntervalSeconds, 120, time.Second))
	cfg.RunInterval(cfg.StartHeartBeat, util.TimeDuration(config.OpsGenieConfig.IntervalSeconds, 240, time.Second))
	cfg.RunInterval(cfg.UptimeHeartBeat, 30*time.Second) // fixed 30 seconds for heartbeat
	cfg.MonitorSites()
	cfg.TopicLatencyTestThread()
	cfg.WebSocketTopicLatencyTestThread()
	cfg.PushToPrometheusProxyThread()
	// Disable tenant usage metering, this is not a monitoring function
	// BuildTenantsUsageThread()

	if config.PrometheusConfig.ExposeMetrics {
		log.Infof("start to listen to http port %s", config.PrometheusConfig.Port)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(util.AssignString(config.PrometheusConfig.Port, ":8089"), nil)
	}
	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
