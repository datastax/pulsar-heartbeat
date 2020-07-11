package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/google/gops/agent"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	cfgFile = flag.String("config", "../config/runtime.yml", "config file for monitoring")
)

type complete struct{} //emptry struct as a single for channel

var clusterHealth = ClusterHealth{}

func main() {
	// runtime.GOMAXPROCS does not the container's CPU quota in Kubernetes
	// therefore, it requires to be set explicitly
	runtime.GOMAXPROCS(util.StrToInt(os.Getenv("GOMAXPROCS"), 1))

	// gops debug instrument
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Panicf("gops instrument error %v", err)
	}

	flag.Parse()
	effectiveCfgFile := util.AssignString(os.Getenv("PULSAR_OPS_MONITOR_CFG"), *cfgFile)
	log.Println("config file ", effectiveCfgFile)
	ReadConfigFile(effectiveCfgFile)

	exit := make(chan *complete)
	cfg := GetConfig()

	SetupAnalytics()

	AnalyticsAppStart(util.AssignString(cfg.Name, "dev"))
	MonitorK8sPulsarCluster()
	RunInterval(PulsarTenants, util.TimeDuration(cfg.PulsarAdminConfig.IntervalSeconds, 120, time.Second))
	RunInterval(StartHeartBeat, util.TimeDuration(cfg.OpsGenieConfig.IntervalSeconds, 240, time.Second))
	RunInterval(UptimeHeartBeat, 30*time.Second) // fixed 30 seconds for heartbeat
	MonitorSites()
	TopicLatencyTestThread()
	WebSocketTopicLatencyTestThread()
	PushToPrometheusProxyThread()

	if cfg.PrometheusConfig.ExposeMetrics {
		log.Printf("start to listen to http port %s", cfg.PrometheusConfig.Port)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(util.AssignString(cfg.PrometheusConfig.Port, ":8089"), nil)
	}
	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
