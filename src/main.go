package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	cfgFile = flag.String("config", "../config/runtime.yml", "config file for monitoring")
)

type complete struct{} //emptry struct as a single for channel

func main() {

	flag.Parse()
	effectiveCfgFile := AssignString(os.Getenv("PULSAR_OPS_MONITOR_CFG"), *cfgFile)
	log.Println("config file ", effectiveCfgFile)
	ReadConfigFile(effectiveCfgFile)

	exit := make(chan *complete)
	cfg := GetConfig()

	SetupAnalytics()

	AnalyticsAppStart(AssignString(cfg.Name, "dev"))
	RunInterval(PulsarTenants, TimeDuration(cfg.PulsarAdminConfig.IntervalSeconds, 120, time.Second))
	RunInterval(StartHeartBeat, TimeDuration(cfg.OpsGenieConfig.IntervalSeconds, 240, time.Second))
	RunInterval(UptimeHeartBeat, 30*time.Second)
	MonitorSites()
	TopicLatencyTestThread()
	WebSocketTopicLatencyTestThread()

	if cfg.PrometheusConfig.ExposeMetrics {
		log.Printf("start to listen to http port %s", cfg.PrometheusConfig.Port)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(cfg.PrometheusConfig.Port, nil)
	}
	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
