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

func main() {

	flag.Parse()
	effectiveCfgFile := AssignString(os.Getenv("PULSAR_OPS_MONITOR_CFG"), *cfgFile)
	log.Println("config file ", effectiveCfgFile)
	ReadConfigFile(effectiveCfgFile)

	exit := make(chan bool)
	cfg := GetConfig()

	RunInterval(PulsarFunctions, TimeDuration(cfg.PulsarPerfConfig.IntervalSeconds, 300, time.Second))
	RunInterval(PulsarTenants, TimeDuration(cfg.PulsarOpsConfig.IntervalSeconds, 120, time.Second))
	RunInterval(StartHeartBeat, TimeDuration(cfg.OpsGenieConfig.IntervalSeconds, 240, time.Second))
	RunInterval(MeasureLatency, TimeDuration(cfg.PulsarPerfConfig.IntervalSeconds, 300, time.Second))
	MonitorSites()

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
