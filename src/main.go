package main

import (
	"flag"
	"log"
	"os"
	"time"
)

var cfgFile = flag.String("config", "../config/runtime.json", "config file for monitoring")

func main() {

	flag.Parse()
	log.Println("config file ", *cfgFile)
	ReadConfigFile(*cfgFile)

	exit := make(chan bool)

	RunInterval(PulsarTenants, TimeDuration(GetConfig().PulsarOpsConfig.IntervalSeconds, 120, time.Second))
	RunInterval(StartHeartBeat, TimeDuration(GetConfig().OpsGenieConfig.IntervalSeconds, 240, time.Second))
	RunInterval(MeasureLatency, TimeDuration(GetConfig().PulsarPerfConfig.IntervalSeconds, 300, time.Second))

	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
