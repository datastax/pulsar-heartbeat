package main

import (
	"flag"
	"log"
	"os"
)

var cfgFile = flag.String("config", "../config/runtime.json", "config file for monitoring")

func main() {

	flag.Parse()
	log.Println("config file ", *cfgFile)
	ReadConfigFile(*cfgFile)

	exit := make(chan bool)

	RunInterval(PulsarTenants, Interval(GetConfig().PulsarOpsConfig.IntervalSeconds, 120))
	RunInterval(StartHeartBeat, Interval(GetConfig().OpsGenieConfig.IntervalSeconds, 240))
	RunInterval(MeasureLatency, Interval(GetConfig().PulsarPerfConfig.IntervalSeconds, 300))

	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
