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

	RunInterval(PulsarTenants, 60*time.Second)
	RunInterval(StartHeartBeat, 150*time.Second)
	RunInterval(MeasureLatency, 90*time.Second)

	for {
		select {
		case <-exit:
			os.Exit(2)
		}
	}
}
