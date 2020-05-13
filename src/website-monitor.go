package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

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

	// log.Println("site status code ", resp.StatusCode)
	if resp.StatusCode != site.StatusCode {
		return fmt.Errorf("Response statusCode %d unmatch expected %d", site.StatusCode, resp.StatusCode)
	}

	return nil
}

func mon(site SiteCfg) {
	err := monitorSite(site)
	if err != nil {
		errMsg := fmt.Sprintf("site monitoring %s error: %v", site.URL, err)
		title := fmt.Sprintf("persisted kafkaesque.io %s endpoint failure", site.Name)
		Alert(errMsg)
		ReportIncident(site.Name, site.Name, title, errMsg, &site.AlertPolicy)
	}
}

// MonitorSites monitors a list of sites
func MonitorSites() {
	sites := GetConfig().SitesConfig.Sites
	log.Println(sites)

	for _, site := range sites {
		log.Println(site.URL)
		go func(s SiteCfg) {
			interval := TimeDuration(s.IntervalSeconds, 120, time.Second)
			mon(s)
			for {
				select {
				case <-time.Tick(interval):
					mon(s)
				}
			}
		}(site)
	}
}
