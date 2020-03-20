package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	netHTTP "net/http"
)

func prometheusHandler() {
	if len(config.MonitorAddr) > 0 {

		info.Printf("COW %s monitor listen http %s\n", version, config.MonitorAddr)

		netHTTP.Handle("/metrics", promhttp.Handler())
		Fatal(netHTTP.ListenAndServe(config.MonitorAddr, nil))
	}
}