# Operation Monitoring for Pulsar

This is a ops monitoring tool for
- monitoring Pulsar admin tenants endpoint
- measure message latency from producing to consuming
- report and monitor heartbeat with OpsGenie
- alert on Slack

This is a data driven tool. The configuraion data is at ./config/runtime.json

## Docker
The runtime.json file must be mounted as /app/runtime.json

This runs a multi stage build that produces a 18MB docker image.
```
$ sudo docker build -t pulsar-ops-monitor .
```

Run docker container with Pulsar CA certificate and expose Prometheus metrics for collection.

``` bash
$ sudo docker run -d -it -v /home/ming/go/src/gitlab.com/operation-monitor/config/runtime.json:/config/runtime.json -v /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem:/etc/ssl/certs/ca-bundle.crt -p 8080:8080 --name=pulsar-monitor pulsar-ops-monitor
```

## Prometheus
This program exposes a Prometheus `\metrics` endpoint to allow measured Pulsar latency to be collected by Prometheus.
