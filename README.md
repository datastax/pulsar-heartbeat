[![Go Report Card](https://goreportcard.com/badge/github.com/datastax/pulsar-heartbeat)](https://goreportcard.com/report/github.com/datastax/pulsar-heartbeat)
[![CI Build](https://github.com/datastax/pulsar-heartbeat/workflows/ci/badge.svg
)](https://github.com/datastax/pulsar-heartbeat/actions)
[![codecov](https://codecov.io/gh/datastax/pulsar-heartbeat/branch/master/graph/badge.svg)](https://codecov.io/gh/datastax/pulsar-heartbeat)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Docker image](https://img.shields.io/docker/image-size/datastax/pulsar-heartbeat)](https://hub.docker.com/r/datastax/pulsar-heartbeat/)
[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/datastax/pulsar-heartbeat/blob/master/LICENSE)

# Operation Monitoring for Pulsar Cluster
Pulsar Heartbeat monitors Pulsar cluster availability, tracks latency of Pulsar message pubsub, and reports failures of the Pulsar cluster. It produces synthetic workloads to measure end-to-end message pubsub latency.

It is a cloud native application that can be installed by Helm within the Pulsar Kubernetes cluster.

Here is a list of features that Pulsar Heartbeat supports.
- [x] monitor Pulsar admin REST API endpoint
- [x] measure end-to-end message latency from producing to consuming messages
- [x] for latency measure, it can produce a list of messages with user specified payload size and the number of messages
- [x] measure average latency over a list of messages
- [x] detect out of order delivery of a list of generated messages
- [x] measure a single message latency over the websocket interface
- [x] measure message latency generated by Pulsar function
- [x] monitor instance availability of broker, proxy, bookkeeper, and zookeeper in a Pulsar Kubernetes cluster
- [x] monitor individual Pulsar broker's health
- [ ] Pulsar function trigger over HTTP interface
- [x] incident alert with OpsGenie with automatic alert clear and deduplication
- [x] customer configurable alert threshold and probe test interval
- [x] tracking analytics and usage
- [x] dead man's snitch heartbeat monitor with OpsGenie
- [x] alert on Slack
- [x] monitor multiple Pulsar clusters (with no kubernetes pods monitoring)
- [x] co-resident monitoring within the same Pulsar Kubernetes cluster

This is a data driven tool that sources configuration from a yaml or json file. Here is a [template](config/runtime_template.yml).
The configuration json file can be specified in the overwrite order of
- an environment variable `PULSAR_OPS_MONITOR_CFG`
- an command line argument `./pulsar-heartbeat -config /path/to/pulsar_ops_monitor_config.yml`
- A default path to `../config/runtime.yml`

## Observability
This tool exposes Prometheus compliant metrics at `\metrics` endpoint for scraping. The exported metrics are:

| Name | Type | Description |
|:------|:------:|:------------|
| pulsar_pubsub_latency_ms | gauge | end to end message pub and sub latency in milliseconds |
| pulsar_pubsub_latency_ms_hst | summary | end to end message latency histogram summary over 50%, 90%, and 99% samples |
| pulsar_websocket_latency_ms | gauge | end to end message pub and sub latency over websocket interface in milliseconds |
| pulsar_k8s_bookkeeper_offline_counter | gauge | bookkeeper offline instances in Kubernetes cluster |
| pulsar_k8s_broker_offline_counter | gauge | broker offline instances in the Kubernetes cluster |
| pulsar_k8s_proxy_offline_counter | gauge | proxy offline instances in the Kubernetes cluster |
| pulsar_k8s_bookkeeper_zookeeper_counter | gauge | zookeeper offline instances in the Kubernetes cluster |
| pulsar_monitor_counter | counter | the total number of heartbeats counter |
| pulsar_tenant_size | gauge | the number of tenants that can be used as a health indicator of admin interface |

## In-cluster monitoring
Pulsar heartbeat can be deployed within the same Pulsar Kubernetes cluster. Kubernetes monitoring and individual broker monitoring are only supported within the same Pulsar Kubernetes cluster deployment.


## Docker
Pulsar Heartbeat's official docker image can be pulled [here](https://hub.docker.com/repository/docker/datastax/pulsar-heartbeat)

### Docker compose
``` bash
$ docker-compose up
```

### Docker example
The runtime.yml/yaml or runtime.json file must be mounted to /config/runtime.yml as the default configuration path.

Run docker container that exposes Prometheus metrics for collection.

``` bash
$ docker run -d -it -v  $HOME/go/src/github.com/datastax/pulsar-heartbeat/config/runtime-astra.yml:/config/runtime.yml -p 8080:8080 --name=pulsar-heartbeat datastax/pulsar-heartbeat:latest
```

## Helm chart

For the following commands, Helm version 3 is supported.

### Install as part of Pulsar cluster using helm
Pulsar Heartbeat can be installed as part of Pulsar cluster in this [Helm chart](https://github.com/datastax/pulsar-helm-chart/blob/master/helm-chart-sources/pulsar/values.yaml#L273). 

### Install as part of DataStax Pulsar cluster using Helm

Pulsar Heartbeat can be directly enabled inside [the DataStax Pulsar chart](https://github.com/datastax/pulsar-helm-chart/blob/master/helm-chart-sources/pulsar/values.yaml#L1571).


## Development

### How to build
This script builds the Pulsar Heartbeat Go application, runs code static analysis(golint), runs unit tests, and creates a binary under ./bin/pulsar-heartbeat.
```
$ ./scripts/ci.sh
```

This command runs a multi stage build to produce a docker image.
```
$ make
```

