package k8s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/apex/log"
	apps_v1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	core_v1 "k8s.io/api/core/v1"
	ext_v1beta1 "k8s.io/api/extensions/v1beta1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	// DefaultPulsarNamespace is the default pulsar namespace in the cluster
	DefaultPulsarNamespace = "pulsar"

	// ZookeeperSts is zookeeper sts name
	ZookeeperSts = "zookeeper"

	// BookkeeperSts is bookkeeper sts name
	BookkeeperSts = "bookkeeper"

	// BrokerDeployment is the broker deployment name
	BrokerDeployment = "broker"

	// BrokerSts is the broker deployment name
	BrokerSts = "brokersts"

	// ProxyDeployment is the proxy deployment name
	ProxyDeployment = "proxy"

	// FunctionWorkerDeployment is the function worker deployment name
	FunctionWorkerDeployment = "functionWorker"
)

// ClusterStatusCode is the high level health of cluster status
type ClusterStatusCode int

const (
	// TotalDown is the initial status
	TotalDown ClusterStatusCode = iota

	// OK is the healthy status
	OK

	// PartialReady is some parts of system are ok
	PartialReady
)

// Client is the k8s client object
type Client struct {
	Clientset        *kubernetes.Clientset
	Metrics          *metrics.Clientset
	ClusterName      string
	DefaultNamespace string
	Status           ClusterStatusCode
	Zookeeper        StatefulSet
	Bookkeeper       StatefulSet
	BrokerSts        StatefulSet
	Broker           Deployment
	Proxy            Deployment
	FunctionWorker   StatefulSet
}

// ClusterStatus is the health status of the cluster and its components
type ClusterStatus struct {
	ZookeeperOfflineInstances  int
	BookkeeperOfflineInstances int
	BrokerOfflineInstances     int
	BrokerStsOfflineInstances  int
	ProxyOfflineInstances      int
	Status                     ClusterStatusCode
}

// Deployment is the k8s deployment
type Deployment struct {
	Name      string
	Replicas  int32
	Instances int32
}

// StatefulSet is the k8s sts
type StatefulSet struct {
	Name      string
	Replicas  int32
	Instances int32
}

// GetK8sClient gets k8s clientset
func GetK8sClient(pulsarNamespace string) (*Client, error) {
	var config *rest.Config

	if home := homedir.HomeDir(); home != "" {
		// TODO: add configuration to allow customized config file
		kubeconfig := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfig); os.IsNotExist(err) {
			log.Infof("this is an in-cluster k8s monitor, pulsar namespace %s", pulsarNamespace)
			if config, err = rest.InClusterConfig(); err != nil {
				return nil, err
			}

		} else {
			log.Infof("this is outside of k8s cluster monitor, kubeconfig dir %s, pulsar namespace %s", kubeconfig, pulsarNamespace)
			if config, err = clientcmd.BuildConfigFromFlags("", kubeconfig); err != nil {
				return nil, err
			}
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	metrics, err := metrics.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	client := Client{
		Clientset: clientset,
		Metrics:   metrics,
	}

	err = client.UpdateReplicas(pulsarNamespace)
	if err != nil {
		return nil, err
	}
	return &client, nil
}

func buildInClusterConfig() kubernetes.Interface {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Can not get kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Can not create kubernetes client: %v", err)
	}

	return clientset
}

// UpdateReplicas updates the replicas for deployments and sts
func (c *Client) UpdateReplicas(namespace string) error {
	brokersts, err := c.getStatefulSets(namespace, BrokerSts)
	if err != nil {
		return err
	}
	if len(brokersts.Items) == 0 {
		c.BrokerSts.Replicas = 0
	} else {
		c.BrokerSts.Replicas = *(brokersts.Items[0]).Spec.Replicas
	}

	broker, err := c.getDeployments(namespace, BrokerDeployment)
	if err != nil {
		return err
	}
	if len(broker.Items) == 0 {
		c.Broker.Replicas = 0
	} else {
		c.Broker.Replicas = *(broker.Items[0]).Spec.Replicas
	}

	proxy, err := c.getDeployments(namespace, ProxyDeployment)
	if err != nil {
		return err
	}
	if len(proxy.Items) == 0 {
		c.Proxy.Replicas = 0
	} else {
		c.Proxy.Replicas = *(proxy.Items[0]).Spec.Replicas
	}

	zk, err := c.getStatefulSets(namespace, ZookeeperSts)
	if err != nil {
		return err
	}
	c.Zookeeper.Replicas = *(zk.Items[0]).Spec.Replicas

	bk, err := c.getStatefulSets(namespace, BookkeeperSts)
	if err != nil {
		return err
	}
	c.Bookkeeper.Replicas = *(bk.Items[0]).Spec.Replicas

	return nil
}

// WatchPods watches the running pods vs intended replicas
func (c *Client) WatchPods(namespace string) error {

	if counts, err := c.runningPodCounts(namespace, "zookeeper"); err == nil {
		c.Zookeeper.Instances = int32(counts)
	} else {
		return err
	}

	if counts, err := c.runningPodCounts(namespace, "bookkeeper"); err == nil {
		c.Bookkeeper.Instances = int32(counts)
	} else {
		return err
	}

	if c.Broker.Replicas > 0 {
		if counts, err := c.runningPodCounts(namespace, "broker"); err == nil {
			c.Broker.Instances = int32(counts)
		} else {
			return err
		}
	}

	if c.BrokerSts.Replicas > 0 {
		if counts, err := c.runningPodCounts(namespace, "brokersts"); err == nil {
			c.BrokerSts.Instances = int32(counts)
		} else {
			return err
		}
	}

	if c.Proxy.Replicas > 0 {
		if counts, err := c.runningPodCounts(namespace, "proxy"); err == nil {
			c.Proxy.Instances = int32(counts)
		} else {
			return err
		}
	}
	return nil
}

// EvalHealth evaluate the health of cluster status
func (c *Client) EvalHealth() (string, ClusterStatus) {
	health := ""
	status := ClusterStatus{
		ZookeeperOfflineInstances:  int(c.Zookeeper.Replicas - c.Zookeeper.Instances),
		BookkeeperOfflineInstances: int(c.Bookkeeper.Replicas - c.Bookkeeper.Instances),
		BrokerOfflineInstances:     int(c.Broker.Replicas - c.Broker.Instances),
		BrokerStsOfflineInstances:  int(c.BrokerSts.Replicas - c.BrokerSts.Instances),
		ProxyOfflineInstances:      int(c.Proxy.Replicas - c.Proxy.Instances),
		Status:                     OK,
	}
	if c.Zookeeper.Instances < 2 {
		health = fmt.Sprintf("\nCluster error - zookeeper is running %d instances out of %d replicas", c.Zookeeper.Instances, c.Zookeeper.Replicas)
		status.Status = TotalDown
	} else if c.Zookeeper.Instances == 2 {
		health = fmt.Sprintf("\nCluster warning - zookeeper is running only 2 instances")
		status.Status = PartialReady
	}

	if c.Bookkeeper.Instances < 2 {
		health = health + fmt.Sprintf("\nCluster error - bookkeeper is running %d instances out of %d replicas", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		status.Status = TotalDown
	} else if c.Bookkeeper.Instances != c.Bookkeeper.Replicas {
		health = health + fmt.Sprintf("\nCluster warning - bookkeeper is running %d instances out of %d", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}

	if (c.Broker.Instances + c.BrokerSts.Instances) == 0 {
		health = health + fmt.Sprintf("\nCluster error - no broker instances is running")
		status.Status = TotalDown
	} else if c.Broker.Instances < c.Broker.Replicas {
		health = fmt.Sprintf("\nCluster warning - broker is running %d instances out of %d", c.Broker.Instances, c.Broker.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}

	if c.BrokerSts.Replicas > 0 && c.BrokerSts.Instances == 0 {
		health = health + fmt.Sprintf("\nCluster error - broker stateful has no running instances out of %d replicas", c.Broker.Replicas)
		status.Status = TotalDown
	} else if status.BrokerStsOfflineInstances > 0 {
		health = health + fmt.Sprintf("\nCluster error - broker statefulset only has %d running instances out of %d replicas", c.BrokerSts.Instances, c.BrokerSts.Replicas)
		status.Status = TotalDown
	}

	if c.Proxy.Replicas > 0 && c.Proxy.Instances == 0 {
		health = health + fmt.Sprintf("\nCluster error - proxy has no running instances out of %d replicas", c.Proxy.Replicas)
		status.Status = TotalDown
	} else if c.Proxy.Replicas > 0 && c.Proxy.Instances < c.Proxy.Replicas {
		health = health + fmt.Sprintf("\nCluster warning - proxy is running %d instances out of %d", c.Proxy.Instances, c.Proxy.Replicas)
		status.Status = updateStatus(status.Status, PartialReady)
	}
	c.Status = status.Status
	return health, status
}

func updateStatus(original, current ClusterStatusCode) ClusterStatusCode {
	if current == TotalDown || original == TotalDown {
		return TotalDown
	} else if current == PartialReady || original == PartialReady {
		return PartialReady
	}
	return current
}

// WatchPodResource watches pod's resource
func (c *Client) WatchPodResource(namespace, component string) error {
	podMetrics, err := c.Metrics.MetricsV1beta1().PodMetricses(namespace).List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
	if err != nil {
		return err
	}
	for _, podMetric := range podMetrics.Items {
		podContainers := podMetric.Containers
		for _, container := range podContainers {
			cpuQuantity := container.Usage.Cpu().AsDec()
			memQuantity, _ := container.Usage.Memory().AsInt64()

			msg := fmt.Sprintf("Container Name: %s \n CPU usage: %v \n Memory usage: %d", container.Name, cpuQuantity, memQuantity)
			fmt.Println(msg)
		}

	}
	return nil
}

func (c *Client) runningPodCounts(namespace, component string) (int, error) {
	pods, err := c.Clientset.CoreV1().Pods(namespace).List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
	if err != nil {
		return -1, err
	}

	counts := 0
	for _, item := range pods.Items {
		containers := 0
		readyContainers := 0
		for _, status := range item.Status.ContainerStatuses {
			// status.Name is container name
			if status.Ready {
				readyContainers++
			}
			containers++
		}
		if containers == readyContainers {
			counts++
		}
	}

	return counts, nil
}

// GetNodeResource gets the node total available memory
func (c *Client) GetNodeResource() {
	nodeList, err := c.Clientset.CoreV1().Nodes().List(context.TODO(), meta_v1.ListOptions{})

	if err == nil {
		if len(nodeList.Items) > 0 {
			node := &nodeList.Items[0]
			memQuantity := node.Status.Allocatable[core_v1.ResourceMemory] // "memory"
			totalMemAvail := int(memQuantity.Value() >> 20)
			fmt.Printf("total memory %d", totalMemAvail)
		} else {
			log.Fatal("Unable to read node list")
			return
		}
	} else {
		log.Fatalf("Error while reading node list data: %v", err)
	}
}

func (c *Client) getDeployments(namespace, component string) (*v1.DeploymentList, error) {
	deploymentsClient := c.Clientset.AppsV1().Deployments(namespace)

	return deploymentsClient.List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
}

func (c *Client) getStatefulSets(namespace, component string) (*v1.StatefulSetList, error) {
	stsClient := c.Clientset.AppsV1().StatefulSets(namespace)

	return stsClient.List(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
}

// GetObjectMetaData returns metadata of a given k8s object
func GetObjectMetaData(obj interface{}) meta_v1.ObjectMeta {

	var objectMeta meta_v1.ObjectMeta

	switch object := obj.(type) {
	case *apps_v1.Deployment:
		objectMeta = object.ObjectMeta
	case *core_v1.ReplicationController:
		objectMeta = object.ObjectMeta
	case *apps_v1.ReplicaSet:
		objectMeta = object.ObjectMeta
	case *apps_v1.DaemonSet:
		objectMeta = object.ObjectMeta
	case *core_v1.Service:
		objectMeta = object.ObjectMeta
	case *core_v1.Pod:
		objectMeta = object.ObjectMeta
	case *batch_v1.Job:
		objectMeta = object.ObjectMeta
	case *core_v1.PersistentVolume:
		objectMeta = object.ObjectMeta
	case *core_v1.Namespace:
		objectMeta = object.ObjectMeta
	case *core_v1.Secret:
		objectMeta = object.ObjectMeta
	case *ext_v1beta1.Ingress:
		objectMeta = object.ObjectMeta
	}
	return objectMeta
}
