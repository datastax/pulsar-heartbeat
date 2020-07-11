package k8s

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	apps_v1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	core_v1 "k8s.io/api/core/v1"
	ext_v1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
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

	// ProxyDeployment is the proxy deployment name
	ProxyDeployment = "proxy"

	// FunctionWorkerDeployment is the function worker deployment name
	FunctionWorkerDeployment = "functionWorker"
)

// ClusterStatus is the high level health of cluster status
type ClusterStatus int

const (
	// TotalDown is the initial status
	TotalDown ClusterStatus = iota

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
	Status           ClusterStatus
	Zookeeper        StatefulSet
	Bookkeeper       StatefulSet
	Broker           Deployment
	Proxy            Deployment
	FunctionWorker   StatefulSet
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
func GetK8sClient() (*Client, error) {
	var config *rest.Config

	if home := homedir.HomeDir(); home != "" {
		// TODO: add configuration to allow customized config file
		kubeconfig := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfig); os.IsNotExist(err) {
			log.Println("this is an in-cluster k8s monitor")
			if config, err = rest.InClusterConfig(); err != nil {
				return nil, err
			}

		} else {
			log.Printf("this is outside of k8s cluster monitor, kubeconfig dir %s", kubeconfig)
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

	err = client.UpdateReplicas()
	if err != nil {
		return nil, err
	}
	return &client, nil
}

func buildInClusterConfig() kubernetes.Interface {
	config, err := rest.InClusterConfig()
	if err != nil {
		logrus.Fatalf("Can not get kubernetes config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logrus.Fatalf("Can not create kubernetes client: %v", err)
	}

	return clientset
}

// UpdateReplicas updates the replicas for deployments and sts
func (c *Client) UpdateReplicas() error {
	broker, err := c.getDeployments(DefaultPulsarNamespace, BrokerDeployment)
	if err != nil {
		return err
	}
	c.Broker.Replicas = *(broker.Items[0]).Spec.Replicas

	proxy, err := c.getDeployments(DefaultPulsarNamespace, ProxyDeployment)
	if err != nil {
		return err
	}
	c.Proxy.Replicas = *(proxy.Items[0]).Spec.Replicas

	zk, err := c.getStatefulSets(DefaultPulsarNamespace, ZookeeperSts)
	if err != nil {
		return err
	}
	c.Zookeeper.Replicas = *(zk.Items[0]).Spec.Replicas

	bk, err := c.getStatefulSets(DefaultPulsarNamespace, BookkeeperSts)
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

	if counts, err := c.runningPodCounts(namespace, "broker"); err == nil {
		c.Broker.Instances = int32(counts)
	} else {
		return err
	}

	if counts, err := c.runningPodCounts(namespace, "proxy"); err == nil {
		c.Proxy.Instances = int32(counts)
	} else {
		return err
	}
	return nil
}

// EvalHealth evaluate the health of cluster status
func (c *Client) EvalHealth() (string, ClusterStatus) {
	health := ""
	if c.Zookeeper.Instances < 2 {
		health = fmt.Sprintf("\nCluster error - zookeeper is running %d instances out of %d replicas", c.Zookeeper.Instances, c.Zookeeper.Replicas)
		c.Status = TotalDown
	} else if c.Zookeeper.Instances == 2 {
		health = fmt.Sprintf("\nCluster warning - zookeeper is running only 2 instances")
		c.Status = PartialReady
	} else if c.Zookeeper.Instances == c.Zookeeper.Replicas {
		c.Status = OK
	}

	if c.Bookkeeper.Instances < 2 {
		health = fmt.Sprintf("\nCluster error - bookkeeper is running %d instances out of %d replicas", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		c.Status = TotalDown
	} else if c.Bookkeeper.Instances != c.Bookkeeper.Replicas {
		health = fmt.Sprintf("\nCluster warning - bookkeeper is running %d instances out of %d", c.Bookkeeper.Instances, c.Bookkeeper.Replicas)
		c.Status = PartialReady
	} else if c.Bookkeeper.Instances == c.Bookkeeper.Replicas {
		c.Status = OK
	}

	if c.Broker.Instances == 0 {
		health = fmt.Sprintf("\nCluster error - broker has no running instances out of %d replicas", c.Broker.Replicas)
		c.Status = TotalDown
	} else if c.Broker.Instances < c.Broker.Replicas {
		health = fmt.Sprintf("\nCluster warning - broker is running %d instances out of %d", c.Broker.Instances, c.Broker.Replicas)
		c.Status = PartialReady
	} else if c.Broker.Instances == c.Broker.Replicas {
		c.Status = OK
	}

	if c.Proxy.Instances == 0 {
		health = fmt.Sprintf("\nCluster error - proxy has no running instances out of %d replicas", c.Proxy.Replicas)
		c.Status = TotalDown
	} else if c.Proxy.Instances < c.Proxy.Replicas {
		health = fmt.Sprintf("\nCluster warning - proxy is running %d instances out of %d", c.Proxy.Instances, c.Proxy.Replicas)
		c.Status = PartialReady
	} else if c.Proxy.Instances == c.Proxy.Replicas {
		c.Status = OK
	}
	return health, c.Status
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
		for _, status := range item.Status.ContainerStatuses {
			if status.Ready {
				counts++
			}
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

// WatchPVC watches any pvc changes
func (c *Client) WatchPVC(namespace, component, maxClaim string) error {
	// maxClaim 2000Gi
	// loop through events
	var maxClaims string
	flag.StringVar(&maxClaims, "max-claims", maxClaim,
		"Maximum total claims to watch")
	var totalClaimedQuant resource.Quantity
	maxClaimedQuant := resource.MustParse(maxClaims)

	// watch future changes to PVCs
	watcher, err := c.Clientset.CoreV1().PersistentVolumeClaims(namespace).Watch(context.TODO(), meta_v1.ListOptions{
		LabelSelector: fmt.Sprintf("component=%s", component),
	})
	if err != nil {
		return err
	}
	ch := watcher.ResultChan()

	fmt.Printf("--- PVC Watch (max claims %v) ----\n", maxClaimedQuant.String())
	for event := range ch {
		pvc, ok := event.Object.(*core_v1.PersistentVolumeClaim)
		if !ok {
			return fmt.Errorf("*core_v1.PersistentVolumeClaim unexpected event type")
		}
		quant := pvc.Spec.Resources.Requests[core_v1.ResourceStorage]

		switch event.Type {
		case watch.Added:
			totalClaimedQuant.Add(quant)
			log.Printf("PVC %s added, claim size %s\n", pvc.Name, quant.String())

			// is claim overage?
			if totalClaimedQuant.Cmp(maxClaimedQuant) == 1 {
				log.Printf("\nClaim overage reached: max %s at %s",
					maxClaimedQuant.String(),
					totalClaimedQuant.String(),
				)
				// trigger action
				log.Println("trigger action")
			}

		case watch.Modified:
			//log.Printf("Pod %s modified\n", pod.GetName())
		case watch.Deleted:
			quant := pvc.Spec.Resources.Requests[core_v1.ResourceStorage]
			totalClaimedQuant.Sub(quant)
			log.Printf("PVC %s removed, size %s\n", pvc.Name, quant.String())

			if totalClaimedQuant.Cmp(maxClaimedQuant) <= 0 {
				log.Printf("Claim usage normal: max %s at %s",
					maxClaimedQuant.String(),
					totalClaimedQuant.String(),
				)
				// trigger action
				log.Println("*** Taking action ***")
			}
		case watch.Error:
			//log.Printf("watcher error encountered\n", pod.GetName())
		}

		log.Printf("\nAt %3.1f%% claim capcity (%s/%s)\n",
			float64(totalClaimedQuant.Value())/float64(maxClaimedQuant.Value())*100,
			totalClaimedQuant.String(),
			maxClaimedQuant.String(),
		)
	}
	return nil
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
