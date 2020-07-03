package k8s

import (
	"context"
	"flag"
	"fmt"
	"log"
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
)

// Client is the k8s client object
type Client struct {
	Clientset        *kubernetes.Clientset
	Metrics          *metrics.Clientset
	ClusterName      string
	DefaultNamespace string
	zookeeperSize    int32
	bookkeeperSize   int32
	brokerSize       int32
	proxySize        int32
	functionSize     int32
}

// GetK8sClient gets k8s clientset
func GetK8sClient() (*Client, error) {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// *kubeconfig is empty string if running inside a cluster
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	metrics, err := metrics.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	client := Client{
		Clientset: clientset,
		Metrics:   metrics,
	}

	broker, err := client.getDeployments(DefaultPulsarNamespace, "broker")
	if err != nil {
		return nil, err
	}
	client.brokerSize = *(broker.Items[0]).Spec.Replicas

	proxy, err := client.getDeployments(DefaultPulsarNamespace, "proxy")
	if err != nil {
		return nil, err
	}
	client.proxySize = *(proxy.Items[0]).Spec.Replicas

	zk, err := client.getStatefulSets(DefaultPulsarNamespace, "zookeeper")
	if err != nil {
		return nil, err
	}
	client.zookeeperSize = *(zk.Items[0]).Spec.Replicas

	bk, err := client.getStatefulSets(DefaultPulsarNamespace, "bookkeeper")
	if err != nil {
		return nil, err
	}
	client.bookkeeperSize = *(bk.Items[0]).Spec.Replicas

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

// WatchPods watches the running pods vs intended replicas
func (c *Client) WatchPods(namespace string) error {
	if counts, err := c.runningPodCounts(namespace, "zookeeper"); err == nil {
		log.Printf("zookeepers running %d instances, replicas %d", counts, c.zookeeperSize)
	}

	if counts, err := c.runningPodCounts(namespace, "bookkeeper"); err == nil {
		log.Printf("bookkeeper running %d instances, replicas %d", counts, c.bookkeeperSize)
	}
	if counts, err := c.runningPodCounts(namespace, "broker"); err == nil {
		log.Printf("broker running %d instances, replicas %d", counts, c.brokerSize)
	}
	if counts, err := c.runningPodCounts(namespace, "proxy"); err == nil {
		log.Printf("proxy running %d instances, replicas %d", counts, c.proxySize)
	}
	return nil
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
	log.Printf("size %d", pods.Size())

	counts := 0
	for _, item := range pods.Items {
		log.Println(item)
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
				log.Println("*** Taking action ***")
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
