package k8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps_v1 "k8s.io/api/apps/v1"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const testNamespace = "pulsar"

func int32Ptr(i int32) *int32 { return &i }

func makeStatefulSet(namespace, name, component string, replicas int32) *apps_v1.StatefulSet {
	return &apps_v1.StatefulSet{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"component": component},
		},
		Spec: apps_v1.StatefulSetSpec{
			Replicas: int32Ptr(replicas),
		},
	}
}

func makeDeployment(namespace, name, component string, replicas int32) *apps_v1.Deployment {
	return &apps_v1.Deployment{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"component": component},
		},
		Spec: apps_v1.DeploymentSpec{
			Replicas: int32Ptr(replicas),
		},
	}
}

func makePod(namespace, name, component string, ready bool) *core_v1.Pod {
	return &core_v1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"component": component},
		},
		Status: core_v1.PodStatus{
			ContainerStatuses: []core_v1.ContainerStatus{
				{Name: "main", Ready: ready},
			},
		},
	}
}

func newTestClient(t *testing.T, namespace string, objs ...interface{}) *Client {
	t.Helper()

	clientset := fake.NewSimpleClientset()
	ctx := context.Background()
	for _, obj := range objs {
		switch o := obj.(type) {
		case *apps_v1.StatefulSet:
			_, err := clientset.AppsV1().StatefulSets(namespace).Create(ctx, o, meta_v1.CreateOptions{})
			require.NoError(t, err)
		case *apps_v1.Deployment:
			_, err := clientset.AppsV1().Deployments(namespace).Create(ctx, o, meta_v1.CreateOptions{})
			require.NoError(t, err)
		case *core_v1.Pod:
			_, err := clientset.CoreV1().Pods(namespace).Create(ctx, o, meta_v1.CreateOptions{})
			require.NoError(t, err)
		default:
			t.Fatalf("unsupported object type in newTestClient: %T", obj)
		}
	}

	client, err := newClient(clientset, nil, namespace)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

func TestNewClient_SyncsEmptyCache(t *testing.T) {
	client := newTestClient(t, testNamespace)

	deployments, err := client.getDeployments(testNamespace, BrokerDeployment)
	require.NoError(t, err)
	assert.Empty(t, deployments)

	stses, err := client.getStatefulSets(testNamespace, ZookeeperSts)
	require.NoError(t, err)
	assert.Empty(t, stses)

	counts, err := client.runningPodCounts(testNamespace, BrokerDeployment)
	require.NoError(t, err)
	assert.Equal(t, 0, counts)
}

func TestGetDeployments_FiltersByComponentLabel(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeDeployment(testNamespace, "broker", BrokerDeployment, 3),
		makeDeployment(testNamespace, "proxy", ProxyDeployment, 2),
	)

	brokers, err := client.getDeployments(testNamespace, BrokerDeployment)
	require.NoError(t, err)
	require.Len(t, brokers, 1)
	assert.Equal(t, "broker", brokers[0].Name)
	assert.Equal(t, int32(3), *brokers[0].Spec.Replicas)

	proxies, err := client.getDeployments(testNamespace, ProxyDeployment)
	require.NoError(t, err)
	require.Len(t, proxies, 1)
	assert.Equal(t, "proxy", proxies[0].Name)
}

func TestGetStatefulSets_FiltersByComponentLabel(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "zookeeper", ZookeeperSts, 3),
		makeStatefulSet(testNamespace, "bookkeeper", BookkeeperSts, 4),
	)

	zk, err := client.getStatefulSets(testNamespace, ZookeeperSts)
	require.NoError(t, err)
	require.Len(t, zk, 1)
	assert.Equal(t, int32(3), *zk[0].Spec.Replicas)

	bk, err := client.getStatefulSets(testNamespace, BookkeeperSts)
	require.NoError(t, err)
	require.Len(t, bk, 1)
	assert.Equal(t, int32(4), *bk[0].Spec.Replicas)
}

func TestRunningPodCounts_OnlyCountsFullyReadyPods(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makePod(testNamespace, "broker-0", BrokerDeployment, true),
		makePod(testNamespace, "broker-1", BrokerDeployment, true),
		makePod(testNamespace, "broker-2", BrokerDeployment, false),
		makePod(testNamespace, "proxy-0", ProxyDeployment, true),
	)

	brokerCount, err := client.runningPodCounts(testNamespace, BrokerDeployment)
	require.NoError(t, err)
	assert.Equal(t, 2, brokerCount)

	proxyCount, err := client.runningPodCounts(testNamespace, ProxyDeployment)
	require.NoError(t, err)
	assert.Equal(t, 1, proxyCount)
}

func TestUpdateReplicas_PopulatesFromCache(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "zookeeper", ZookeeperSts, 3),
		makeStatefulSet(testNamespace, "bookkeeper", BookkeeperSts, 4),
		makeDeployment(testNamespace, "broker", BrokerDeployment, 3),
		makeDeployment(testNamespace, "proxy", ProxyDeployment, 2),
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))

	assert.Equal(t, int32(3), client.Zookeeper.Replicas)
	assert.Equal(t, int32(4), client.Bookkeeper.Replicas)
	assert.Equal(t, int32(3), client.Broker.Replicas)
	assert.Equal(t, int32(2), client.Proxy.Replicas)
	assert.Equal(t, int32(0), client.BrokerSts.Replicas)
}

func TestUpdateReplicas_BookkeeperFallsBackToBookkeeper2(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "bookkeeper2", BookkeeperSts2, 5),
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))

	assert.Equal(t, int32(5), client.Bookkeeper.Replicas)
}

func TestUpdateReplicas_BrokerFallsBackToStatefulSet(t *testing.T) {
	// No broker Deployment exists; broker is deployed as a StatefulSet
	// labeled component=broker instead.
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "broker", BrokerDeployment, 3),
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))

	assert.Equal(t, int32(0), client.Broker.Replicas)
	assert.Equal(t, int32(3), client.BrokerSts.Replicas)
}

func TestWatchPods_BookkeeperFallsBackToBookkeeper2Pods(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "bookkeeper2", BookkeeperSts2, 3),
		makePod(testNamespace, "bookkeeper2-0", BookkeeperSts2, true),
		makePod(testNamespace, "bookkeeper2-1", BookkeeperSts2, true),
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))
	require.NoError(t, client.WatchPods(testNamespace))

	assert.Equal(t, int32(2), client.Bookkeeper.Instances)
}

func TestEvalHealth_HealthyCluster(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "zookeeper", ZookeeperSts, 3),
		makeStatefulSet(testNamespace, "bookkeeper", BookkeeperSts, 3),
		makeDeployment(testNamespace, "broker", BrokerDeployment, 2),
		makeDeployment(testNamespace, "proxy", ProxyDeployment, 1),
		makePod(testNamespace, "zk-0", ZookeeperSts, true),
		makePod(testNamespace, "zk-1", ZookeeperSts, true),
		makePod(testNamespace, "zk-2", ZookeeperSts, true),
		makePod(testNamespace, "bk-0", BookkeeperSts, true),
		makePod(testNamespace, "bk-1", BookkeeperSts, true),
		makePod(testNamespace, "bk-2", BookkeeperSts, true),
		makePod(testNamespace, "broker-0", BrokerDeployment, true),
		makePod(testNamespace, "broker-1", BrokerDeployment, true),
		makePod(testNamespace, "proxy-0", ProxyDeployment, true),
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))
	require.NoError(t, client.WatchPods(testNamespace))

	_, status := client.EvalHealth()
	assert.Equal(t, OK, status.Status)
	assert.Equal(t, 0, status.ZookeeperOfflineInstances)
	assert.Equal(t, 0, status.BookkeeperOfflineInstances)
	assert.Equal(t, 0, status.BrokerOfflineInstances)
	assert.Equal(t, 0, status.ProxyOfflineInstances)
}

func TestEvalHealth_TotalDownWhenNoBrokerRunning(t *testing.T) {
	client := newTestClient(t, testNamespace,
		makeStatefulSet(testNamespace, "zookeeper", ZookeeperSts, 3),
		makeStatefulSet(testNamespace, "bookkeeper", BookkeeperSts, 3),
		makeDeployment(testNamespace, "broker", BrokerDeployment, 2),
		makePod(testNamespace, "zk-0", ZookeeperSts, true),
		makePod(testNamespace, "zk-1", ZookeeperSts, true),
		makePod(testNamespace, "zk-2", ZookeeperSts, true),
		makePod(testNamespace, "bk-0", BookkeeperSts, true),
		makePod(testNamespace, "bk-1", BookkeeperSts, true),
		makePod(testNamespace, "bk-2", BookkeeperSts, true),
		// no broker pods running despite 2 desired replicas
	)

	require.NoError(t, client.UpdateReplicas(testNamespace))
	require.NoError(t, client.WatchPods(testNamespace))

	_, status := client.EvalHealth()
	assert.Equal(t, TotalDown, status.Status)
	assert.Equal(t, 2, status.BrokerOfflineInstances)
}

func TestClose_IsIdempotent(t *testing.T) {
	client := newTestClient(t, testNamespace)
	client.Close()
	client.Close() // must not panic on repeated Close
}
