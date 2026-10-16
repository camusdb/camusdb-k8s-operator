/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
	"github.com/camusdb/k8s-operator/internal/camusdb"
)

// fakeCamusDB serves a canned roster so reconciler behaviour can be exercised without a running
// CamusDB cluster. err models "no node was reachable", which the operator must treat as a reason
// to hold rather than as a reason to act.
type fakeCamusDB struct {
	membership *camusdb.Membership
	err        error
	// leaveResult is what a decommission request returns; nil means a committed removal.
	leaveResult *camusdb.LeaveResult
	leaveErr    error
	// leaveCalls records which base URLs were asked to leave, so a test can assert the operator
	// asked before removing rather than just removing.
	leaveCalls []string
	// targets records every node the operator called, so a test can assert how it called them.
	targets []camusdb.Target
	// placement is the replica placement reported; nil means every data partition has a replica
	// set, which is what a cluster created with more nodes than its factor looks like.
	placement *camusdb.Placement
	// metrics returns the dashboard metrics for a base URL; nil means the endpoint is refused.
	metrics func(baseURL string) *camusdb.Metrics
	// cpu is the process CPU share every node reports; nil means the route does not exist.
	cpu *float64
}

func (f *fakeCamusDB) Placement(_ context.Context, t camusdb.Target) (*camusdb.Placement, error) {
	f.targets = append(f.targets, t)
	if f.placement != nil {
		return f.placement, nil
	}
	return &camusdb.Placement{ReplicationFactor: 3, Initialized: true, Partitions: []camusdb.PartitionPlacement{
		{PartitionID: 1, State: camusdb.PartitionStateActive, Replicas: []camusdb.ReplicaPlacement{{Endpoint: "a"}, {Endpoint: "b"}, {Endpoint: "c"}}},
	}}, nil
}

func (f *fakeCamusDB) Metrics(_ context.Context, t camusdb.Target) (*camusdb.Metrics, error) {
	f.targets = append(f.targets, t)
	if f.metrics == nil {
		return nil, fmt.Errorf("/v1/dashboard/metrics returned HTTP 403")
	}
	return f.metrics(t.BaseURL), nil
}

func (f *fakeCamusDB) NodeStats(_ context.Context, t camusdb.Target) (*camusdb.NodeStats, error) {
	f.targets = append(f.targets, t)
	if f.cpu == nil {
		return nil, fmt.Errorf("/v1/admin/stats returned HTTP 404")
	}
	return &camusdb.NodeStats{Status: "ok", CPU: &camusdb.CPUStats{ProcessCPUPercent: *f.cpu}}, nil
}

func (f *fakeCamusDB) Membership(_ context.Context, t camusdb.Target) (*camusdb.Membership, error) {
	f.targets = append(f.targets, t)
	if f.err != nil {
		return nil, f.err
	}
	return f.membership, nil
}

func (f *fakeCamusDB) Health(context.Context, camusdb.Target) (*camusdb.Health, error) {
	return &camusdb.Health{Ready: true, Initialized: true, LocalRole: camusdb.RoleVoter}, nil
}

func (f *fakeCamusDB) Leave(_ context.Context, t camusdb.Target) (*camusdb.LeaveResult, error) {
	f.leaveCalls = append(f.leaveCalls, t.BaseURL)
	if f.leaveErr != nil {
		return nil, f.leaveErr
	}
	if f.leaveResult != nil {
		return f.leaveResult, nil
	}
	return &camusdb.LeaveResult{Left: true, Outcome: camusdb.OutcomeCommitted}, nil
}

// testCA is a real self-signed CA certificate in PEM. The operator parses the CA it trusts, so a
// placeholder would fail before any behaviour under test is reached.
// staleClusterClient stands in for the manager's informer cache lagging the API server: Gets of
// the CamusDBCluster return a snapshot taken before the last status write, while everything else,
// writes included, goes to the real API server.
type staleClusterClient struct {
	client.Client
	stale *camusdbv1alpha1.CamusDBCluster
}

func (c *staleClusterClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if out, ok := obj.(*camusdbv1alpha1.CamusDBCluster); ok && key.Name == c.stale.Name {
		c.stale.DeepCopyInto(out)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// conflictOnceClient rejects the first status write with a conflict, as the API server does when
// someone else changed the object between the reconciler's read and its write.
type conflictOnceClient struct {
	client.Client
	failed bool
}

func (c *conflictOnceClient) Status() client.SubResourceWriter {
	return &conflictOnceStatus{SubResourceWriter: c.Client.Status(), parent: c}
}

type conflictOnceStatus struct {
	client.SubResourceWriter
	parent *conflictOnceClient
}

func (s *conflictOnceStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if !s.parent.failed {
		s.parent.failed = true
		return apierrors.NewConflict(camusdbv1alpha1.GroupVersion.WithResource("camusdbclusters").GroupResource(),
			obj.GetName(), fmt.Errorf("the object has been modified"))
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

var testCA = sync.OnceValue(func() []byte {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "envtest-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemCertificate, Bytes: der})
})

// rosterOf builds a roster of Voters for ordinals 0..n-1 of the given cluster.
func rosterOf(cluster *camusdbv1alpha1.CamusDBCluster, n int32) *camusdb.Membership {
	m := &camusdb.Membership{MembershipVersion: int64(n), Initialized: true, LocalRole: camusdb.RoleVoter}
	for i := range n {
		m.Members = append(m.Members, camusdb.Member{
			Endpoint: raftEndpoint(cluster, i),
			NodeID:   nodeIDForOrdinal(i),
			Role:     camusdb.RoleVoter,
		})
	}
	return m
}

var _ = Describe("CamusDBCluster Controller", func() {
	const namespace = "default"

	var (
		ctx        context.Context
		name       string
		nn         types.NamespacedName
		cluster    *camusdbv1alpha1.CamusDBCluster
		reconciler *CamusDBClusterReconciler
		counter    int
	)

	newCluster := func(replicas int32) *camusdbv1alpha1.CamusDBCluster {
		return &camusdbv1alpha1.CamusDBCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: camusdbv1alpha1.CamusDBClusterSpec{
				Replicas:   ptr.To(replicas),
				Image:      "camusdb/camusdb:dev",
				Partitions: ptr.To(int32(3)),
				Storage: camusdbv1alpha1.StorageSpec{
					Backend: camusdbv1alpha1.StorageBackendRocksDB,
					Data:    camusdbv1alpha1.VolumeSpec{Size: resource.MustParse("1Gi")},
				},
				TLS: camusdbv1alpha1.TLSSpec{
					SecretRef: &corev1.LocalObjectReference{Name: testTLSSecret},
				},
			},
		}
	}

	// ensurePods stands in for the StatefulSet controller, which envtest does not run. The
	// reconciler reaches nodes by pod IP, so a roster is only observable when pods carrying one
	// exist.
	ensurePods := func(n int32) {
		for i := range n {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName(cluster, i),
					Namespace: namespace,
					Labels:    selectorLabels(cluster),
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: containerName, Image: "camusdb/camusdb:dev"}}},
			}
			if err := k8sClient.Create(ctx, pod); err != nil {
				Expect(apierrors.IsAlreadyExists(err)).To(BeTrue(), "%v", err)
				continue
			}
			pod.Status.PodIP = fmt.Sprintf("10.244.0.%d", i+1)
			pod.Status.Conditions = []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
	}

	// syncSTSStatus stands in for the StatefulSet controller, which envtest also does not run.
	// Pods count as ready only up to the roster size, mirroring reality: a node just added by a
	// scale is not ready until it has joined.
	syncSTSStatus := func(rosterSize int32) {
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, nn, sts); err != nil {
			return
		}
		want := *sts.Spec.Replicas
		ready := min(rosterSize, want)
		sts.Status.Replicas = want
		sts.Status.ReadyReplicas = ready
		sts.Status.CurrentReplicas = want
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
	}

	// reconcile runs a pass with the given roster view and returns the fake, so a test can assert
	// on what the operator asked the cluster to do.
	reconcileUsing := func(fake *fakeCamusDB) *fakeCamusDB {
		n := int32(3)
		if fake.membership != nil {
			n = int32(len(fake.membership.Members))
		}
		ensurePods(n)
		syncSTSStatus(n)
		reconciler.CamusDB = fake
		_, rerr := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(rerr).NotTo(HaveOccurred())
		return fake
	}

	reconcileWith := func(m *camusdb.Membership, err error) {
		reconcileUsing(&fakeCamusDB{membership: m, err: err})
	}

	getSTS := func() *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, nn, sts)).To(Succeed())
		return sts
	}

	getCluster := func() *camusdbv1alpha1.CamusDBCluster {
		out := &camusdbv1alpha1.CamusDBCluster{}
		Expect(k8sClient.Get(ctx, nn, out)).To(Succeed())
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		name = fmt.Sprintf("camus-%d", counter)
		nn = types.NamespacedName{Name: name, Namespace: namespace}
		// The keystore Secret every test cluster mounts. Without it no StatefulSet is created.
		tlsSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testTLSSecret, Namespace: namespace},
			Data:       map[string][]byte{defaultKeystoreKey: []byte("pfx"), defaultCAKey: testCA()},
		}
		if err := k8sClient.Create(ctx, tlsSecret); err != nil {
			Expect(apierrors.IsAlreadyExists(err)).To(BeTrue(), "%v", err)
		}
		cluster = newCluster(3)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		reconciler = &CamusDBClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		obj := &camusdbv1alpha1.CamusDBCluster{}
		if err := k8sClient.Get(ctx, nn, obj); err == nil {
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())
		}
	})

	It("creates the owned objects with a bootstrap-safe StatefulSet", func() {
		reconcileWith(nil, fmt.Errorf("no cluster yet"))

		sts := getSTS()
		// Parallel is required: under OrderedReady, pod-0 would have to become ready before
		// pod-1 is created, but it cannot become ready until its peers exist.
		Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		Expect(sts.Spec.ServiceName).To(Equal(peerServiceName(cluster)))

		peer := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: peerServiceName(cluster), Namespace: namespace}, peer)).To(Succeed())
		Expect(peer.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		// Without this, peers cannot resolve each other before they are ready, and readiness
		// requires a quorum — bootstrap would deadlock.
		Expect(peer.Spec.PublishNotReadyAddresses).To(BeTrue())

		client := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clientServiceName(cluster), Namespace: namespace}, client)).To(Succeed())
		Expect(client.Spec.PublishNotReadyAddresses).To(BeFalse())

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: configMapName(cluster), Namespace: namespace}, cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey(entrypointFileName))
		Expect(cm.Data).To(HaveKey(serverConfigFileName))
		Expect(cm.Data).To(HaveKey(topologyFileName))

		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pdbName(cluster), Namespace: namespace}, pdb)).To(Succeed())
		Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))

		Expect(getCluster().Status.BootstrapSize).To(Equal(int32(3)))
	})

	It("marks the cluster bootstrapped only once the roster carries every node as a voter", func() {
		reconcileWith(nil, fmt.Errorf("unreachable"))
		Expect(getCluster().Status.Bootstrapped).To(BeFalse())

		// A seeded roster that is still short of the full voter set is not bootstrap complete.
		reconcileWith(rosterOf(cluster, 2), nil)
		Expect(getCluster().Status.Bootstrapped).To(BeFalse())

		reconcileWith(rosterOf(cluster, 3), nil)
		out := getCluster()
		Expect(out.Status.Bootstrapped).To(BeTrue())
		Expect(out.Status.Voters).To(Equal(int32(3)))
		Expect(out.Status.Phase).To(Equal(camusdbv1alpha1.PhaseRunning))
	})

	It("scales up one node at a time and only from a converged roster", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// The roster still shows 3, so exactly one node may be added.
		reconcileWith(rosterOf(cluster, 3), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		// Until the new node is a committed Voter, no further step is taken.
		reconcileWith(rosterOf(cluster, 3), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		reconcileWith(rosterOf(cluster, 4), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 5)})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))
		Expect(getCluster().Status.Phase).To(Equal(camusdbv1alpha1.PhaseRunning))
		Expect(fake.leaveCalls).To(BeEmpty(), "growing the cluster asked a node to leave")
	})

	It("scales down one node at a time, waiting for the cluster to evict each one", func() {
		// Grow to 5 first: 3 cannot shrink anywhere legal (2 is rejected outright, and 1 is a
		// different quorum model the API refuses to transition into).
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		// The pod is gone but the roster still counts it: SWIM has not committed the eviction,
		// so removing another node now could cost quorum.
		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		reconcileWith(rosterOf(cluster, 4), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))
	})

	It("asks the departing node to leave before removing its pod", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 5)})

		// The highest ordinal is the one the StatefulSet will delete, so it is the one that has to
		// be out of the roster first.
		Expect(fake.leaveCalls).To(HaveLen(1))
		// Over the TLS port: CamusDB refuses the operator's token over plaintext.
		Expect(fake.leaveCalls[0]).To(Equal("https://10.244.0.5:7141"), "asked the wrong node to leave")
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)),
			"the pod was not removed after the node left")
	})

	It("keeps the node when it cannot be asked to leave", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// Deleting a pod that is still a committed member is exactly what the leave call exists to
		// avoid, so an unreachable node must stall the scale rather than fall back to it.
		reconcileUsing(&fakeCamusDB{
			membership: rosterOf(cluster, 5),
			leaveErr:   fmt.Errorf("connection refused"),
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		// A permanent refusal holds too, and must not be retried into a loop of pod deletions.
		reconcileUsing(&fakeCamusDB{
			membership: rosterOf(cluster, 5),
			leaveResult: &camusdb.LeaveResult{
				Outcome:   camusdb.OutcomeRefusedInsufficientVoters,
				Retryable: false,
				Reason:    "would leave the cluster without a voter",
			},
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

		// So does CamusDB's own refusal of the operator's account.
		reconcileUsing(&fakeCamusDB{
			membership: rosterOf(cluster, 5),
			leaveResult: &camusdb.LeaveResult{
				Outcome: camusdb.OutcomeRefused, Retryable: false,
				Reason: "Cluster administration requires a superuser",
			},
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))
	})

	It("falls back to eviction when authentication is off", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// Without authentication CamusDB refuses the leave call from anything but loopback, so the
		// operator does not ask. It removes the pod and waits for the failure detector instead.
		fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 5)})
		Expect(fake.leaveCalls).To(BeEmpty())
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

		// The roster still counts the removed node: no further step until it is evicted.
		reconcileWith(rosterOf(cluster, 5), nil)
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))
	})

	It("treats a node that already left as removable", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		obj := getCluster()
		obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)
		reconcileWith(rosterOf(cluster, 4), nil)
		reconcileWith(rosterOf(cluster, 5), nil)

		obj = getCluster()
		obj.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		// Idempotency: a retried scale step must not stall because the node is already gone.
		reconcileUsing(&fakeCamusDB{
			membership:  rosterOf(cluster, 5),
			leaveResult: &camusdb.LeaveResult{Outcome: camusdb.OutcomeNotAMember},
		})
		Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))
	})

	It("holds every node in place while the roster cannot be read", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		reconcileWith(nil, fmt.Errorf("all nodes unreachable"))

		sts := getSTS()
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		// Freezing the rolling update matters as much as freezing the size: taking another node
		// down while the cluster's state is unknown can turn degraded into dead.
		Expect(*sts.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(int32(3)))
	})

	It("keeps the pod template stable across a scale", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		before := getSTS().Spec.Template.Annotations[configHashAnnotation]

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getSTS().Spec.Template.Annotations[configHashAnnotation]).To(Equal(before),
			"scaling changed the pod template, which would restart every existing node")
	})

	It("rolls the cluster when the configuration actually changes", func() {
		reconcileWith(rosterOf(cluster, 3), nil)
		before := getSTS().Spec.Template.Annotations[configHashAnnotation]

		obj := getCluster()
		obj.Spec.ExtraArgs = []string{flagSchemaAck, "5000"}
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getSTS().Spec.Template.Annotations[configHashAnnotation]).NotTo(Equal(before))
	})

	Context("with autoscaling", func() {
		// steadyLoad returns a metrics source where every node dispatches writesPerNode log entries
		// per second. Each call advances the node's clock by 15 s, past the sampler's minimum interval.
		steadyLoad := func(writesPerNode float64) func(string) *camusdb.Metrics {
			clocks := map[string]int64{}
			return func(baseURL string) *camusdb.Metrics {
				clocks[baseURL] += 15_000
				total := writesPerNode * float64(clocks[baseURL]) / 1000
				return &camusdb.Metrics{MonotonicMs: clocks[baseURL], Rows: []camusdb.MetricRow{{
					Metric: metricLogEntries, Kind: camusdb.MetricKindCounter, Total: ptr.To(total),
				}}}
			}
		}

		// autoscale turns the test's cluster into an autoscaled one founded at 4 nodes, which is the
		// smallest size at which a replication factor of 3 produces real placement.
		autoscale := func() {
			obj := getCluster()
			obj.Spec.Replicas = ptr.To(int32(4))
			obj.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
			obj.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{
				MinReplicas:                   3,
				MaxReplicas:                   6,
				TargetLogEntriesPerSecond:     ptr.To(int32(1000)),
				ScaleUpStabilizationSeconds:   ptr.To(int32(0)),
				ScaleDownStabilizationSeconds: ptr.To(int32(0)),
			}
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		}

		It("adds a node under load, one at a time", func() {
			autoscale()
			load := steadyLoad(5000)

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: load})
			// The first converged pass has no rate yet: one reading is not a rate.
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: load})
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: load})
			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(5)))
			Expect(out.Status.Autoscaling.RecommendedReplicas).To(Equal(int32(6)))
			Expect(*out.Status.Autoscaling.LogEntriesPerSecondPerNode).To(Equal(int64(5000)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

			// The new node has not joined: the autoscaler must not stack a second step on it.
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: load})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(5)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(5)))

			// spec.replicas is only the starting size; the autoscaler owns the count now.
			Expect(*getCluster().Spec.Replicas).To(Equal(int32(4)))
		})

		It("removes a node when idle, through the leave call, down to the minimum", func() {
			autoscale()
			idle := steadyLoad(0)

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})

			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
			Expect(fake.leaveCalls).To(HaveLen(1))
			Expect(fake.leaveCalls[0]).To(Equal("https://10.244.0.4:7141"))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 3), metrics: idle})
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 3), metrics: idle})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)),
				"scaled below minReplicas")
		})

		// The pass right after a step is triggered by the StatefulSet and pod changes that step
		// caused, and runs before the cache has seen the step's own status write.
		It("does not undo a step down when the cached cluster predates it", func() {
			autoscale()
			idle := steadyLoad(0)

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			beforeStep := getCluster()
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))

			reconciler.APIReader = k8sClient
			reconciler.Client = &staleClusterClient{Client: k8sClient, stale: beforeStep}
			// The departed node is out of the roster; its pod has not terminated yet. A pass acting on
			// the stale copy fails its status write, but only after resizing, so the error is not
			// what this asserts on.
			reconciler.CamusDB = &fakeCamusDB{membership: rosterOf(cluster, 3), metrics: idle}
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})

			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)),
				"a cached status from before the step grew the StatefulSet back")
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
		})

		It("does not resize the StatefulSet when the new size could not be recorded", func() {
			autoscale()
			idle := steadyLoad(0)

			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})

			reconciler.Client = &conflictOnceClient{Client: k8sClient}
			fake := &fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle}
			reconciler.CamusDB = fake
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(apierrors.IsConflict(err)).To(BeTrue(), "%v", err)

			// Stored status still says 4. Had the step gone ahead, the next pass would read 4 and
			// put the departed node back.
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(4)))
			Expect(fake.leaveCalls).To(BeEmpty(), "a node was asked to leave for a size that was never recorded")
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(4)))

			// The retry decides again and goes through.
			fake = reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), metrics: idle})
			Expect(fake.leaveCalls).To(HaveLen(1))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
		})

		It("does not grow a fully replicated cluster past the replication cap", func() {
			obj := getCluster()
			obj.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
			obj.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{
				MinReplicas: 3, MaxReplicas: 6,
				TargetLogEntriesPerSecond:   ptr.To(int32(1000)),
				ScaleUpStabilizationSeconds: ptr.To(int32(0)),
			}
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())

			// Founded at 3 nodes with factor 3: the server left every data partition without a
			// replica set, so each node added would be one more copy of everything.
			full := &camusdb.Placement{ReplicationFactor: 3, Initialized: true, Partitions: []camusdb.PartitionPlacement{
				{PartitionID: 1, State: camusdb.PartitionStateActive}, {PartitionID: 2, State: camusdb.PartitionStateActive},
			}}
			load := steadyLoad(5000)
			for range 4 {
				reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 3), metrics: load, placement: full})
			}

			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
			Expect(*getSTS().Spec.Replicas).To(Equal(int32(3)))
			limited := apimeta.FindStatusCondition(out.Status.Conditions, camusdbv1alpha1.ConditionAutoscalingLimited)
			Expect(limited).NotTo(BeNil())
			Expect(limited.Status).To(Equal(metav1.ConditionTrue))
			Expect(limited.Message).To(ContainSubstring("fully replicated"))
		})

		It("holds and says why when the metrics endpoint is missing", func() {
			autoscale()
			for range 3 {
				reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4)})
			}
			out := getCluster()
			Expect(out.Status.Autoscaling.DesiredReplicas).To(Equal(int32(4)))
			limited := apimeta.FindStatusCondition(out.Status.Conditions, camusdbv1alpha1.ConditionAutoscalingLimited)
			Expect(limited).NotTo(BeNil())
			Expect(limited.Message).To(ContainSubstring("traffic metrics could not be read"))
		})

		It("grows on CPU and reads it only when a CPU target is set", func() {
			autoscale()
			obj := getCluster()
			obj.Spec.Autoscaling.TargetLogEntriesPerSecond = nil
			obj.Spec.Autoscaling.TargetCPUPercent = ptr.To(int32(50))
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())

			busy := 100.0
			// The first pass records the bootstrap; CPU needs no second reading to be a signal.
			reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), cpu: &busy})
			fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 4), cpu: &busy})
			Expect(getCluster().Status.Autoscaling.DesiredReplicas).To(Equal(int32(5)))
			Expect(*getCluster().Status.Autoscaling.CPUPercentPerNode).To(Equal(int64(100)))
			// No metrics source is set, so a metrics read would have failed the measurement.
			for _, t := range fake.targets {
				Expect(t.Credentials).NotTo(BeNil(), "an autoscaled cluster called a node without credentials")
			}
		})

		It("rejects autoscaling without authentication", func() {
			obj := getCluster()
			obj.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{
				MinReplicas: 3, MaxReplicas: 6, TargetQueueDepth: ptr.To(int32(10)),
			}
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("autoscaling requires spec.auth"))
		})

		It("rejects autoscaling without a replication factor", func() {
			obj := getCluster()
			obj.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{
				MinReplicas: 3, MaxReplicas: 6, TargetLogEntriesPerSecond: ptr.To(int32(1000)),
			}
			err := k8sClient.Update(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("autoscaling requires spec.replication"))
		})

		It("rejects a factor above 3 and a minimum below 3", func() {
			obj := getCluster()
			obj.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 5}
			Expect(k8sClient.Update(ctx, obj)).NotTo(Succeed())

			obj = getCluster()
			obj.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: 3}
			obj.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{
				MinReplicas: 1, MaxReplicas: 6, TargetLogEntriesPerSecond: ptr.To(int32(1000)),
			}
			Expect(k8sClient.Update(ctx, obj)).NotTo(Succeed())
		})
	})

	It("creates no StatefulSet until the keystore exists", func() {
		obj := getCluster()
		obj.Spec.TLS.SecretRef = &corev1.LocalObjectReference{Name: name + "-later"}
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		reconcileWith(nil, fmt.Errorf("no cluster yet"))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, nn, &appsv1.StatefulSet{}))).To(BeTrue(),
			"a StatefulSet created before its keystore would deadlock in a rolling update once the keystore appeared")
		out := getCluster()
		Expect(out.Status.Phase).To(Equal(camusdbv1alpha1.PhasePending))
		ks := apimeta.FindStatusCondition(out.Status.Conditions, conditionKeystoreReady)
		Expect(ks).NotTo(BeNil())
		Expect(ks.Status).To(Equal(metav1.ConditionFalse))

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-later", Namespace: namespace},
			Data:       map[string][]byte{defaultKeystoreKey: []byte("pfx"), defaultCAKey: testCA()},
		})).To(Succeed())
		reconcileWith(nil, fmt.Errorf("no cluster yet"))
		Expect(getSTS().Spec.Template.Annotations[keystoreHashAnnotation]).NotTo(BeEmpty())
	})

	It("turns on authentication with generated secrets and calls the nodes over TLS", func() {
		obj := getCluster()
		obj.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())

		fake := reconcileUsing(&fakeCamusDB{err: fmt.Errorf("no cluster yet")})

		admin := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: adminSecretName(cluster), Namespace: namespace}, admin)).To(Succeed())
		Expect(admin.Data[adminUsernameKey]).NotTo(BeEmpty())
		Expect(admin.Data[adminPasswordKey]).NotTo(BeEmpty())
		keys := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: authKeysSecretName(cluster), Namespace: namespace}, keys)).To(Succeed())
		// CamusDB refuses to start with a token key or node secret under 32 bytes.
		Expect(len(keys.Data[tokenKeyKey])).To(BeNumerically(">=", 32))
		Expect(len(keys.Data[nodeSecretKey])).To(BeNumerically(">=", 32))

		env := map[string]corev1.EnvVar{}
		for _, e := range getSTS().Spec.Template.Spec.Containers[0].Env {
			env[e.Name] = e
		}
		Expect(env["CAMUSDB_AUTH_ENABLED"].Value).To(Equal("true"))
		for _, n := range []string{"CAMUSDB_AUTH_TOKEN_KEY", "CAMUSDB_NODE_SECRET", "CAMUSDB_BOOTSTRAP_USER", "CAMUSDB_BOOTSTRAP_PASSWORD"} {
			Expect(env[n].ValueFrom).NotTo(BeNil(), "%s is not read from a Secret", n)
			Expect(env[n].Value).To(BeEmpty(), "%s is written into the pod spec", n)
		}

		Expect(fake.targets).NotTo(BeEmpty())
		for _, t := range fake.targets {
			Expect(t.BaseURL).To(HavePrefix("https://"))
			Expect(t.ServerName).To(HaveSuffix("." + peerDomain(cluster)))
			Expect(t.RootCAs).NotTo(BeNil())
			Expect(t.Credentials.Username).To(Equal(string(admin.Data[adminUsernameKey])))
		}

		// Created once and never rewritten: the values are baked into the user catalog and tokens.
		reconcileUsing(&fakeCamusDB{err: fmt.Errorf("no cluster yet")})
		again := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: adminSecretName(cluster), Namespace: namespace}, again)).To(Succeed())
		Expect(again.Data[adminPasswordKey]).To(Equal(admin.Data[adminPasswordKey]))
	})

	It("calls the nodes over plaintext without credentials when authentication is off", func() {
		fake := reconcileUsing(&fakeCamusDB{membership: rosterOf(cluster, 3)})
		Expect(fake.targets).NotTo(BeEmpty())
		for _, t := range fake.targets {
			Expect(t.BaseURL).To(HavePrefix("http://"))
			Expect(t.Credentials).To(BeNil())
		}
	})

	It("pins the bootstrap size so a later scale does not change who founds the cluster", func() {
		reconcileWith(rosterOf(cluster, 3), nil)

		obj := getCluster()
		obj.Spec.Replicas = ptr.To(int32(5))
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		reconcileWith(rosterOf(cluster, 3), nil)

		Expect(getCluster().Status.BootstrapSize).To(Equal(int32(3)))

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: configMapName(cluster), Namespace: namespace}, cm)).To(Succeed())
		Expect(cm.Data[topologyFileName]).To(ContainSubstring("BOOTSTRAP_SIZE=3"))
		Expect(cm.Data[topologyFileName]).To(ContainSubstring("REPLICAS=5"))
	})
})
