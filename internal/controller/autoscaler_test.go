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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
	"github.com/camusdb/k8s-operator/internal/camusdb"
)

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func autoscaleSpec() *camusdbv1alpha1.AutoscalingSpec {
	return &camusdbv1alpha1.AutoscalingSpec{
		MinReplicas:                   3,
		MaxReplicas:                   7,
		TargetLogEntriesPerSecond:     ptr.To(int32(1000)),
		ScaleUpStabilizationSeconds:   ptr.To(int32(60)),
		ScaleDownStabilizationSeconds: ptr.To(int32(300)),
	}
}

func writes(v float64) trafficSignal {
	return trafficSignal{entriesPerNode: ptr.To(v), queuePerNode: ptr.To(0.0)}
}

// step runs decisions at the given offsets from t0, feeding each one the previous state, and
// returns the final state.
func step(t *testing.T, spec *camusdbv1alpha1.AutoscalingSpec, start int32, sig trafficSignal, offsets ...time.Duration) autoscaleDecision {
	t.Helper()
	var prev *camusdbv1alpha1.AutoscalingStatus
	var d autoscaleDecision
	for _, off := range offsets {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: start, signal: sig, converged: true, now: t0.Add(off),
		})
		prev = &d.status
	}
	return d
}

func TestAutoscaleAddsOneNodeAfterTheScaleUpWindow(t *testing.T) {
	// Twice the target per node asks for twice the nodes, but only one is added per step.
	d := step(t, autoscaleSpec(), 4, writes(2000), 0)
	if d.status.DesiredReplicas != 4 || d.status.RecommendedReplicas != 7 {
		t.Fatalf("first pass: desired %d recommended %d, want 4 and 7 (capped by max)",
			d.status.DesiredReplicas, d.status.RecommendedReplicas)
	}
	if d.status.AboveTargetSince == nil {
		t.Fatal("the scale-up window did not start")
	}

	d = step(t, autoscaleSpec(), 4, writes(2000), 0, 30*time.Second)
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("scaled up before the window elapsed: desired %d", d.status.DesiredReplicas)
	}

	d = step(t, autoscaleSpec(), 4, writes(2000), 0, 30*time.Second, 60*time.Second)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("desired %d after the window, want 5 (one node per step)", d.status.DesiredReplicas)
	}
	if d.status.AboveTargetSince != nil || d.status.LastScaleTime == nil {
		t.Fatal("a step must restart the window and record when it happened")
	}
}

func TestAutoscaleNeedsTheWholeScaleDownWindow(t *testing.T) {
	d := step(t, autoscaleSpec(), 5, writes(100), 0, 299*time.Second)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("scaled down early: desired %d", d.status.DesiredReplicas)
	}
	d = step(t, autoscaleSpec(), 5, writes(100), 0, 300*time.Second)
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("desired %d after the window, want 4", d.status.DesiredReplicas)
	}
}

func TestAutoscaleSpikeInsideTheWindowRestartsIt(t *testing.T) {
	spec := autoscaleSpec()
	d := decideAutoscale(autoscaleInput{spec: spec, initial: 5, signal: writes(100), converged: true, now: t0})
	// Load returns to target: the below-target streak is broken.
	d = decideAutoscale(autoscaleInput{spec: spec, prev: &d.status, initial: 5, signal: writes(1000), converged: true, now: t0.Add(200 * time.Second)})
	if d.status.BelowTargetSince != nil {
		t.Fatal("a reading at target did not reset the scale-down window")
	}
	d = decideAutoscale(autoscaleInput{spec: spec, prev: &d.status, initial: 5, signal: writes(100), converged: true, now: t0.Add(300 * time.Second)})
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("scaled down on a window that was interrupted: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleDeadBandHoldsSteady(t *testing.T) {
	d := step(t, autoscaleSpec(), 4, writes(1050), 0, time.Hour)
	if d.status.DesiredReplicas != 4 || d.status.RecommendedReplicas != 4 {
		t.Fatalf("a reading inside the tolerance moved the cluster: %+v", d.status)
	}
}

func TestAutoscaleRespectsBounds(t *testing.T) {
	d := step(t, autoscaleSpec(), 3, writes(0), 0, time.Hour)
	if d.status.DesiredReplicas != 3 {
		t.Fatalf("shrank below minReplicas: %d", d.status.DesiredReplicas)
	}

	d = step(t, autoscaleSpec(), 7, writes(50000), 0, time.Hour)
	if d.status.DesiredReplicas != 7 {
		t.Fatalf("grew past maxReplicas: %d", d.status.DesiredReplicas)
	}
	if !d.limited {
		t.Fatal("traffic above maxReplicas must be reported as limited")
	}

	// A bound edited below the current size applies at once.
	spec := autoscaleSpec()
	spec.MaxReplicas = 4
	prev := &camusdbv1alpha1.AutoscalingStatus{DesiredReplicas: 6}
	d = decideAutoscale(autoscaleInput{spec: spec, prev: prev, initial: 3, signal: writes(1000), converged: true, now: t0})
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("desired %d after maxReplicas dropped to 4", d.status.DesiredReplicas)
	}
}

func TestAutoscaleHoldsWhileTheRosterConverges(t *testing.T) {
	spec := autoscaleSpec()
	prev := &camusdbv1alpha1.AutoscalingStatus{DesiredReplicas: 5, AboveTargetSince: ptr.To(metav1.NewTime(t0))}
	d := decideAutoscale(autoscaleInput{spec: spec, prev: prev, initial: 3, signal: writes(5000), converged: false, now: t0.Add(time.Hour)})
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("stacked a step on one still in flight: desired %d", d.status.DesiredReplicas)
	}
	if d.status.AboveTargetSince != nil {
		t.Fatal("a window must not accumulate while the previous step is still converging")
	}
	if d.limited {
		t.Fatal("a scale in progress is not a limit")
	}
}

// A node whose pod can never start must not pin the cluster at a size it never reached: once the
// traffic no longer needs it, the step that asked for it is given back.
func TestAutoscaleGivesBackANodeThatNeverJoined(t *testing.T) {
	spec := autoscaleSpec()
	prev := &camusdbv1alpha1.AutoscalingStatus{DesiredReplicas: 5}
	const blocked = "camusdb-4 cannot be scheduled"
	var d autoscaleDecision
	for _, off := range []time.Duration{0, 299 * time.Second, 300 * time.Second} {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: 4, signal: writes(100), joinPending: true,
			joinBlocked: blocked, now: t0.Add(off),
		})
		prev = &d.status
		if off < 300*time.Second && d.status.DesiredReplicas != 5 {
			t.Fatalf("gave the node back before the scale-down window elapsed: desired %d", d.status.DesiredReplicas)
		}
	}
	if d.status.DesiredReplicas != 4 {
		t.Fatalf("desired %d after the window, want 4", d.status.DesiredReplicas)
	}
	if !d.limited || d.reason != blocked {
		t.Fatalf("the stuck pod was not reported: limited=%v reason=%q", d.limited, d.reason)
	}
}

func TestAutoscaleDoesNotStackOnANodeThatNeverJoined(t *testing.T) {
	spec := autoscaleSpec()
	prev := &camusdbv1alpha1.AutoscalingStatus{DesiredReplicas: 5}
	var d autoscaleDecision
	for _, off := range []time.Duration{0, time.Hour} {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: 4, signal: writes(5000), joinPending: true,
			joinBlocked: "camusdb-4 cannot be scheduled", now: t0.Add(off),
		})
		prev = &d.status
	}
	if d.status.DesiredReplicas != 5 || d.status.RecommendedReplicas != 5 {
		t.Fatalf("stacked a step on one that has not landed: desired %d recommended %d",
			d.status.DesiredReplicas, d.status.RecommendedReplicas)
	}
	if d.status.AboveTargetSince != nil {
		t.Fatal("a scale-up window accumulated while the previous step had not landed")
	}
	if !d.limited {
		t.Fatal("the stuck pod was not reported")
	}
}

func TestJoinBlockedReason(t *testing.T) {
	starting := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "camusdb-4"}}
	if r := joinBlockedReason(starting); r != "" {
		t.Fatalf("a pod that may still be starting was reported as blocked: %q", r)
	}
	unschedulable := starting.DeepCopy()
	unschedulable.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
		Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available",
	}}
	if r := joinBlockedReason(unschedulable); !strings.Contains(r, "cannot be scheduled: 0/1 nodes are available") {
		t.Fatalf("unschedulable pod reported as %q", r)
	}
	crashing := starting.DeepCopy()
	crashing.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: waitReasonCrashLoopBackOff, Message: "back-off 40s"},
	}}}
	if r := joinBlockedReason(crashing); !strings.Contains(r, "CrashLoopBackOff: back-off 40s") {
		t.Fatalf("crash-looping pod reported as %q", r)
	}
}

func TestAutoscaleNeverShrinksOnAPartialSignal(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetQueueDepth = ptr.To(int32(10))
	// Writes are known and low; the queue depth could not be read and might be the high one.
	sig := trafficSignal{entriesPerNode: ptr.To(10.0)}
	d := step(t, spec, 5, sig, 0, time.Hour)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("shrank with a signal missing: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleGrowsOnEitherSignal(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetQueueDepth = ptr.To(int32(10))
	sig := trafficSignal{entriesPerNode: ptr.To(10.0), queuePerNode: ptr.To(40.0)}
	d := step(t, spec, 4, sig, 0, time.Minute)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("a queue four times its target did not add a node: desired %d", d.status.DesiredReplicas)
	}
}

func TestAutoscaleHoldsAtTheReplicationCeiling(t *testing.T) {
	spec := autoscaleSpec()
	var prev *camusdbv1alpha1.AutoscalingStatus
	var d autoscaleDecision
	for _, off := range []time.Duration{0, time.Hour} {
		d = decideAutoscale(autoscaleInput{
			spec: spec, prev: prev, initial: 3, signal: writes(5000), converged: true,
			growthCeiling: 3, ceilingReason: "fully replicated", now: t0.Add(off),
		})
		prev = &d.status
	}
	if d.status.DesiredReplicas != 3 {
		t.Fatalf("grew a fully replicated cluster past the replication cap: desired %d", d.status.DesiredReplicas)
	}
	if !d.limited || d.reason != "fully replicated" {
		t.Fatalf("the ceiling was not reported: limited=%v reason=%q", d.limited, d.reason)
	}
}

func TestAutoscaleWaitsForASignal(t *testing.T) {
	d := step(t, autoscaleSpec(), 4, trafficSignal{}, 0, time.Hour)
	if d.status.DesiredReplicas != 4 || !d.limited {
		t.Fatalf("acted with no signal, or did not say so: %+v limited=%v", d.status, d.limited)
	}
}

func TestSamplerComputesRatesAndSurvivesRestarts(t *testing.T) {
	samples := map[types.UID]podSample{}

	if _, ok := observeSample(samples, "pod", 1000, 0); ok {
		t.Fatal("a first reading has no baseline and must not produce a rate")
	}
	// Too soon: no new rate yet, and the baseline is kept.
	if _, ok := observeSample(samples, "pod", 1500, 5_000); ok {
		t.Fatal("a reading inside the minimum interval produced a rate")
	}
	rate, ok := observeSample(samples, "pod", 3000, 20_000)
	if !ok || rate != 100 {
		t.Fatalf("rate %v ok=%v, want 100 writes/s over 20 s", rate, ok)
	}
	// Reused, not recomputed, inside the interval.
	if r, ok := observeSample(samples, "pod", 3100, 21_000); !ok || r != 100 {
		t.Fatalf("cached rate %v ok=%v, want 100", r, ok)
	}

	// The container restarted under the same pod: the counter went backwards.
	if _, ok := observeSample(samples, "pod", 10, 40_000); ok {
		t.Fatal("a counter reset produced a rate (it would be negative)")
	}
	rate, ok = observeSample(samples, "pod", 1010, 50_000)
	if !ok || rate != 100 {
		t.Fatalf("rate after reset %v ok=%v, want 100", rate, ok)
	}
}

func TestPlacementFindsFullyReplicatedDataPartitions(t *testing.T) {
	p := &camusdb.Placement{Partitions: []camusdb.PartitionPlacement{
		// The system partition always spans every node; it is not a violation.
		{PartitionID: 0, State: camusdb.PartitionStateActive},
		{PartitionID: 1, State: camusdb.PartitionStateActive, Replicas: []camusdb.ReplicaPlacement{{Endpoint: "a"}}},
		{PartitionID: 2, State: camusdb.PartitionStateActive},
		{PartitionID: 3, State: "Removed"},
	}}
	full := p.FullyReplicated()
	if len(full) != 1 || full[0] != 2 {
		t.Fatalf("fully replicated partitions %v, want [2]", full)
	}
}

func TestMetricsSumAcrossTagSets(t *testing.T) {
	m := &camusdb.Metrics{Rows: []camusdb.MetricRow{
		{Metric: metricLogEntries, Kind: camusdb.MetricKindCounter, Tags: "class=ordinary", Total: ptr.To(30.0)},
		{Metric: metricLogEntries, Kind: camusdb.MetricKindCounter, Tags: "class=terminal", Total: ptr.To(12.0)},
		{Metric: metricClientQueueDepth, Kind: camusdb.MetricKindGauge, Tags: "partition_id=1", Last: ptr.To(4.0)},
		{Metric: metricClientQueueDepth, Kind: camusdb.MetricKindGauge, Tags: "partition_id=2", Last: ptr.To(6.0)},
	}}
	if v, ok := m.CounterTotal(metricLogEntries); !ok || v != 42 {
		t.Fatalf("log entries %v ok=%v, want 42", v, ok)
	}
	if v, ok := m.GaugeSum(metricClientQueueDepth); !ok || v != 10 {
		t.Fatalf("queue %v ok=%v, want 10", v, ok)
	}
}

// CPU is a signal in its own right: it is the only one that sees read load, so it must be able to
// grow a cluster whose write signals are quiet.
func TestAutoscaleGrowsOnCPU(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetCPUPercent = ptr.To(int32(60))
	sig := trafficSignal{entriesPerNode: ptr.To(10.0), queuePerNode: ptr.To(0.0), cpuPerNode: ptr.To(90.0)}
	d := step(t, spec, 4, sig, 0, time.Minute)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("CPU at 1.5x its target did not add a node: desired %d", d.status.DesiredReplicas)
	}
	if d.status.CPUPercentPerNode == nil || *d.status.CPUPercentPerNode != 90 {
		t.Fatalf("cpuPercentPerNode = %v, want 90", d.status.CPUPercentPerNode)
	}
}

// An unreadable CPU signal blocks a shrink like any other unknown signal: the node whose CPU could
// not be read may be the busy one.
func TestAutoscaleNeverShrinksWithCPUUnknown(t *testing.T) {
	spec := autoscaleSpec()
	spec.TargetCPUPercent = ptr.To(int32(60))
	sig := trafficSignal{entriesPerNode: ptr.To(10.0), queuePerNode: ptr.To(0.0)}
	d := step(t, spec, 5, sig, 0, time.Hour)
	if d.status.DesiredReplicas != 5 {
		t.Fatalf("shrank with the CPU signal missing: desired %d", d.status.DesiredReplicas)
	}
}

func TestDesiredReplicasFollowsTheAutoscaler(t *testing.T) {
	cluster := testCluster(5)
	cluster.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{MinReplicas: 3, MaxReplicas: 4}
	if got := desiredReplicas(cluster); got != 4 {
		t.Fatalf("starting size %d, want spec.replicas clamped to maxReplicas (4)", got)
	}
	cluster.Status.Autoscaling = &camusdbv1alpha1.AutoscalingStatus{DesiredReplicas: 3}
	if got := desiredReplicas(cluster); got != 3 {
		t.Fatalf("desired %d, want the autoscaler's choice (3)", got)
	}
	cluster.Spec.Autoscaling = nil
	if got := desiredReplicas(cluster); got != 5 {
		t.Fatalf("desired %d after autoscaling was removed, want spec.replicas (5)", got)
	}
}
