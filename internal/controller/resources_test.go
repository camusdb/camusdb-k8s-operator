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
	"fmt"
	"testing"

	"k8s.io/utils/ptr"
)

func TestPodsKeepServingBeforeTheyAreToldToStop(t *testing.T) {
	cluster := testCluster(3)
	r, err := render(cluster)
	if err != nil {
		t.Fatal(err)
	}

	sts := desiredStatefulSet(cluster, 0, r, "")
	lifecycle := sts.Spec.Template.Spec.Containers[0].Lifecycle
	if lifecycle == nil || lifecycle.PreStop == nil || lifecycle.PreStop.Sleep == nil {
		t.Fatalf("the CamusDB container has no preStop sleep: %+v", lifecycle)
	}
	if got := lifecycle.PreStop.Sleep.Seconds; got != preStopSleepSeconds {
		t.Errorf("preStop sleeps %d seconds, want %d", got, preStopSleepSeconds)
	}
}

func TestPreStopSleepLeavesTheServerMostOfTheGracePeriod(t *testing.T) {
	for _, tc := range []struct {
		grace *int64
		want  int64 // 0 means no hook
	}{
		{grace: nil, want: 5},
		{grace: ptr.To[int64](120), want: 5},
		{grace: ptr.To[int64](15), want: 5},
		{grace: ptr.To[int64](12), want: 4},
		{grace: ptr.To[int64](6), want: 2},
		{grace: ptr.To[int64](2), want: 0},
		{grace: ptr.To[int64](0), want: 0},
	} {
		cluster := testCluster(3)
		cluster.Spec.TerminationGracePeriodSeconds = tc.grace

		lifecycle := containerLifecycle(cluster)
		var got int64
		if lifecycle != nil {
			got = lifecycle.PreStop.Sleep.Seconds
		}
		if got != tc.want {
			grace := "unset"
			if tc.grace != nil {
				grace = fmt.Sprint(*tc.grace)
			}
			t.Errorf("grace period %s: preStop sleeps %d seconds, want %d", grace, got, tc.want)
		}
	}
}
