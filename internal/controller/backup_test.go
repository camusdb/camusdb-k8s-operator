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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
	"github.com/camusdb/k8s-operator/internal/camusdb"
)

// testBackupSchedule is a full-backup schedule far enough apart that one test never sees two runs.
const testBackupSchedule = "@daily"

// Images for the tests: a release before composed backups, and two tags without a release version.
const (
	testOldImage    = "camusdb/camusdb:0.15.3"
	testDevImage    = "camusdb/camusdb:dev"
	testLatestImage = "camusdb/camusdb:latest"
)

func mustSchedule(t *testing.T, s string) cron.Schedule {
	t.Helper()
	sched, err := parseSchedule(s)
	if err != nil {
		t.Fatalf("parsing %q: %v", s, err)
	}
	return sched
}

func at(d time.Duration) *metav1.Time { return ptr.To(metav1.NewTime(t0.Add(d))) }

// A cluster with no full backup has nothing to restore, so it takes one at once rather than at the
// next schedule time, which can be a day away.
func TestFirstFullBackupRunsAtOnce(t *testing.T) {
	d := decideBackup(backupInput{full: mustSchedule(t, "0 2 * * *"), now: t0})
	if d.kind != backupFull || !d.scheduled.Equal(t0) {
		t.Fatalf("got %+v, want a full backup scheduled now", d)
	}
}

func TestFullBackupIsDueAtTheNextScheduleTime(t *testing.T) {
	// t0 is 12:00; the last full backup covered 02:00 today.
	st := camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-10 * time.Hour), LastBackupID: "b1"}
	full := mustSchedule(t, "0 2 * * *")

	d := decideBackup(backupInput{full: full, status: st, now: t0})
	if d.kind != backupNone {
		t.Fatalf("got %s before the next schedule time", d.kind)
	}
	if want := t0.Add(14 * time.Hour); !d.next.Equal(want) {
		t.Fatalf("next = %v, want %v", d.next, want)
	}

	d = decideBackup(backupInput{full: full, status: st, now: t0.Add(14 * time.Hour)})
	if d.kind != backupFull || !d.scheduled.Equal(t0.Add(14*time.Hour)) {
		t.Fatalf("got %+v, want the 02:00 full backup", d)
	}
}

// Like a CronJob, missed runs collapse into one, and that run covers the latest schedule time, so
// the cadence continues from now rather than replaying every missed time.
func TestMissedFullBackupsCollapseIntoOne(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-72 * time.Hour), LastBackupID: "b1"}
	d := decideBackup(backupInput{full: mustSchedule(t, "@hourly"), status: st, now: t0.Add(30 * time.Minute)})
	if d.kind != backupFull || !d.scheduled.Equal(t0) {
		t.Fatalf("got %+v, want one full backup for %v", d, t0)
	}
}

func TestIncrementalBackupFollowsItsOwnSchedule(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{
		LastFullScheduleTime:        at(-10 * time.Hour),
		LastIncrementalScheduleTime: at(0),
		LastBackupID:                "b1",
	}
	in := backupInput{full: mustSchedule(t, "0 2 * * *"), incremental: mustSchedule(t, "*/15 * * * *"), status: st}

	in.now = t0.Add(5 * time.Minute)
	if d := decideBackup(in); d.kind != backupNone || !d.next.Equal(t0.Add(15*time.Minute)) {
		t.Fatalf("got %+v, want nothing until 12:15", d)
	}
	in.now = t0.Add(16 * time.Minute)
	if d := decideBackup(in); d.kind != backupIncremental || !d.scheduled.Equal(t0.Add(15*time.Minute)) {
		t.Fatalf("got %+v, want the 12:15 incremental", d)
	}
}

// An incremental needs a parent. Without one, only the full schedule applies.
func TestIncrementalBackupNeedsAParent(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-10 * time.Hour)}
	d := decideBackup(backupInput{
		full: mustSchedule(t, "0 2 * * *"), incremental: mustSchedule(t, "*/15 * * * *"), status: st, now: t0,
	})
	if d.kind != backupNone {
		t.Fatalf("got %s with no parent backup", d.kind)
	}
}

// A full backup starts a new chain, so it wins when both are due.
func TestFullBackupWinsWhenBothAreDue(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{
		LastFullScheduleTime:        at(-24 * time.Hour),
		LastIncrementalScheduleTime: at(-time.Hour),
		LastBackupID:                "b1",
	}
	d := decideBackup(backupInput{
		full: mustSchedule(t, "0 12 * * *"), incremental: mustSchedule(t, "@hourly"), status: st, now: t0,
	})
	if d.kind != backupFull {
		t.Fatalf("got %s, want the full backup", d.kind)
	}
}

// A failed backup stays due. Without a backoff, a refusal that repeats would be sent on every pass.
func TestFailedBackupIsRetriedAfterABackoff(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{ConsecutiveFailures: 3, LastFailureTime: at(0)}
	full := mustSchedule(t, "0 2 * * *")

	d := decideBackup(backupInput{full: full, status: st, now: t0.Add(time.Minute)})
	if d.kind != backupNone || !d.retryAt.Equal(t0.Add(4*time.Minute)) {
		t.Fatalf("got %+v, want a retry at +4m after three failures", d)
	}
	if d := decideBackup(backupInput{full: full, status: st, now: t0.Add(4 * time.Minute)}); d.kind != backupFull {
		t.Fatalf("got %+v, want the retry once the backoff has passed", d)
	}
}

func TestBackupRetryDelayIsCapped(t *testing.T) {
	for failures, want := range map[int32]time.Duration{1: time.Minute, 2: 2 * time.Minute, 6: 30 * time.Minute, 40: 30 * time.Minute} {
		if got := backupRetryDelay(failures); got != want {
			t.Errorf("backupRetryDelay(%d) = %v, want %v", failures, got, want)
		}
	}
}

// A schedule that never matches must not make a backup look due.
func TestScheduleWithNoMatchIsNeverDue(t *testing.T) {
	st := camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	if d := decideBackup(backupInput{full: mustSchedule(t, "0 0 30 2 *"), status: st, now: t0}); d.kind != backupNone {
		t.Fatalf("got %+v from a schedule with no match", d)
	}
}

// scriptedBackups answers each node from a script, and records the order the nodes were asked in.
type scriptedBackups struct {
	fakeCamusDB
	answers map[string]error
	asked   []string
}

func (s *scriptedBackups) TakeCoordinatedBackup(_ context.Context, t camusdb.Target) (*camusdb.BackupInfo, error) {
	s.asked = append(s.asked, t.BaseURL)
	if err := s.answers[t.BaseURL]; err != nil {
		return nil, err
	}
	return &camusdb.BackupInfo{BackupID: "b-" + t.BaseURL, Type: camusdb.BackupTypeFull}, nil
}

func targets(urls ...string) []backupTarget {
	out := make([]backupTarget, 0, len(urls))
	for _, u := range urls {
		out = append(out, backupTarget{pod: u, target: camusdb.Target{BaseURL: u}})
	}
	return out
}

// Error codes as CamusDB 0.16 sends them for the refusals the tests use.
var (
	errNotCoordinator = &camusdb.BackupError{StatusCode: 421, Code: camusdb.BackupCodeNotCoordinator}
	errTopology       = &camusdb.BackupError{StatusCode: 503, Code: camusdb.BackupCodeTopologyChanged}
	errNotConfigured  = &camusdb.BackupError{StatusCode: 503, Code: camusdb.BackupCodeNotConfigured}
)

// Only the meta-partition leader takes a coordinated backup, and the others refuse at once.
func TestCoordinatedBackupFindsTheLeader(t *testing.T) {
	c := &scriptedBackups{answers: map[string]error{"a": errNotCoordinator, "b": errors.New("connection refused")}}

	info, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	if err != nil {
		t.Fatalf("runBackup: %v", err)
	}
	if info.BackupID != "b-c" || !slices.Equal(c.asked, []string{"a", "b", "c"}) {
		t.Fatalf("got %s after asking %v, want the backup from c", info.BackupID, c.asked)
	}
}

// A refusal that every node would repeat ends the attempt at once.
func TestBackupStopsOnARefusalEveryNodeWouldRepeat(t *testing.T) {
	insecure := &camusdb.BackupError{StatusCode: 500, Code: "CADB070F", Message: "the backup root is group-writable"}
	c := &scriptedBackups{answers: map[string]error{"a": insecure}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
	if err == nil || !strings.Contains(err.Error(), "CADB070F") || len(c.asked) != 1 {
		t.Fatalf("got %v after asking %v, want the refusal from a only", err, c.asked)
	}
}

func TestBackupReportsEveryNodeWhenNoneTakesIt(t *testing.T) {
	c := &scriptedBackups{answers: map[string]error{"a": errNotCoordinator, "b": errNotCoordinator}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
	if err == nil || !strings.Contains(err.Error(), "a: ") || !strings.Contains(err.Error(), "b: ") {
		t.Fatalf("got %v, want both refusals named", err)
	}
}

// Right after bootstrap no node leads the meta partition, so every node refuses, or the topology
// moves under the first attempt. That is reported as unclaimed, not as a plain failure.
func TestBackupRefusedOnlyForWantOfALeaderIsUnclaimed(t *testing.T) {
	c := &scriptedBackups{answers: map[string]error{"a": errNotCoordinator, "b": errTopology, "c": errNotCoordinator}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	var unclaimed *unclaimedBackupError
	if !errors.As(err, &unclaimed) || !strings.Contains(err.Error(), "b: ") {
		t.Fatalf("got %v, want an unclaimed backup that names every refusal", err)
	}
}

// CamusDB reports a missing share of the partitions, and a cut that transactions overtook, with
// its generic failure code, which an I/O error also uses. Another node is asked, but the result is
// a plain failure, retried after the failure backoff.
func TestBackupWithTheGenericFailureCodeIsAFailure(t *testing.T) {
	share := &camusdb.BackupError{StatusCode: 500, Code: camusdb.BackupCodeFailed,
		Message: "partition(s) 4, 5 on node camusdb-3: the node could not be reached"}
	c := &scriptedBackups{answers: map[string]error{"a": errNotCoordinator, "b": share, "c": errNotCoordinator}}

	_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b", "c"))
	var unclaimed *unclaimedBackupError
	if err == nil || errors.As(err, &unclaimed) || !strings.Contains(err.Error(), "partition(s) 4, 5") || len(c.asked) != 3 {
		t.Fatalf("got %v after asking %v, want a plain failure that names the missing share", err, c.asked)
	}
}

// A node that could not be reached, or one without the backup settings, is not a leader election.
// The result is a plain failure.
func TestBackupRefusedForAnotherCauseIsNotUnclaimed(t *testing.T) {
	for name, other := range map[string]error{"unreachable": errors.New("connection refused"), "not configured": errNotConfigured} {
		c := &scriptedBackups{answers: map[string]error{"a": errNotCoordinator, "b": other}}
		_, err := runBackup(context.Background(), c, backupFull, "", targets("a", "b"))
		var unclaimed *unclaimedBackupError
		if err == nil || errors.As(err, &unclaimed) {
			t.Fatalf("%s: got %v, want a plain failure", name, err)
		}
	}
}

// An unclaimed backup stays due, is not counted, and goes out again after a short delay instead of
// the failure backoff.
func TestUnclaimedBackupIsRetriedSoonWithoutAFailure(t *testing.T) {
	full := mustSchedule(t, "0 2 * * *")
	st := &camusdbv1alpha1.BackupStatus{}
	job := backupJob{kind: backupFull, scheduled: t0, err: &unclaimedBackupError{refusals: []string{"a: x"}}}
	applyBackupResult(st, job, t0, logr.Discard())

	if st.ConsecutiveFailures != 0 || st.LastFailureTime != nil || st.LastFullScheduleTime != nil {
		t.Fatalf("status after an unclaimed backup: %+v", st)
	}
	if !st.UnclaimedSince.Time.Equal(t0) || !st.LastUnclaimedTime.Time.Equal(t0) {
		t.Fatalf("unclaimed times: since %v, last %v", st.UnclaimedSince, st.LastUnclaimedTime)
	}
	d := decideBackup(backupInput{full: full, status: *st, now: t0.Add(time.Second)})
	if d.kind != backupNone || !d.retryAt.Equal(t0.Add(unclaimedRetryDelay)) {
		t.Fatalf("got %+v, want the retry held until %v", d, t0.Add(unclaimedRetryDelay))
	}
	if d := decideBackup(backupInput{full: full, status: *st, now: t0.Add(unclaimedRetryDelay)}); d.kind != backupFull {
		t.Fatalf("got %+v, want the full backup again after the delay", d)
	}

	// A later unclaimed attempt keeps the start of the run.
	applyBackupResult(st, job, t0.Add(time.Minute), logr.Discard())
	if !st.UnclaimedSince.Time.Equal(t0) || !st.LastUnclaimedTime.Time.Equal(t0.Add(time.Minute)) || st.ConsecutiveFailures != 0 {
		t.Fatalf("status after a second unclaimed backup: %+v", st)
	}

	// Success ends the run.
	applyBackupResult(st, backupJob{kind: backupFull, scheduled: t0, info: &camusdb.BackupInfo{BackupID: "b1"}}, t0.Add(2*time.Minute), logr.Discard())
	if st.UnclaimedSince != nil || st.LastUnclaimedTime != nil {
		t.Fatalf("status after success: %+v", st)
	}
}

// A cluster that never gets a backup coordinator is still reported, once the bound is reached.
func TestUnclaimedBackupFailsAfterTheBound(t *testing.T) {
	st := &camusdbv1alpha1.BackupStatus{UnclaimedSince: at(0), LastUnclaimedTime: at(4 * time.Minute)}
	job := backupJob{kind: backupFull, scheduled: t0, err: &unclaimedBackupError{refusals: []string{"a: x"}}}
	applyBackupResult(st, job, t0.Add(unclaimedFailAfter), logr.Discard())

	if st.ConsecutiveFailures != 1 || st.LastFailureTime == nil || !strings.Contains(st.LastFailureMessage, "a: x") {
		t.Fatalf("status after the bound: %+v", st)
	}
	if st.UnclaimedSince != nil || st.LastUnclaimedTime != nil {
		t.Fatalf("unclaimed times not cleared: %+v", st)
	}

	// From here an unclaimed attempt counts like any other failure, with the backoff.
	applyBackupResult(st, job, t0.Add(10*time.Minute), logr.Discard())
	if st.ConsecutiveFailures != 2 || st.UnclaimedSince != nil {
		t.Fatalf("status after an unclaimed backup with a failure outstanding: %+v", st)
	}
}

func TestSuccessfulFullBackupAdvancesBothAnchors(t *testing.T) {
	st := &camusdbv1alpha1.BackupStatus{ConsecutiveFailures: 2, LastFailureTime: at(-time.Hour), LastFailureMessage: "x"}
	job := backupJob{kind: backupFull, scheduled: t0, info: &camusdb.BackupInfo{BackupID: "b1", Type: camusdb.BackupTypeFull}}
	applyBackupResult(st, job, t0.Add(2*time.Minute), logr.Discard())

	if st.LastBackupID != "b1" || st.LastBackupType != camusdb.BackupTypeFull || st.ConsecutiveFailures != 0 || st.LastFailureMessage != "" {
		t.Fatalf("status after success: %+v", st)
	}
	if !st.LastFullScheduleTime.Time.Equal(t0) || !st.LastIncrementalScheduleTime.Time.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("anchors: full %v, incremental %v", st.LastFullScheduleTime, st.LastIncrementalScheduleTime)
	}
}

// The server takes a full backup in place of an incremental when the incremental cannot continue
// the chain. Status reports what was taken, and the chain continues from it.
func TestSubstitutedIncrementalIsRecordedAsFull(t *testing.T) {
	st := &camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	info := &camusdb.BackupInfo{BackupID: "b2", Type: camusdb.BackupTypeFull, ActualKind: camusdb.BackupTypeFull, SubstitutionReason: "ParentOutsideWindow"}
	applyBackupResult(st, backupJob{kind: backupIncremental, scheduled: t0, info: info}, t0, logr.Discard())

	if st.LastBackupID != "b2" || st.LastBackupType != camusdb.BackupTypeFull || !st.LastIncrementalScheduleTime.Time.Equal(t0) {
		t.Fatalf("status after substitution: %+v", st)
	}
}

func TestFailedBackupIsCounted(t *testing.T) {
	st := &camusdbv1alpha1.BackupStatus{ConsecutiveFailures: 1}
	job := backupJob{kind: backupFull, scheduled: t0, err: errors.New(strings.Repeat("x", 4000))}
	applyBackupResult(st, job, t0, logr.Discard())

	if st.ConsecutiveFailures != 2 || st.LastFailureTime == nil || st.LastFullScheduleTime != nil {
		t.Fatalf("status after failure: %+v", st)
	}
	if len(st.LastFailureMessage) > maxFailureMessage+len("Full backup: ") {
		t.Fatalf("failure message is %d bytes, want it truncated", len(st.LastFailureMessage))
	}
}

// When the parent is gone, no incremental can continue the chain, so a new chain starts with a
// full backup at once, and that is not a failure.
func TestMissingParentStartsANewChain(t *testing.T) {
	st := &camusdbv1alpha1.BackupStatus{LastFullScheduleTime: at(-time.Hour), LastBackupID: "b1"}
	missing := &camusdb.BackupError{StatusCode: 404, Code: camusdb.BackupCodeParentMissing}
	applyBackupResult(st, backupJob{kind: backupIncremental, scheduled: t0, parent: "b1", err: missing}, t0, logr.Discard())

	if st.LastBackupID != "" || st.LastFullScheduleTime != nil || st.ConsecutiveFailures != 0 {
		t.Fatalf("status after a missing parent: %+v", st)
	}
	if d := decideBackup(backupInput{full: mustSchedule(t, "0 2 * * *"), status: *st, now: t0}); d.kind != backupFull {
		t.Fatalf("got %s, want a full backup at once", d.kind)
	}
}

func backupCluster() *camusdbv1alpha1.CamusDBCluster {
	c := testCluster(3)
	c.Spec.Auth = &camusdbv1alpha1.AuthSpec{}
	c.Spec.Backup = &camusdbv1alpha1.BackupSpec{
		Schedule: testBackupSchedule,
		Volume:   camusdbv1alpha1.BackupVolumeSpec{Claim: &camusdbv1alpha1.VolumeSpec{Size: resource.MustParse("10Gi")}},
	}
	return c
}

func TestBackupSettings(t *testing.T) {
	c := backupCluster()
	c.Spec.Backup.Retention = &camusdbv1alpha1.BackupRetentionSpec{
		MaxChains: ptr.To(int32(7)),
		MaxAge:    &metav1.Duration{Duration: 168 * time.Hour},
		MaxBytes:  ptr.To(resource.MustParse("1Gi")),
	}
	kahuna := generatedConfig(c)["kahuna"].(map[string]any)
	for key, want := range map[string]any{
		"backup_dir":                       "/backup/" + c.Name,
		"backup_cluster_id":                c.Namespace + "/" + c.Name,
		"backup_mac_key_file":              "/etc/camusdb/backup-mac/key",
		"backup_gc_interval_seconds":       0,
		"backup_retention_max_chains":      int32(7),
		"backup_retention_max_age_seconds": int64(604800),
		"backup_retention_max_bytes":       int64(1073741824),
	} {
		if got := kahuna[key]; got != want {
			t.Errorf("kahuna.%s = %#v, want %#v", key, got, want)
		}
	}

	c.Spec.Backup.ClusterID = "prod/camusdb"
	if got := generatedConfig(c)["kahuna"].(map[string]any)["backup_cluster_id"]; got != "prod/camusdb" {
		t.Errorf("kahuna.backup_cluster_id = %v, want the spec's", got)
	}

	c.Spec.Backup = nil
	for key := range generatedConfig(c)["kahuna"].(map[string]any) {
		if strings.Contains(key, "backup") {
			t.Errorf("a cluster without backups got kahuna.%s", key)
		}
	}
}

func TestBackupVolumes(t *testing.T) {
	c := backupCluster()
	c.Spec.Backup.Volume = camusdbv1alpha1.BackupVolumeSpec{ClaimName: ptr.To("shared-nfs")}
	c.Spec.Backup.MACKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "my-mac"}, Key: "k",
	}

	var claim, mac *corev1.Volume
	for _, v := range podVolumes(c) {
		switch v.Name {
		case backupVolumeName:
			claim = &v
		case backupMACVolumeName:
			mac = &v
		}
	}
	if claim == nil || claim.PersistentVolumeClaim.ClaimName != "shared-nfs" {
		t.Fatalf("backup volume: %+v", claim)
	}
	if mac == nil || mac.Secret.SecretName != "my-mac" || mac.Secret.Items[0].Key != "k" || mac.Secret.Items[0].Path != backupMACFileName {
		t.Fatalf("MAC volume: %+v", mac)
	}
}

// The operator's claim is shared by every node, so it is ReadWriteMany unless the spec says
// otherwise.
func TestBackupClaimDefaultsToReadWriteMany(t *testing.T) {
	c := backupCluster()
	pvc := desiredBackupClaim(c)
	if pvc.Name != c.Name+"-backup" || !slices.Equal(pvc.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}) {
		t.Fatalf("claim %s with access modes %v", pvc.Name, pvc.Spec.AccessModes)
	}
	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) {
		t.Fatalf("access modes %v, want the spec's", got)
	}
}

// Nothing shares a single node's claim, and most StorageClasses cannot provision ReadWriteMany, so
// a single-node cluster asks for ReadWriteOnce.
func TestSingleNodeBackupClaimDefaultsToReadWriteOnce(t *testing.T) {
	c := backupCluster()
	c.Spec.Replicas = ptr.To(int32(1))
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) {
		t.Fatalf("access modes %v, want ReadWriteOnce", got)
	}
	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	if got := desiredBackupClaim(c).Spec.AccessModes; !slices.Equal(got, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}) {
		t.Fatalf("access modes %v, want the spec's", got)
	}
}

// An unbound claim holds no data, so it follows the spec. A bound one is never replaced.
func TestUnboundBackupClaimFollowsTheSpec(t *testing.T) {
	c := backupCluster()
	existing := desiredBackupClaim(c)
	existing.Status.Phase = corev1.ClaimPending
	// The API server fills in the default class when the spec names none.
	existing.Spec.StorageClassName = ptr.To("standard")

	if claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that matches the spec")
	}

	c.Spec.Backup.Volume.Claim.StorageClassName = ptr.To("nfs")
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with a different storage class")
	}
	c.Spec.Backup.Volume.Claim.StorageClassName = ptr.To("standard")

	c.Spec.Backup.Volume.Claim.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with different access modes")
	}
	c.Spec.Backup.Volume.Claim.AccessModes = nil

	c.Spec.Backup.Volume.Claim.Size = resource.MustParse("20Gi")
	if !claimNeedsReplacing(existing, desiredBackupClaim(c)) {
		t.Fatal("kept a claim with a different size")
	}

	bound := existing.DeepCopy()
	bound.Status.Phase = corev1.ClaimBound
	if claimNeedsReplacing(bound, desiredBackupClaim(c)) {
		t.Fatal("replaced a bound claim")
	}
	prebound := existing.DeepCopy()
	prebound.Spec.VolumeName = "pv-1"
	if claimNeedsReplacing(prebound, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that has a volume")
	}
	deleting := existing.DeepCopy()
	deleting.DeletionTimestamp = ptr.To(metav1.NewTime(t0))
	if claimNeedsReplacing(deleting, desiredBackupClaim(c)) {
		t.Fatal("replaced a claim that is already being deleted")
	}
}

// Events written through the events.k8s.io API carry their time in eventTime or series, not in
// lastTimestamp.
func TestEventTimeUsesTheNewestField(t *testing.T) {
	e := &corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(t0)}}
	if got := eventTime(e); !got.Equal(t0) {
		t.Fatalf("got %v, want the creation time", got)
	}
	e.LastTimestamp = metav1.NewTime(t0.Add(time.Minute))
	e.EventTime = metav1.NewMicroTime(t0.Add(2 * time.Minute))
	e.Series = &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(t0.Add(3 * time.Minute))}
	if got := eventTime(e); !got.Equal(t0.Add(3 * time.Minute)) {
		t.Fatalf("got %v, want the series time", got)
	}
}

func readyPod(c *camusdbv1alpha1.CamusDBCluster, ordinal int32, hash string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName(c, ordinal), Annotations: map[string]string{configHashAnnotation: hash}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: containerName, Image: c.Spec.Image}}},
		Status: corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}},
	}
}

// A backup waits for every node to run the backup flags: the coordinated backup goes to the
// meta-partition leader, which can be any node.
func TestBackupWaitsForTheRollout(t *testing.T) {
	c := backupCluster()
	c.Status.Bootstrapped = true
	rc, err := render(c)
	if err != nil {
		t.Fatal(err)
	}
	hash := configHash(rc.script, rc.config)
	m := rosterOf(c, 3)

	pods := []corev1.Pod{readyPod(c, 0, hash), readyPod(c, 1, "old"), readyPod(c, 2, hash)}
	if reason := backupBlocked(c, m, pods, rc); !strings.Contains(reason, "current configuration") {
		t.Fatalf("blocked = %q, want the rollout named", reason)
	}
	pods[1] = readyPod(c, 1, hash)
	if reason := backupBlocked(c, m, pods, rc); reason != "" {
		t.Fatalf("blocked = %q on a steady cluster", reason)
	}
	if reason := backupBlocked(c, m, pods[:2], rc); reason == "" {
		t.Fatal("not blocked with a pod missing")
	}
	if reason := backupBlocked(c, nil, pods, rc); reason == "" {
		t.Fatal("not blocked with an unknown roster")
	}
}

// The config hash does not cover the image, so an image rollout is checked on its own: under a
// replication factor the coordinator asks every node for a share of the partitions.
func TestBackupWaitsForTheImageRollout(t *testing.T) {
	c := backupCluster()
	c.Status.Bootstrapped = true
	rc, err := render(c)
	if err != nil {
		t.Fatal(err)
	}
	hash := configHash(rc.script, rc.config)
	m := rosterOf(c, 3)

	pods := []corev1.Pod{readyPod(c, 0, hash), readyPod(c, 1, hash), readyPod(c, 2, hash)}
	pods[2].Spec.Containers[0].Image = testOldImage
	if reason := backupBlocked(c, m, pods, rc); !strings.Contains(reason, "does not run image") {
		t.Fatalf("blocked = %q, want the image rollout named", reason)
	}
}

func TestImageVersion(t *testing.T) {
	for image, want := range map[string][3]int{
		"camusdb/camusdb:0.16.3":                      {0, 16, 3},
		"registry.local:5000/camusdb/camusdb:v0.15.2": {0, 15, 2},
		"camusdb/camusdb:0.16.0-rc.1@sha256:abc":      {0, 16, 0},
	} {
		if got, ok := imageVersion(image); !ok || got != want {
			t.Errorf("imageVersion(%q) = %v, %v; want %v", image, got, ok, want)
		}
	}
	for _, image := range []string{
		"camusdb/camusdb", testLatestImage, testDevImage,
		"registry.local:5000/camusdb/camusdb", "camusdb/camusdb@sha256:abc", "camusdb/camusdb:0.16",
	} {
		if got, ok := imageVersion(image); ok {
			t.Errorf("imageVersion(%q) = %v, want no version", image, got)
		}
	}
}

// Only a server before 0.16.0, on a cluster that can have more nodes than the factor, takes a
// backup that misses partitions. An image without a release tag is not refused.
func TestBackupUnsupportedOnlyBeforeComposedBackups(t *testing.T) {
	cases := []struct {
		name     string
		image    string
		replicas int32
		factor   int32
		maxNodes int32
		refused  bool
	}{
		{"old server, placed", testOldImage, 5, 3, 0, true},
		{"old server, autoscaled past the factor", testOldImage, 3, 3, 6, true},
		{"old server, factor covers every node", testOldImage, 3, 3, 0, false},
		{"old server, no factor", testOldImage, 5, 0, 0, false},
		{"composing server", "camusdb/camusdb:0.16.0", 5, 3, 0, false},
		{"newer server", "camusdb/camusdb:0.16.3", 5, 3, 0, false},
		{"unknown version", testLatestImage, 5, 3, 0, false},
	}
	for _, tc := range cases {
		c := backupCluster()
		c.Spec.Image = tc.image
		c.Spec.Replicas = ptr.To(tc.replicas)
		if tc.factor > 0 {
			c.Spec.Replication = &camusdbv1alpha1.ReplicationSpec{Factor: tc.factor}
		}
		if tc.maxNodes > 0 {
			c.Spec.Autoscaling = &camusdbv1alpha1.AutoscalingSpec{MinReplicas: 3, MaxReplicas: tc.maxNodes}
		}
		reason := backupUnsupported(c)
		if (reason != "") != tc.refused {
			t.Errorf("%s: reason %q, want refused=%v", tc.name, reason, tc.refused)
		}
		if tc.refused && !strings.Contains(reason, "CamusDB 0.16.0 or later") {
			t.Errorf("%s: reason %q does not name the minimum", tc.name, reason)
		}
	}
}
