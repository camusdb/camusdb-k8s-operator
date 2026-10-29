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

// Package camusdb is a thin client for the CamusDB server's cluster endpoints. The operator gates
// every membership-changing step on what the cluster itself reports, rather than on what Kubernetes
// believes, because the Raft roster is the authoritative record of who is a member — a pod can be
// Running and not be one, and a roster entry can outlive its pod.
package camusdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Role values reported by the server. Only Voters count toward quorum.
const (
	RoleVoter     = "Voter"
	RoleLearner   = "Learner"
	RoleLeaving   = "Leaving"
	RoleNotMember = "NotMember"
)

// Member is one entry of the committed Raft roster.
type Member struct {
	Endpoint      string `json:"endpoint"`
	NodeID        int32  `json:"nodeId"`
	Role          string `json:"role"`
	JoinedVersion int64  `json:"joinedVersion"`
}

// Membership is the response of GET /v1/cluster/membership.
type Membership struct {
	MembershipVersion int64    `json:"membershipVersion"`
	Members           []Member `json:"members"`
	LocalRole         string   `json:"localRole"`
	Initialized       bool     `json:"initialized"`
}

// Voters returns the members that count toward quorum.
func (m *Membership) Voters() []Member {
	var out []Member
	for _, mem := range m.Members {
		if mem.Role == RoleVoter {
			out = append(out, mem)
		}
	}
	return out
}

// HasVoter reports whether the endpoint is a committed Voter — the condition the operator waits
// for before treating a newly added node as carrying its share of the cluster.
func (m *Membership) HasVoter(endpoint string) bool {
	for _, mem := range m.Members {
		if mem.Endpoint == endpoint && mem.Role == RoleVoter {
			return true
		}
	}
	return false
}

// Contains reports whether the endpoint appears in the roster in any role. Scale-down waits for
// this to go false, which is how an eviction is observed.
func (m *Membership) Contains(endpoint string) bool {
	for _, mem := range m.Members {
		if mem.Endpoint == endpoint {
			return true
		}
	}
	return false
}

// Health is the response of GET /v1/cluster/health.
type Health struct {
	Ready       bool   `json:"ready"`
	Initialized bool   `json:"initialized"`
	LocalRole   string `json:"localRole"`
}

// Leave outcomes reported by POST /v1/cluster/leave.
const (
	// OutcomeCommitted means the RemoveMember entry is committed: the node is out of the roster
	// and surviving members no longer count it toward quorum.
	OutcomeCommitted = "Committed"
	// OutcomeNotAMember means the endpoint was already absent. Success, for idempotency.
	OutcomeNotAMember = "NotAMember"
	// OutcomeRefusedInsufficientVoters means the removal would leave zero voters. Permanent:
	// retrying can never succeed while the roster is unchanged.
	OutcomeRefusedInsufficientVoters = "RefusedInsufficientVoters"
	// OutcomeRefused is CamusDB's own refusal, before the request reaches the cluster: the caller
	// is not allowed to administer it. Permanent until the credentials change.
	OutcomeRefused = "Refused"
)

// LeaveResult is the response of POST /v1/cluster/leave.
type LeaveResult struct {
	Left              bool   `json:"left"`
	Drained           bool   `json:"drained"`
	Outcome           string `json:"outcome"`
	MembershipVersion int64  `json:"membershipVersion"`
	Retryable         bool   `json:"retryable"`
	Reason            string `json:"reason"`
}

// Removed reports whether the node is out of the roster — either because this call committed the
// removal or because it was already gone. Both mean the pod is safe to stop.
func (l *LeaveResult) Removed() bool {
	return l.Outcome == OutcomeCommitted || l.Outcome == OutcomeNotAMember
}

// PartitionStateActive is the state of a partition that serves traffic, as reported by
// GET /v1/cluster/placement.
const PartitionStateActive = "Active"

// ReplicaPlacement is one replica of a partition as GET /v1/cluster/placement reports it.
type ReplicaPlacement struct {
	Endpoint string `json:"endpoint"`
	Role     string `json:"role"`
}

// PartitionPlacement is one partition's committed replica set. An empty Replicas list means full
// replication: every roster Voter hosts the partition.
type PartitionPlacement struct {
	PartitionID                int32              `json:"partitionId"`
	State                      string             `json:"state"`
	EffectiveReplicationFactor int32              `json:"effectiveReplicationFactor"`
	Replicas                   []ReplicaPlacement `json:"replicas"`
}

// Placement is the response of GET /v1/cluster/placement.
type Placement struct {
	ReplicationFactor int32                `json:"replicationFactor"`
	RebalancerEnabled bool                 `json:"rebalancerEnabled"`
	Initialized       bool                 `json:"initialized"`
	Partitions        []PartitionPlacement `json:"partitions"`
}

// FullyReplicated returns the active data partitions that have no replica set. Under a replication
// factor these are the partitions that grow by one copy for every node added, because the server
// never converts them to placed ranges.
func (p *Placement) FullyReplicated() []int32 {
	var out []int32
	for _, part := range p.Partitions {
		// Partition 0 is the system partition, which always spans every node by design.
		if part.PartitionID > 0 && part.State == PartitionStateActive && len(part.Replicas) == 0 {
			out = append(out, part.PartitionID)
		}
	}
	return out
}

// Metric kinds reported by GET /v1/dashboard/metrics. CamusDB spells them in lower case.
const (
	MetricKindCounter = "counter"
	MetricKindGauge   = "gauge"
)

// MetricRow is one instrument-and-tag-set row of GET /v1/dashboard/metrics. Counters carry their
// cumulative sum in Total; gauges carry the sampled value in Last.
type MetricRow struct {
	Metric string   `json:"metric"`
	Tags   string   `json:"tags"`
	Kind   string   `json:"kind"`
	Count  int64    `json:"count"`
	Total  *float64 `json:"total"`
	Last   *float64 `json:"last"`
}

// Metrics is the response of GET /v1/dashboard/metrics.
type Metrics struct {
	Rows []MetricRow `json:"rows"`
	// MonotonicMs is the node's monotonic clock at sampling time. Rates are computed against it
	// rather than the operator's clock, so request latency does not distort them.
	MonotonicMs int64 `json:"monotonicMs"`
}

// CounterTotal sums a counter across every tag set.
func (m *Metrics) CounterTotal(metric string) (float64, bool) {
	var sum float64
	found := false
	for _, r := range m.Rows {
		if r.Metric == metric && strings.EqualFold(r.Kind, MetricKindCounter) && r.Total != nil {
			sum += *r.Total
			found = true
		}
	}
	return sum, found
}

// GaugeSum sums a gauge's latest value across every tag set.
func (m *Metrics) GaugeSum(metric string) (float64, bool) {
	var sum float64
	found := false
	for _, r := range m.Rows {
		if r.Metric == metric && strings.EqualFold(r.Kind, MetricKindGauge) && r.Last != nil {
			sum += *r.Last
			found = true
		}
	}
	return sum, found
}

// CPUStats is the cpu block of GET /v1/admin/stats.
type CPUStats struct {
	// ProcessCPUPercent is the server process's share of the whole node, 0 to 100, averaged over
	// ProcessCPUWindowMs. -1 when the node could not read it.
	ProcessCPUPercent  float64 `json:"processCpuPercent"`
	ProcessCPUWindowMs int64   `json:"processCpuWindowMs"`
	ProcessorCount     int32   `json:"processorCount"`
}

// NodeStats is the response of GET /v1/admin/stats.
type NodeStats struct {
	Status string    `json:"status"`
	CPU    *CPUStats `json:"cpu"`
}

// Backup types reported by the server in BackupInfo.Type.
const (
	BackupTypeFull        = "Full"
	BackupTypeIncremental = "Incremental"
)

// Error codes the server returns for a refused backup request, in the "code" field of the body.
// Only the codes the operator acts on are named; the server has more.
const (
	// BackupCodeNotConfigured means the node runs without kahuna.backup_dir, which is what a node
	// not yet restarted with the backup settings looks like.
	BackupCodeNotConfigured = "CADB0700"
	// BackupCodeParentMissing means the catalog has no backup with the requested parent id.
	BackupCodeParentMissing = "CADB0705"
	// BackupCodeFailed is the server's code for every backup failure it does not classify. It
	// covers an I/O error, and also a cut that the transactions in flight overtook and a share of
	// the partitions that one node did not deliver, which CamusDB does not tell apart from the
	// first.
	BackupCodeFailed = "CADB0704"
	// BackupCodeRetryableLeadershipLoss means leadership moved during the backup. Nothing was
	// published.
	BackupCodeRetryableLeadershipLoss = "CADB070A"
	// BackupCodeTopologyChanged means the roster or the partition map moved during the backup.
	// Nothing was published.
	BackupCodeTopologyChanged = "CADB070D"
	// BackupCodeNotCoordinator means the node does not lead the backup meta partition, so it
	// cannot take a coordinated backup. Another node can.
	BackupCodeNotCoordinator = "CADB070E"
)

// BackupInfo is the server's description of one backup (BackupInfoModel). Only the fields the
// operator uses are decoded.
type BackupInfo struct {
	BackupID        string `json:"backupId"`
	Type            string `json:"type"`
	ParentBackupID  string `json:"parentBackupId"`
	ClusterID       string `json:"clusterId"`
	CoordinatorNode string `json:"coordinatorNode"`
	// ActualKind differs from the requested kind when the server took a full backup in place of an
	// incremental one; SubstitutionReason then says why.
	ActualKind         string `json:"actualKind"`
	SubstitutionReason string `json:"substitutionReason"`
}

// BackupError is a backup request the server refused with an error code.
type BackupError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *BackupError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("backup refused with HTTP %d (%s)", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("backup refused with HTTP %d (%s): %s", e.StatusCode, e.Code, e.Message)
}

// TryAnotherNode reports whether a different node can still take the backup now. Every other
// code is a refusal that a different node would repeat.
func (e *BackupError) TryAnotherNode() bool {
	switch e.Code {
	case BackupCodeNotCoordinator, BackupCodeRetryableLeadershipLoss, BackupCodeTopologyChanged,
		BackupCodeNotConfigured, BackupCodeFailed:
		return true
	}
	return false
}

// ClearsOnRetry reports whether the same request can succeed later with nothing changed: the
// refusal comes from a meta partition with no leader yet, a leader that just moved, or a topology
// that moved during the backup. NotConfigured is not one of these: it needs a restart with the
// backup settings. Failed is not either, because it can also be an I/O error.
func (e *BackupError) ClearsOnRetry() bool {
	switch e.Code {
	case BackupCodeNotCoordinator, BackupCodeRetryableLeadershipLoss, BackupCodeTopologyChanged:
		return true
	}
	return false
}

// Credentials authenticate the operator to a cluster with authentication on.
type Credentials struct {
	// Realm scopes the cached token. Every node of one cluster shares a token key, so a token from
	// one node is good on all of them; nodes of different clusters share nothing.
	Realm    string
	Username string
	Password string
}

// Target is one node as the operator reaches it.
type Target struct {
	// BaseURL is the node's REST root, e.g. http://10.0.0.5:5095 or https://10.0.0.5:7141.
	BaseURL string
	// ServerName is the name the node's certificate is verified against. The URL carries the pod
	// IP, which the certificate does not name, so the pod's DNS name is supplied here instead.
	ServerName string
	// RootCAs verifies the node's certificate. Nil uses the system pool.
	RootCAs *x509.CertPool
	// CAFingerprint identifies RootCAs, so connections are reused per trust root rather than per
	// call. Required whenever RootCAs is set.
	CAFingerprint string
	// Credentials, when set, are exchanged for a bearer token sent with every call.
	Credentials *Credentials
}

// Client reads cluster state from a CamusDB node, asks one to decommission itself, and asks one
// to take a backup.
type Client interface {
	// Membership returns the committed roster as seen by the node.
	Membership(ctx context.Context, t Target) (*Membership, error)
	// Health returns the node's readiness. A 503 is a valid answer, not an error: it means the
	// node is up but cannot serve.
	Health(ctx context.Context, t Target) (*Health, error)
	// Leave asks the node to remove itself from the committed roster. Every documented outcome
	// comes back as a result rather than an error, because a caller driving a scale-down has to
	// tell "the node is out" from "nothing happened" — and only a transport failure is genuinely
	// unclassifiable.
	Leave(ctx context.Context, t Target) (*LeaveResult, error)
	// Placement returns the committed replica set of every partition, as seen by the node.
	Placement(ctx context.Context, t Target) (*Placement, error)
	// Metrics returns the node's curated engine metrics.
	Metrics(ctx context.Context, t Target) (*Metrics, error)
	// NodeStats returns the node's disk, CPU and memory state.
	NodeStats(ctx context.Context, t Target) (*NodeStats, error)
	// TakeCoordinatedBackup asks the node to take a full backup at one cluster-wide cut. Only the
	// node that leads the backup meta partition accepts it. A refusal comes back as a *BackupError.
	TakeCoordinatedBackup(ctx context.Context, t Target) (*BackupInfo, error)
	// TakeIncrementalBackup asks the node to back up the log written since the parent backup. A
	// refusal comes back as a *BackupError.
	TakeIncrementalBackup(ctx context.Context, t Target, parentBackupID string) (*BackupInfo, error)
}

// Default timeouts. A status read and a leave need different bounds, and one value cannot serve
// both.
//
// DefaultLeaveTimeout covers the server's own bound on a graceful leave: it holds the request open
// while it evacuates the departing node's replicas, for up to kahuna.decommission_drain_timeout_ms
// (120s by default) plus a guard margin. The server links that work to the request, so a client
// that gives up first cancels the drain rather than merely stopping the wait. Under the default
// full replication no range names the node, so nothing is evacuated and the call returns at once.
const (
	DefaultReadTimeout  = 5 * time.Second
	DefaultLeaveTimeout = 150 * time.Second
)

// tokenRefreshMargin renews a token this long before the server says it expires, so a call never
// carries a token that lapses in flight.
const tokenRefreshMargin = 60 * time.Second

// maxTransports bounds the per-node transport cache. Pod names are stable, so the cache normally
// holds one entry per node; the bound only matters across many trust-root rotations.
const maxTransports = 256

// ErrUnauthorized means the node rejected the operator's credentials.
var ErrUnauthorized = errors.New("the node rejected the operator's credentials")

type cachedToken struct {
	token   string
	expires time.Time
}

// HTTPClient talks to a node's REST port.
//
// Without credentials it uses the plaintext port, as the probes do. With credentials it must use
// the TLS port: CamusDB refuses a credential over plaintext from anything but loopback.
type HTTPClient struct {
	// ReadTimeout bounds Membership, Health, Placement, Metrics and NodeStats. It stays short: a
	// status read must never wedge a reconcile, and an unreachable node is reported as an unknown
	// roster rather than an error.
	ReadTimeout time.Duration
	// LeaveTimeout bounds Leave, which is not a status read and must not carry a read bound.
	LeaveTimeout time.Duration

	mu         sync.Mutex
	transports map[string]*http.Client
	tokens     map[string]cachedToken
	now        func() time.Time
}

// NewHTTPClient returns a client with a separate bound for reads and for a leave.
//
// Each call attaches its own deadline to the request context instead of relying on a client-wide
// Timeout, which bounds the dial, the response and the body read alike, and lets one client hold
// two different bounds.
func NewHTTPClient(readTimeout, leaveTimeout time.Duration) *HTTPClient {
	return &HTTPClient{
		ReadTimeout:  readTimeout,
		LeaveTimeout: leaveTimeout,
		transports:   map[string]*http.Client{},
		tokens:       map[string]cachedToken{},
		now:          time.Now,
	}
}

// bound derives a deadline context from ctx. A non-positive duration means "no bound of our own",
// which leaves the caller's context as the only limit.
func bound(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// httpFor returns the client for a target's trust settings. The TLS server name is a property of
// the transport, so each node gets its own; that also keeps a connection pinned to the node it was
// verified for.
func (c *HTTPClient) httpFor(t Target) *http.Client {
	key := t.ServerName + "|" + t.CAFingerprint
	c.mu.Lock()
	defer c.mu.Unlock()
	if hc, ok := c.transports[key]; ok {
		return hc
	}
	if len(c.transports) >= maxTransports {
		for _, hc := range c.transports {
			hc.CloseIdleConnections()
		}
		c.transports = map[string]*http.Client{}
	}
	hc := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    t.RootCAs,
				ServerName: t.ServerName,
				MinVersion: tls.VersionTLS12,
			},
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
		// A redirect is never part of these APIs; following one would resend the bearer token to
		// wherever it points.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	c.transports[key] = hc
	return hc
}

// tokenKey identifies a cached token. The password digest is part of it so that a rotated
// password is used at once instead of after the old token expires.
func tokenKey(cr *Credentials) string {
	sum := sha256.Sum256([]byte(cr.Password))
	return cr.Realm + "|" + cr.Username + "|" + hex.EncodeToString(sum[:8])
}

// loginResponse is the response of POST /login.
type loginResponse struct {
	Status           string `json:"status"`
	Token            string `json:"token"`
	ExpiresInSeconds *int64 `json:"expiresInSeconds"`
	Code             string `json:"code"`
	Message          string `json:"message"`
}

// token returns a bearer token for the target's credentials, logging in when none is cached or the
// cached one is about to expire.
func (c *HTTPClient) token(ctx context.Context, t Target) (string, error) {
	key := tokenKey(t.Credentials)
	c.mu.Lock()
	cached, ok := c.tokens[key]
	c.mu.Unlock()
	if ok && c.now().Add(tokenRefreshMargin).Before(cached.expires) {
		return cached.token, nil
	}

	body, err := json.Marshal(map[string]string{"user": t.Credentials.Username, "password": t.Credentials.Password})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.BaseURL+"/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpFor(t).Do(req)
	if err != nil {
		return "", fmt.Errorf("logging in: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out loginResponse
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("logging in: %w", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("decoding login response (status %d): %w", resp.StatusCode, err)
	}
	if out.Status != "ok" || out.Token == "" {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return "", fmt.Errorf("%w: %s %s", ErrUnauthorized, out.Code, out.Message)
		}
		return "", fmt.Errorf("login returned HTTP %d: %s %s", resp.StatusCode, out.Code, out.Message)
	}

	// The server-measured duration is used rather than the absolute deadline, so a clock skew
	// between the operator and the node cannot make a fresh token look expired.
	lifetime := 15 * time.Minute
	if out.ExpiresInSeconds != nil {
		lifetime = time.Duration(*out.ExpiresInSeconds) * time.Second
	}
	c.mu.Lock()
	c.tokens[key] = cachedToken{token: out.Token, expires: c.now().Add(lifetime)}
	c.mu.Unlock()
	return out.Token, nil
}

// forgetToken drops a cached token the server rejected.
func (c *HTTPClient) forgetToken(cr *Credentials) {
	c.mu.Lock()
	delete(c.tokens, tokenKey(cr))
	c.mu.Unlock()
}

// call sends one request and returns the status code and the body. A nil body sends none, and a
// body is sent as JSON. With credentials a rejected token is renewed once: the server can drop
// tokens before their stated expiry, for example on a password change.
func (c *HTTPClient) call(ctx context.Context, t Target, method, path string, body []byte) (int, []byte, error) {
	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, t.BaseURL+path, reader)
		if err != nil {
			return 0, nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if t.Credentials != nil {
			tok, err := c.token(ctx, t)
			if err != nil {
				return 0, nil, err
			}
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := c.httpFor(t).Do(req)
		if err != nil {
			return 0, nil, err
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if err != nil {
			return resp.StatusCode, nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && t.Credentials != nil && attempt == 0 {
			c.forgetToken(t.Credentials)
			continue
		}
		return resp.StatusCode, raw, nil
	}
}

// getJSON reads a route that answers 200 with a JSON body.
func (c *HTTPClient) getJSON(ctx context.Context, t Target, path string, out any, accept ...int) error {
	ctx, cancel := bound(ctx, c.ReadTimeout)
	defer cancel()

	code, body, err := c.call(ctx, t, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	ok := code == http.StatusOK
	for _, a := range accept {
		ok = ok || code == a
	}
	if !ok {
		return statusError(path, code, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decoding %s (status %d): %w", path, code, err)
	}
	return nil
}

// statusError describes an unexpected status, with the server's own error code when it sent one.
func statusError(path string, code int, body []byte) error {
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &e)
	if code == http.StatusUnauthorized {
		return fmt.Errorf("%s: %w", path, ErrUnauthorized)
	}
	if e.Code != "" || e.Message != "" {
		return fmt.Errorf("%s returned HTTP %d: %s %s", path, code, e.Code, e.Message)
	}
	return fmt.Errorf("%s returned HTTP %d", path, code)
}

// Membership implements Client.
func (c *HTTPClient) Membership(ctx context.Context, t Target) (*Membership, error) {
	var m Membership
	if err := c.getJSON(ctx, t, "/v1/cluster/membership", &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Health implements Client.
//
// 503 is the documented "up but not serving" answer and carries a valid body, so it is decoded
// and returned rather than surfaced as an error.
func (c *HTTPClient) Health(ctx context.Context, t Target) (*Health, error) {
	var h Health
	if err := c.getJSON(ctx, t, "/v1/cluster/health", &h, http.StatusServiceUnavailable); err != nil {
		return nil, err
	}
	return &h, nil
}

// Placement implements Client.
func (c *HTTPClient) Placement(ctx context.Context, t Target) (*Placement, error) {
	var p Placement
	if err := c.getJSON(ctx, t, "/v1/cluster/placement", &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Metrics implements Client.
func (c *HTTPClient) Metrics(ctx context.Context, t Target) (*Metrics, error) {
	var m Metrics
	if err := c.getJSON(ctx, t, "/v1/dashboard/metrics", &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// NodeStats implements Client. A server that predates the route answers 404.
func (c *HTTPClient) NodeStats(ctx context.Context, t Target) (*NodeStats, error) {
	var s NodeStats
	if err := c.getJSON(ctx, t, "/v1/admin/stats", &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Leave implements Client.
//
// The server answers every documented outcome with a body — 200 committed or already-gone, 403 a
// refused caller, 409 the permanent last-voter refusal, 503 not-initialized, no-leader or another
// node mid-drain, 504 unresolved before the deadline — so the status code is not consulted here
// beyond decoding. The caller branches on Outcome, which carries the distinction that matters.
//
// Every outcome the operator does not name is retryable on the server's own classification, so an
// unrecognized one holds the scale and is tried again. That covers DrainTimedOut, which resumes
// the drain rather than restarting it: replicas already moved stay moved.
func (c *HTTPClient) Leave(ctx context.Context, t Target) (*LeaveResult, error) {
	ctx, cancel := bound(ctx, c.LeaveTimeout)
	defer cancel()

	code, body, err := c.call(ctx, t, http.MethodPost, "/v1/cluster/leave", nil)
	if err != nil {
		return nil, err
	}
	var out LeaveResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decoding leave response (status %d): %w", code, err)
	}
	if out.Outcome == "" {
		return nil, statusError("/v1/cluster/leave", code, body)
	}
	return &out, nil
}

// TakeCoordinatedBackup implements Client. It has no timeout of its own: a full backup copies the
// whole store, so only the caller knows how long to wait for it.
func (c *HTTPClient) TakeCoordinatedBackup(ctx context.Context, t Target) (*BackupInfo, error) {
	return c.postBackup(ctx, t, "/v1/backups/coordinated", nil)
}

// TakeIncrementalBackup implements Client. It is bounded by the caller's context, as above.
func (c *HTTPClient) TakeIncrementalBackup(ctx context.Context, t Target, parentBackupID string) (*BackupInfo, error) {
	body, err := json.Marshal(map[string]string{"parentBackupId": parentBackupID})
	if err != nil {
		return nil, err
	}
	return c.postBackup(ctx, t, "/v1/backups/incremental", body)
}

// backupResponse is the body of every answer from a backup route.
type backupResponse struct {
	Status  string      `json:"status"`
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Backup  *BackupInfo `json:"backup"`
}

// postBackup sends a backup request. A 200 carries the new backup; every refusal carries an error
// code, and comes back as a *BackupError.
func (c *HTTPClient) postBackup(ctx context.Context, t Target, path string, body []byte) (*BackupInfo, error) {
	code, raw, err := c.call(ctx, t, http.MethodPost, path, body)
	if err != nil {
		return nil, err
	}
	var out backupResponse
	// A body that does not decode still leaves the status, which is the answer.
	_ = json.Unmarshal(raw, &out)
	if code == http.StatusUnauthorized {
		return nil, fmt.Errorf("%s: %w", path, ErrUnauthorized)
	}
	if code != http.StatusOK || out.Status != "ok" {
		return nil, &BackupError{StatusCode: code, Code: out.Code, Message: out.Message}
	}
	if out.Backup == nil || out.Backup.BackupID == "" {
		return nil, fmt.Errorf("%s: the response carries no backup id", path)
	}
	return out.Backup, nil
}
