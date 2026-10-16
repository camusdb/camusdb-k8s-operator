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

package camusdb

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Test bounds. The read bound is short enough that a delayed answer trips it, and the leave bound
// is far above the delay, so the two cases separate without depending on machine speed.
const (
	testReadTimeout  = 50 * time.Millisecond
	testLeaveTimeout = 10 * time.Second
	testServerDelay  = 400 * time.Millisecond
)

// delayedServer answers path after a delay, and answers anything else at once.
func delayedServer(t *testing.T, path, body string, delay time.Duration) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				// The client gave up. Returning drops the connection, which is what a real
				// server does when the request is aborted.
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A leave must not carry the read bound. The server holds the request open while it evacuates the
// departing node's replicas, and it links that work to the request, so a client that gives up
// early cancels the evacuation rather than merely stopping its own wait.
func TestLeaveIsNotBoundByTheReadTimeout(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/leave",
		`{"left":true,"outcome":"Committed","membershipVersion":7,"retryable":false}`, testServerDelay)

	c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

	result, err := c.Leave(context.Background(), Target{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("Leave failed although its own bound is %v: %v", testLeaveTimeout, err)
	}
	if !result.Removed() {
		t.Errorf("Removed() = false for outcome %q, want true", result.Outcome)
	}
	if result.MembershipVersion != 7 {
		t.Errorf("MembershipVersion = %d, want 7", result.MembershipVersion)
	}
}

// The read bound still applies to a status read, which must never wedge a reconcile.
func TestReadsAreBoundByTheReadTimeout(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/membership",
		`{"membershipVersion":1,"members":[],"localRole":"Voter","initialized":true}`, testServerDelay)

	c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

	start := time.Now()
	if _, err := c.Membership(context.Background(), Target{BaseURL: srv.URL}); err == nil {
		t.Fatal("Membership succeeded although the server answered after the read bound")
	}
	if elapsed := time.Since(start); elapsed >= testServerDelay {
		t.Errorf("Membership waited %v, which is at least the server delay %v; the read bound did not apply",
			elapsed, testServerDelay)
	}
}

// The autoscaler's reads carry the same bound. It polls them on every pass, so an unbounded one
// would let a single unresponsive node stall the cluster's reconcile indefinitely.
func TestAutoscalerReadsAreBoundByTheReadTimeout(t *testing.T) {
	calls := map[string]func(*HTTPClient, string) error{
		"/v1/cluster/placement": func(c *HTTPClient, url string) error {
			_, err := c.Placement(context.Background(), Target{BaseURL: url})
			return err
		},
		"/v1/dashboard/metrics": func(c *HTTPClient, url string) error {
			_, err := c.Metrics(context.Background(), Target{BaseURL: url})
			return err
		},
		"/v1/admin/stats": func(c *HTTPClient, url string) error {
			_, err := c.NodeStats(context.Background(), Target{BaseURL: url})
			return err
		},
	}
	for path, call := range calls {
		t.Run(path, func(t *testing.T) {
			srv := delayedServer(t, path, `{}`, testServerDelay)
			c := NewHTTPClient(testReadTimeout, testLeaveTimeout)

			start := time.Now()
			if err := call(c, srv.URL); err == nil {
				t.Fatalf("%s succeeded although the server answered after the read bound", path)
			}
			if elapsed := time.Since(start); elapsed >= testServerDelay {
				t.Errorf("%s waited %v, which is at least the server delay %v; the read bound did not apply",
					path, elapsed, testServerDelay)
			}
		})
	}
}

// A leave bound of zero means "no bound of our own", leaving the caller's context as the only
// limit. Without that, a zero value would read as an instant deadline and fail every call.
func TestZeroLeaveTimeoutDefersToTheCallerContext(t *testing.T) {
	srv := delayedServer(t, "/v1/cluster/leave", `{"left":false,"outcome":"NoLeader","retryable":true}`, 0)

	c := NewHTTPClient(testReadTimeout, 0)

	result, err := c.Leave(context.Background(), Target{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("Leave failed with no bound of its own: %v", err)
	}
	if result.Outcome != "NoLeader" {
		t.Errorf("Outcome = %q, want NoLeader", result.Outcome)
	}
}

// The outcomes the operator does not name must not read as success. A scale-down that treats an
// unrecognized outcome as "the node is out" would delete a pod that is still a committed member,
// which is the exact situation the leave API exists to avoid.
func TestUnnamedOutcomesAreNotRemoved(t *testing.T) {
	// Every outcome the server can report beside the ones the operator names, plus CamusDB's own
	// refusal of a caller it does not let administer the cluster.
	for _, outcome := range []string{
		"RefusedDrainInProgress", "DrainTimedOut", "NotInitialized", "NoLeader", "Timeout", OutcomeRefused,
	} {
		result := &LeaveResult{Outcome: outcome}
		if result.Removed() {
			t.Errorf("Removed() = true for outcome %q, want false", outcome)
		}
	}
	for _, outcome := range []string{OutcomeCommitted, OutcomeNotAMember} {
		result := &LeaveResult{Outcome: outcome}
		if !result.Removed() {
			t.Errorf("Removed() = false for outcome %q, want true", outcome)
		}
	}
	if (&LeaveResult{Outcome: OutcomeRefusedInsufficientVoters}).Removed() {
		t.Error("Removed() = true for a last-voter refusal, want false")
	}
}

// Health reports a 503 as a result rather than an error: it is the documented "up but not
// serving" answer, and it carries a valid body.
func TestHealthAcceptsServiceUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"ready":false,"initialized":false,"localRole":"NotMember"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	health, err := c.Health(context.Background(), Target{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("Health returned an error for a 503: %v", err)
	}
	if health.Ready {
		t.Error("Ready = true, want false")
	}
}

// authServer is a TLS node with authentication on. It issues token "tok-<n>" on the n-th login and
// accepts only the newest one, so a test can make it revoke a token by logging in elsewhere.
type authServer struct {
	srv    *httptest.Server
	logins atomic.Int32
	// expiresIn is the lifetime the login reports, in seconds.
	expiresIn int64
}

func newAuthServer(t *testing.T) *authServer {
	t.Helper()
	a := &authServer{expiresIn: 900}
	a.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["user"] != "admin" || body["password"] != "pw" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"status":"failed","code":"CADB0510","message":"bad credentials"}`))
				return
			}
			n := a.logins.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "ok", "token": tokenFor(n), "expiresInSeconds": a.expiresIn,
			})
		case "/v1/cluster/membership":
			if r.Header.Get("Authorization") != "Bearer "+tokenFor(a.logins.Load()) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"status":"failed","code":"CADB0511"}`))
				return
			}
			_, _ = w.Write([]byte(`{"membershipVersion":3,"members":[],"localRole":"Voter","initialized":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func tokenFor(n int32) string { return fmt.Sprintf("tok-%d", n) }

// target addresses the server the way the operator addresses a pod: by IP, with the certificate
// verified against a DNS name the test certificate carries.
func (a *authServer) target(password string) Target {
	pool := x509.NewCertPool()
	pool.AddCert(a.srv.Certificate())
	sum := sha256.Sum256(a.srv.Certificate().Raw)
	return Target{
		BaseURL:       a.srv.URL,
		ServerName:    "example.com",
		RootCAs:       pool,
		CAFingerprint: hex.EncodeToString(sum[:]),
		Credentials:   &Credentials{Realm: "ns/db", Username: "admin", Password: password},
	}
}

// The operator logs in once and reuses the token. CamusDB hashes the password on every login on
// purpose, so logging in per call would cost the nodes a password hash on every reconcile.
func TestCredentialsLogInOnceAndReuseTheToken(t *testing.T) {
	a := newAuthServer(t)
	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	for range 3 {
		m, err := c.Membership(context.Background(), a.target("pw"))
		if err != nil {
			t.Fatalf("Membership failed: %v", err)
		}
		if m.MembershipVersion != 3 {
			t.Fatalf("MembershipVersion = %d, want 3", m.MembershipVersion)
		}
	}
	if got := a.logins.Load(); got != 1 {
		t.Errorf("logged in %d times for 3 calls, want 1", got)
	}
}

// A token the server stopped accepting is replaced once, at the first 401, rather than carried
// until its stated expiry.
func TestRejectedTokenIsRenewedOnce(t *testing.T) {
	a := newAuthServer(t)
	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	if _, err := c.Membership(context.Background(), a.target("pw")); err != nil {
		t.Fatalf("Membership failed: %v", err)
	}
	// Another login revokes the operator's cached token.
	a.logins.Add(1)

	if _, err := c.Membership(context.Background(), a.target("pw")); err != nil {
		t.Fatalf("Membership failed after the token was revoked: %v", err)
	}
	if got := a.logins.Load(); got != 3 {
		t.Errorf("logins = %d, want 3 (initial, revoking, renewal)", got)
	}
}

// A token close to its expiry is renewed before it is used, so it never lapses in flight.
func TestTokenNearExpiryIsRenewed(t *testing.T) {
	a := newAuthServer(t)
	a.expiresIn = 30 // inside tokenRefreshMargin
	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	for range 2 {
		if _, err := c.Membership(context.Background(), a.target("pw")); err != nil {
			t.Fatalf("Membership failed: %v", err)
		}
	}
	if got := a.logins.Load(); got != 2 {
		t.Errorf("logins = %d, want 2: a token inside the refresh margin must not be reused", got)
	}
}

// Wrong credentials surface as ErrUnauthorized, which names the fix, rather than as a status code.
func TestWrongPasswordIsUnauthorized(t *testing.T) {
	a := newAuthServer(t)
	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	_, err := c.Membership(context.Background(), a.target("wrong"))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

// The node's certificate is verified. Sending the operator's password to a server that merely
// answers on the pod's IP would hand it to anything that took over that address.
func TestUntrustedCertificateIsRefused(t *testing.T) {
	a := newAuthServer(t)
	c := NewHTTPClient(testLeaveTimeout, testLeaveTimeout)

	tgt := a.target("pw")
	tgt.RootCAs = x509.NewCertPool()
	tgt.CAFingerprint = "empty"
	if _, err := c.Membership(context.Background(), tgt); err == nil {
		t.Fatal("Membership succeeded against a certificate outside the trust root")
	}

	tgt = a.target("pw")
	tgt.ServerName = "other.example.org"
	if _, err := c.Membership(context.Background(), tgt); err == nil {
		t.Fatal("Membership succeeded against a certificate that does not name the server")
	}
	if got := a.logins.Load(); got != 0 {
		t.Errorf("logged in %d times to an unverified server, want 0", got)
	}
}

// CamusDB spells metric kinds in lower case. Matching them exactly as another server spells them
// would read every counter as absent and every write rate as zero.
func TestMetricKindsMatchWhateverTheCase(t *testing.T) {
	one, two := 1.0, 2.0
	m := &Metrics{Rows: []MetricRow{
		{Metric: "kahuna.kv.write.entries", Tags: "class=ordinary", Kind: "counter", Total: &one},
		{Metric: "kahuna.kv.write.entries", Tags: "class=terminal", Kind: "Counter", Total: &two},
		{Metric: "raft.executor.client_queue_depth", Tags: "partition_id=0", Kind: "gauge", Last: &two},
	}}
	if total, ok := m.CounterTotal("kahuna.kv.write.entries"); !ok || total != 3 {
		t.Errorf("CounterTotal = %v, %v; want 3, true", total, ok)
	}
	if sum, ok := m.GaugeSum("raft.executor.client_queue_depth"); !ok || sum != 2 {
		t.Errorf("GaugeSum = %v, %v; want 2, true", sum, ok)
	}
}
