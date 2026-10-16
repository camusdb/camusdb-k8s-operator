//go:build e2e
// +build e2e

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

package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/camusdb/k8s-operator/test/utils"
)

const (
	operatorNamespace = "camusdb-operator-system"
	testNamespace     = "camusdb-e2e"
	clusterName       = "camus"
	testDatabase      = "e2e"

	// Scale steps are one consensus round plus the operator's requeue interval now that nodes are
	// asked to leave rather than evicted by the failure detector. The allowance stays generous so
	// a slow CI runner does not turn a timing difference into a failure.
	membershipTimeout = 5 * time.Minute
	// Without authentication a scale-down waits for the failure detector to evict each node,
	// about 90 seconds per node on top of the operator's own steps.
	evictionScaleDownTimeout = 10 * time.Minute
	bootstrapTimeout  = 4 * time.Minute
	pollInterval      = 5 * time.Second
)

// kubectl runs a kubectl command and returns its trimmed stdout.
func kubectl(args ...string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", args...))
	return strings.TrimSpace(out), err
}

// mustKubectl runs a kubectl command and fails the spec if it errors.
func mustKubectl(args ...string) string {
	out, err := kubectl(args...)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "kubectl %s: %s", strings.Join(args, " "), out)
	return out
}

// applyYAML pipes a manifest into kubectl apply.
func applyYAML(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "applying manifest: %s", out)
}

// clusterField reads a jsonpath expression off the CamusDBCluster.
func clusterField(path string) string {
	out, err := kubectl("-n", testNamespace, "get", "camusdbcluster", clusterName,
		"-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// podUIDs maps pod name to UID, which is how a restart is detected: a restarted pod keeps its
// name but gets a new UID.
func podUIDs() map[string]string {
	out, err := kubectl("-n", testNamespace, "get", "pods",
		"-l", "app.kubernetes.io/instance="+clusterName,
		"-o", `jsonpath={range .items[*]}{.metadata.name}={.metadata.uid} {end}`)
	uids := map[string]string{}
	if err != nil {
		return uids
	}
	for _, kv := range strings.Fields(out) {
		if name, uid, ok := strings.Cut(kv, "="); ok {
			uids[name] = uid
		}
	}
	return uids
}

// authEnabled selects whether the suite turns on CamusDB authentication. It is off by default:
// authentication needs node-to-node mutual TLS, which the default camusdb/camusdb:0.15.1 image does
// not have. Turn it on (E2E_AUTH=true) with CAMUSDB_IMAGE set to a server that has it.
var authEnabled = envOr("E2E_AUTH", "false") == "true"

// secretValue reads one key of a Secret in the test namespace.
func secretValue(name, key string) string {
	encoded := mustKubectl("-n", testNamespace, "get", "secret", name, "-o", "jsonpath={.data."+key+"}")
	raw, err := base64.StdEncoding.DecodeString(encoded)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(raw)
}

// sqlClient talks to the cluster through a port-forward to the client Service.
//
// A port-forward reaches the pod over its loopback interface, and CamusDB exempts loopback from the
// rule that a credential must travel over TLS, so the plaintext port accepts the token here.
type sqlClient struct {
	baseURL string
	token   string
}

// post sends a JSON request and decodes a JSON answer.
func (c *sqlClient) post(path string, body any, out any) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := http.DefaultClient.Do(req)
	ExpectWithOffset(2, err).NotTo(HaveOccurred())
	defer func() { _ = resp.Body.Close() }()
	ExpectWithOffset(2, json.NewDecoder(resp.Body).Decode(out)).To(Succeed())
}

// statusResult is the part of every answer that says whether the call worked.
type statusResult struct {
	Status  string `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// exec runs a statement that returns no rows.
func (c *sqlClient) exec(path, sql string) {
	var out statusResult
	c.post(path, map[string]any{"databaseName": testDatabase, "sql": sql}, &out)
	ExpectWithOffset(1, out.Status).To(Equal("ok"), "%s failed: %s %s", sql, out.Code, out.Message)
}

// queryValue runs a query that returns one string cell.
func (c *sqlClient) queryValue(sql string) string {
	var out struct {
		statusResult
		Rows [][]any `json:"rows"`
	}
	c.post("/execute-sql-query", map[string]any{"databaseName": testDatabase, "sql": sql}, &out)
	ExpectWithOffset(1, out.Status).To(Equal("ok"), "%s failed: %s %s", sql, out.Code, out.Message)
	ExpectWithOffset(1, out.Rows).To(HaveLen(1))
	ExpectWithOffset(1, out.Rows[0]).To(HaveLen(1))
	return fmt.Sprint(out.Rows[0][0])
}

// withSQL opens a port-forward to the client Service, logs in as the operator's admin account when
// authentication is on, and runs fn. Pod and Service IPs inside Kind are not routable from the test process, so this is
// how the suite talks to the cluster it just built.
func withSQL(fn func(c *sqlClient)) {
	const localPort = 35095
	cmd := exec.Command("kubectl", "-n", testNamespace, "port-forward",
		"svc/"+clusterName, fmt.Sprintf("%d:5095", localPort))
	ExpectWithOffset(1, cmd.Start()).To(Succeed())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	c := &sqlClient{baseURL: fmt.Sprintf("http://localhost:%d", localPort)}
	// The forward takes a moment to bind; poll rather than sleep a fixed amount.
	EventuallyWithOffset(1, func() error {
		resp, err := http.Get(c.baseURL + "/ping")
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}, 60*time.Second, time.Second).Should(Succeed(), "port-forward never became usable")

	if authEnabled {
		var login struct {
			statusResult
			Token string `json:"token"`
		}
		c.post("/login", map[string]string{
			"user":     secretValue(clusterName+"-admin", "username"),
			"password": secretValue(clusterName+"-admin", "password"),
		}, &login)
		ExpectWithOffset(1, login.Token).NotTo(BeEmpty(), "login failed: %s %s", login.Code, login.Message)
		c.token = login.Token
	}

	fn(c)
}

// clusterManifest renders a CamusDBCluster at the given size.
func clusterManifest(replicas int, extra string) string {
	if authEnabled {
		extra = "  auth: {}\n" + extra
	}
	return fmt.Sprintf(`
apiVersion: camusdb.io/v1alpha1
kind: CamusDBCluster
metadata:
  name: %s
  namespace: %s
spec:
  replicas: %d
  image: %s
  imagePullPolicy: IfNotPresent
  partitions: 3
  storage:
    data:
      size: 1Gi
  tls:
    certManager:
      issuerRef:
        name: camus-ca
        kind: Issuer
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
%s
`, clusterName, testNamespace, replicas, serverImage, extra)
}

// expectConverged waits for the operator to report a converged cluster of the given size.
func expectConverged(replicas int) {
	expectConvergedWithin(replicas, membershipTimeout)
}

// expectConvergedWithin is expectConverged with an explicit bound.
func expectConvergedWithin(replicas int, timeout time.Duration) {
	EventuallyWithOffset(2, func() string {
		return fmt.Sprintf("%s/%s/%s",
			clusterField("{.status.voters}"),
			clusterField("{.status.readyReplicas}"),
			clusterField("{.status.phase}"))
	}, timeout, pollInterval).Should(
		Equal(fmt.Sprintf("%d/%d/Running", replicas, replicas)),
		"cluster never converged at %d nodes", replicas)
}

var _ = Describe("CamusDBCluster", Ordered, func() {
	BeforeAll(func() {
		By("deploying the operator")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "failed to install CRDs")
		_, err = utils.Run(exec.Command("make", "deploy", "IMG="+managerImage))
		Expect(err).NotTo(HaveOccurred(), "failed to deploy the operator")

		// The image tag is fixed, so a rebuilt image leaves the Deployment spec byte-identical and
		// Kubernetes has no reason to restart anything — on a reused cluster that silently tests
		// the *previous* build. Forcing a rollout makes "the operator under test" mean the binary
		// this run just built.
		By("restarting the operator so the freshly built image is the one running")
		mustKubectl("-n", operatorNamespace, "rollout", "restart",
			"deployment/camusdb-operator-controller-manager")
		mustKubectl("-n", operatorNamespace, "rollout", "status",
			"deployment/camusdb-operator-controller-manager", "--timeout=5m")

		Eventually(func() string {
			return clusterFieldIn(operatorNamespace, "deployment", "camusdb-operator-controller-manager",
				"{.status.readyReplicas}")
		}, 3*time.Minute, pollInterval).Should(Equal("1"), "operator never became ready")

		// A CA issuer rather than a self-signed one for the cluster certificate: the nodes trust
		// the issuing CA, and a CA that stays fixed across renewals keeps old and new nodes
		// trusting each other while a renewal rolls through.
		By("creating the test namespace and a CA issuer")
		applyYAML(fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", testNamespace))
		applyYAML(fmt.Sprintf(`
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: selfsigned
  namespace: %[1]s
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: camus-ca
  namespace: %[1]s
spec:
  isCA: true
  commonName: camus-e2e-ca
  secretName: camus-ca
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: selfsigned
    kind: Issuer
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: camus-ca
  namespace: %[1]s
spec:
  ca:
    secretName: camus-ca
`, testNamespace))
	})

	AfterAll(func() {
		By("removing the test namespace")
		_, _ = kubectl("delete", "namespace", testNamespace, "--wait=false")
	})

	It("rejects configurations that cannot work", func() {
		By("refusing a two-node cluster")
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(clusterManifest(2, ""))
		out, err := utils.Run(cmd)
		Expect(err).To(HaveOccurred(), "a 2-replica cluster was accepted: %s", out)
		Expect(out).To(ContainSubstring("must not be 2"))
	})

	It("bootstraps a three node cluster without an extra rollout", func() {
		applyYAML(clusterManifest(3, ""))

		Eventually(func() string {
			return clusterField("{.status.phase}")
		}, bootstrapTimeout, pollInterval).ShouldNot(BeEmpty(), "operator never reconciled the cluster")

		expectConverged(3)

		By("checking the keystore came from cert-manager")
		Expect(mustKubectl("-n", testNamespace, "get", "certificate", clusterName+"-tls",
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")).To(Equal("True"))
		Expect(clusterField("{.status.conditions[?(@.type=='KeystoreReady')].status}")).To(Equal("True"))

		// A second revision here would mean the cluster rolled itself immediately after
		// bootstrapping — the failure mode that moving topology out of the pod template, and
		// holding the StatefulSet back until the keystore exists, both prevent.
		By("checking the StatefulSet has exactly one revision")
		revs := mustKubectl("-n", testNamespace, "get", "controllerrevision",
			"-o", "jsonpath={.items[*].metadata.name}")
		Expect(strings.Fields(revs)).To(HaveLen(1), "the cluster rolled itself after bootstrap")
	})

	It("serves SQL through the client service", func() {
		withSQL(func(c *sqlClient) {
			var out statusResult
			c.post("/create-db", map[string]any{"databaseName": testDatabase, "ifNotExists": true}, &out)
			Expect(out.Status).To(Equal("ok"), "create-db failed: %s %s", out.Code, out.Message)
			c.exec("/execute-sql-ddl", "CREATE TABLE IF NOT EXISTS kv (k STRING PRIMARY KEY NOT NULL, v STRING NOT NULL)")
			c.exec("/execute-sql-non-query", "INSERT INTO kv (k, v) VALUES ('a', 'before-scale')")
			Expect(c.queryValue("SELECT v FROM kv WHERE k = 'a'")).To(Equal("before-scale"))
		})
	})

	It("rejects edits to fields that are fixed at bootstrap", func() {
		out, err := kubectl("-n", testNamespace, "patch", "camusdbcluster", clusterName,
			"--type=merge", "-p", `{"spec":{"partitions":5}}`)
		Expect(err).To(HaveOccurred(), "partitions was mutable: %s", out)
		Expect(out).To(ContainSubstring("immutable"))
	})

	It("scales up one node at a time without disturbing the existing ones", func() {
		before := podUIDs()
		Expect(before).To(HaveLen(3))

		mustKubectl("-n", testNamespace, "patch", "camusdbcluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replicas":5}}`)

		expectConverged(5)

		By("checking the founding nodes were never restarted")
		after := podUIDs()
		for name, uid := range before {
			Expect(after).To(HaveKeyWithValue(name, uid),
				"%s was restarted by the scale-up", name)
		}

		By("checking the new nodes joined rather than formed a cluster")
		for _, pod := range []string{clusterName + "-3", clusterName + "-4"} {
			logs, err := kubectl("-n", testNamespace, "logs", pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(logs).To(ContainSubstring("--join-existing true"),
				"%s did not join the running cluster", pod)
		}
	})

	It("scales down one node at a time, removing each from the roster before the next", func() {
		mustKubectl("-n", testNamespace, "patch", "camusdbcluster", clusterName,
			"--type=merge", "-p", `{"spec":{"replicas":3}}`)

		logs := func() string {
			out, err := kubectl("-n", operatorNamespace, "logs",
				"deployment/camusdb-operator-controller-manager", "--tail=2000")
			Expect(err).NotTo(HaveOccurred())
			return out
		}

		if authEnabled {
			expectConverged(3)
			// A node that was asked to leave is out of the roster before its pod goes away, so the
			// operator never has to wait for the cluster to notice a member is missing.
			By("checking the operator asked nodes to leave rather than letting them be evicted")
			Expect(logs()).To(ContainSubstring("Node left the cluster"))
			Expect(logs()).NotTo(ContainSubstring("falling back to eviction"))
		} else {
			expectConvergedWithin(3, evictionScaleDownTimeout)
			// Without authentication the leave call is loopback-only, so the operator must not try
			// it; the failure detector evicts each removed node instead.
			By("checking the operator relied on eviction and never called leave")
			Expect(logs()).To(ContainSubstring("Authentication is off, so the node cannot be asked to leave"))
			Expect(logs()).NotTo(ContainSubstring("Node left the cluster"))
		}

		Eventually(func() string {
			return clusterField("{.status.replicas}")
		}, membershipTimeout, pollInterval).Should(Equal("3"))
	})

	It("still serves the data written before the scale", func() {
		withSQL(func(c *sqlClient) {
			Expect(c.queryValue("SELECT v FROM kv WHERE k = 'a'")).To(Equal("before-scale"))
		})
	})

	It("rolls the cluster when configuration changes, holding quorum throughout", func() {
		before := podUIDs()

		mustKubectl("-n", testNamespace, "patch", "camusdbcluster", clusterName,
			"--type=merge", "-p", `{"spec":{"config":{"slow_query_log_enabled":true}}}`)

		// The roll is observable as every pod getting a new UID.
		Eventually(func() bool {
			after := podUIDs()
			if len(after) != len(before) {
				return false
			}
			for name, uid := range before {
				if after[name] == uid {
					return false
				}
			}
			return true
		}, membershipTimeout, pollInterval).Should(BeTrue(), "the cluster never rolled")

		expectConverged(3)

		By("checking no node crash-looped through the roll")
		restarts := mustKubectl("-n", testNamespace, "get", "pods",
			"-l", "app.kubernetes.io/instance="+clusterName,
			"-o", "jsonpath={.items[*].status.containerStatuses[0].restartCount}")
		for _, r := range strings.Fields(restarts) {
			Expect(r).To(Equal("0"), "a node restarted during the rolling update")
		}

		withSQL(func(c *sqlClient) {
			Expect(c.queryValue("SELECT v FROM kv WHERE k = 'a'")).To(Equal("before-scale"))
		})
	})
})

// clusterFieldIn reads a jsonpath expression off an arbitrary resource.
func clusterFieldIn(namespace, kind, name, path string) string {
	out, err := kubectl("-n", namespace, "get", kind, name, "-o", "jsonpath="+path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
