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
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	camusdbv1alpha1 "github.com/camusdb/k8s-operator/api/v1alpha1"
)

// objectMeta builds the metadata shared by every owned object.
func objectMeta(cluster *camusdbv1alpha1.CamusDBCluster, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: cluster.Namespace,
		Labels:    podLabels(cluster),
	}
}

// servicePort builds a TCP Service port that targets the same container port.
func servicePort(name string, port int32) corev1.ServicePort {
	return corev1.ServicePort{Name: name, Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP}
}

// desiredPeerService builds the headless Service that gives every pod a stable DNS name.
//
// publishNotReadyAddresses is mandatory, not a tuning choice. Readiness requires cluster
// initialization, initialization requires a quorum, and a quorum requires peers to resolve each
// other — so if DNS only published ready pods, bootstrap could never start.
func desiredPeerService(cluster *camusdbv1alpha1.CamusDBCluster) *corev1.Service {
	p := ports(cluster)
	return &corev1.Service{
		ObjectMeta: objectMeta(cluster, peerServiceName(cluster)),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selectorLabels(cluster),
			Ports: []corev1.ServicePort{
				servicePort(portNameHTTP, p.HTTP),
				servicePort(portNameHTTPS, p.HTTPS),
				servicePort(portNameGRPC, p.GRPC),
				servicePort(portNameRaft, p.Raft),
			},
		},
	}
}

// desiredClientService builds the Service clients connect through. Unlike the peer Service it
// routes only to ready pods, so a node that is still initializing never receives client traffic.
func desiredClientService(cluster *camusdbv1alpha1.CamusDBCluster) *corev1.Service {
	p := ports(cluster)
	return &corev1.Service{
		ObjectMeta: objectMeta(cluster, clientServiceName(cluster)),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(cluster),
			Ports: []corev1.ServicePort{
				servicePort(portNameHTTP, p.HTTP),
				servicePort(portNameHTTPS, p.HTTPS),
				servicePort(portNameGRPC, p.GRPC),
			},
		},
	}
}

// rendered is the generated configuration of one pass: the startup script and the server's
// config.yml. Both feed the pod template hash.
type rendered struct {
	script string
	config string
}

// render generates the startup script and server config. It fails only on a spec.config that is
// not a JSON object, which the CRD schema already prevents.
func render(cluster *camusdbv1alpha1.CamusDBCluster) (rendered, error) {
	cfg, err := serverConfig(cluster)
	if err != nil {
		return rendered{}, err
	}
	return rendered{script: entrypointScript(cluster), config: cfg}, nil
}

// desiredConfigMap holds the generated startup script, the server config and the cluster topology
// the script reads.
func desiredConfigMap(cluster *camusdbv1alpha1.CamusDBCluster, bootstrapSize int32, r rendered) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(cluster, configMapName(cluster)),
		Data: map[string]string{
			entrypointFileName:   r.script,
			serverConfigFileName: r.config,
			topologyFileName:     topologyEnv(cluster, bootstrapSize),
		},
	}
}

// desiredPDB keeps voluntary disruptions (node drains, cluster upgrades) from taking more than
// one CamusDB node at a time, which is what preserves quorum during maintenance.
func desiredPDB(cluster *camusdbv1alpha1.CamusDBCluster) *policyv1.PodDisruptionBudget {
	maxUnavailable := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: objectMeta(cluster, pdbName(cluster)),
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:       selector(cluster),
			MaxUnavailable: &maxUnavailable,
		},
	}
}

// wantsPDB reports whether a PodDisruptionBudget should exist. A single-node cluster has no
// quorum to protect and a PDB would only block node drains forever.
func wantsPDB(cluster *camusdbv1alpha1.CamusDBCluster) bool {
	if cluster.Spec.PodDisruptionBudget != nil {
		return *cluster.Spec.PodDisruptionBudget
	}
	return desiredReplicas(cluster) >= 3
}

// volumeClaim builds one PVC template.
func volumeClaim(name string, v camusdbv1alpha1.VolumeSpec) corev1.PersistentVolumeClaim {
	accessModes := v.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: v.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: v.Size},
			},
		},
	}
}

// defaultAffinity spreads nodes across hosts on a best-effort basis. It is a preference rather
// than a requirement so a cluster still schedules on a single-node test environment.
func defaultAffinity(cluster *camusdbv1alpha1.CamusDBCluster) *corev1.Affinity {
	if cluster.Spec.Affinity != nil {
		return cluster.Spec.Affinity
	}
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: selector(cluster),
					TopologyKey:   corev1.LabelHostname,
				},
			}},
		},
	}
}

// containerEnv wires the pod's own identity in through the downward API — the startup script
// derives the Raft node identity from POD_NAME — plus logging, authentication and the user's own
// variables, in that order so the user's can override the generated ones.
func containerEnv(cluster *camusdbv1alpha1.CamusDBCluster) []corev1.EnvVar {
	logLevel := cluster.Spec.LogLevel
	if logLevel == "" {
		logLevel = "Information"
	}
	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
		{Name: "CAMUS_LOG_LEVEL", Value: logLevel},
		// Matches the tuning the upstream images apply.
		{Name: "DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", Value: "1"},
	}
	env = append(env, authEnv(cluster)...)
	return append(env, cluster.Spec.Env...)
}

// podVolumes builds the non-PVC volumes: the generated configuration, the keystore and the scratch
// directory the CA bundle is assembled in.
func podVolumes(cluster *camusdbv1alpha1.CamusDBCluster) []corev1.Volume {
	return []corev1.Volume{{
		Name: configVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(cluster)},
				DefaultMode:          ptr.To(int32(0o555)),
			},
		},
	}, {
		Name: tlsVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: tlsSecretName(cluster),
				// Readable by any user: the pod may run as a non-root UID the Secret's default
				// owner does not match, and the keystore has no password to fall back on anyway.
				DefaultMode: ptr.To(int32(0o444)),
			},
		},
	}, {
		Name:         trustVolumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
}

// podVolumeMounts mirrors podVolumes plus the persistent claims.
func podVolumeMounts(cluster *camusdbv1alpha1.CamusDBCluster) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: dataVolumeName, MountPath: storageMountPath},
		{Name: configVolumeName, MountPath: configMountPath},
		{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true},
		{Name: trustVolumeName, MountPath: trustMountPath},
	}
	if cluster.Spec.Storage.WAL != nil {
		// Nested inside the data volume's mount: kubelet orders mounts by path depth, so the
		// WAL volume lands on top of the directory the data volume provides.
		mounts = append(mounts, corev1.VolumeMount{Name: walVolumeName, MountPath: walPath})
	}
	return mounts
}

// desiredStatefulSet builds the StatefulSet backing the cluster.
//
// partition is the rolling-update partition the operator wants held right now; pods below it are
// left alone. The operator owns this value so that template changes never trigger an unsupervised
// restart of a quorum member. keystoreHash fingerprints the mounted keystore, so a renewed
// certificate rolls the nodes onto it.
func desiredStatefulSet(cluster *camusdbv1alpha1.CamusDBCluster, partition int32, r rendered, keystoreHash string) *appsv1.StatefulSet {
	p := ports(cluster)

	// Only the script and the server config are hashed. Topology is excluded on purpose so that a
	// scale does not roll the cluster; see topologyEnv.
	annotations := map[string]string{
		configHashAnnotation:   configHash(r.script, r.config),
		keystoreHashAnnotation: keystoreHash,
	}
	maps.Copy(annotations, cluster.Spec.PodAnnotations)

	claims := []corev1.PersistentVolumeClaim{volumeClaim(dataVolumeName, cluster.Spec.Storage.Data)}
	if cluster.Spec.Storage.WAL != nil {
		claims = append(claims, volumeClaim(walVolumeName, *cluster.Spec.Storage.WAL))
	}

	whenScaled := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if cluster.Spec.Storage.PVCRetentionPolicy == camusdbv1alpha1.PVCRetentionDelete {
		whenScaled = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: objectMeta(cluster, stsName(cluster)),
		Spec: appsv1.StatefulSetSpec{
			ServiceName: peerServiceName(cluster),
			Replicas:    ptr.To(desiredReplicas(cluster)),
			Selector:    selector(cluster),
			// Parallel is required, not an optimization. Under OrderedReady the StatefulSet
			// waits for pod-0 to become ready before creating pod-1 — but pod-0 cannot become
			// ready until a quorum forms, which needs its peers to exist. Bootstrap would
			// deadlock permanently.
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: ptr.To(partition),
				},
			},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: whenScaled,
				// Deleting the CamusDBCluster never reclaims volumes: losing a Raft cluster's
				// data to a mistyped delete is not recoverable, so it stays an explicit act.
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: claims,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels(cluster),
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:            cluster.Spec.ServiceAccountName,
					ImagePullSecrets:              cluster.Spec.ImagePullSecrets,
					SecurityContext:               cluster.Spec.PodSecurityContext,
					NodeSelector:                  cluster.Spec.NodeSelector,
					Tolerations:                   cluster.Spec.Tolerations,
					Affinity:                      defaultAffinity(cluster),
					TopologySpreadConstraints:     cluster.Spec.TopologySpreadConstraints,
					TerminationGracePeriodSeconds: cluster.Spec.TerminationGracePeriodSeconds,
					Volumes:                       podVolumes(cluster),
					Containers: []corev1.Container{{
						Name:            containerName,
						Image:           cluster.Spec.Image,
						ImagePullPolicy: cluster.Spec.ImagePullPolicy,
						// The image's own entrypoint hardcodes a single-node identity, so it is
						// replaced with the generated script.
						Command:         []string{"/bin/bash", entrypointPath},
						Env:             containerEnv(cluster),
						Resources:       cluster.Spec.Resources,
						SecurityContext: cluster.Spec.SecurityContext,
						VolumeMounts:    podVolumeMounts(cluster),
						Ports: []corev1.ContainerPort{
							{Name: portNameHTTP, ContainerPort: p.HTTP, Protocol: corev1.ProtocolTCP},
							{Name: portNameHTTPS, ContainerPort: p.HTTPS, Protocol: corev1.ProtocolTCP},
							{Name: portNameGRPC, ContainerPort: p.GRPC, Protocol: corev1.ProtocolTCP},
							{Name: portNameRaft, ContainerPort: p.Raft, Protocol: corev1.ProtocolTCP},
						},
						// Liveness deliberately does NOT use /v1/cluster/health. That endpoint
						// reports 503 for a node that is up but not yet a serving member — a
						// state a healthy node passes through on every restart, and one a node
						// evicted while down can sit in. Restarting the process would not fix
						// either, so liveness only asks whether the server is answering at all.
						// Not "/" either: that is the dashboard, which refuses a non-loopback
						// caller when authentication is off and redirects to a sign-in page
						// when it is on. /ping answers anyone, always.
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
								Path: "/ping", Port: intstr.FromInt32(p.HTTP),
							}},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
							TimeoutSeconds:      5,
							FailureThreshold:    6,
						},
						// Readiness is the membership-aware check: 200 only once the node is
						// initialized and holds a serving role, so client traffic never lands
						// on a node that would refuse it. It needs no credentials, with
						// authentication on or off.
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
								Path: "/v1/cluster/health", Port: intstr.FromInt32(p.HTTP),
							}},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
							TimeoutSeconds:      5,
							FailureThreshold:    3,
						},
					}},
				},
			},
		},
	}

	return sts
}
