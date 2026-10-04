# Workload and node scheduling algorithms

The `workload-placement` plugin lets a Pod choose scheduling algorithms from those advertised by a node. Annotate a node with `volcano.sh/scheduling-algorithms: "rr,binpack"` to support both algorithms, or list either one. Annotate a Pod template with the same key to set its preference order. For each candidate node, the first algorithm in the Pod's list that the node supports is used. `rr` spreads replicas of the same workload; `binpack` prefers nodes with higher utilization of the Pod's requested CPU, memory, extended resources, and DRA devices. The plugin currently implements these two algorithms only.

Replicas are grouped by their Volcano job (PodGroup): a vcjob's Pods, or the Pods of one ReplicaSet, form one workload, so no extra label is needed. (A Deployment rollout creates a new ReplicaSet and thus a new group, unless the controller's `podgroup-level-rules` group Pods by Deployment.) RR counts the workload's non-releasing replicas on each node. Pods without a scheduling annotation retain ordinary Volcano behavior. An invalid policy or no matching node leaves an annotated Pod Pending with a scheduling reason.

Volcano's normal `preemptionPolicy`, `volcano.sh/preemptable`, priority, and PodDisruptionBudget rules decide whether preemption is allowed and which Pods can be victims. No workload class has special preemption rights or protection. GPUs may be requested as an extended resource (for example `nvidia.com/gpu`) or through DRA. DRA preemption simulation supports independent whole-device `ExactCount` claims; other claim forms cannot be simulated safely.

Enable the normal `preempt` action (and Volcano's DRA predicate if you use DRA). Keep the victim filter plugins in the same tier so priority, preemptibility, and PDB decisions all apply:

```yaml
actions: "enqueue, allocate, backfill, preempt"
tiers:
- plugins:
  - name: priority
  - name: gang
    enablePreemptable: false
  - name: conformance
  - name: pdb
  - name: workload-placement
- plugins:
  - name: overcommit
  - name: drf
    enablePreemptable: false
  - name: predicates
    arguments:
      predicate.DynamicResourceAllocationEnable: true
  - name: proportion
  - name: nodeorder
```

For normal placement, the first preferred algorithm with a fitting node wins. RR chooses the node with the fewest replicas of the same job; Binpack uses Volcano's resource binpacking formula and the allocated fraction of matching DRA devices. During preemption, only priority and fairness matter: lower-priority victims go first, and among equal priorities the plugin always evicts from the job that would lose the smallest share of its replicas (counting replicas already releasing). Candidate nodes are ordered by the highest victim priority, then the worst share any single job would lose, then the number of victims. Normal fit checks, priority, preemptibility, and PDB checks still apply. For DRA, the plugin simulates releasing eligible victims' exclusive whole-device claims in a private snapshot and checks the requesting claim with the Kubernetes allocator. Victims without DRA claims are allowed; a missing, shared, or unsupported DRA claim makes that candidate ineligible.

After successful preemption, Volcano evicts the victims and nominates the requesting Pod to the chosen node. Victim termination and DRA claim release are asynchronous; the Pod remains Pending until ordinary predicates can bind it. Lower-priority replacements cannot take the nominated node. Use `enableTopologyAwarePreemption: false` for the normal preempt action (the default).

For DRA, this policy requires a functioning DRA driver and the [Volcano DRA setup](how_to_enable_dra.md). End-to-end validation on a real cluster is required before production use; fake scheduler tests cannot verify driver release timing or kubelet behavior.

## Manual verification (kind, fake GPUs)

No GPUs are needed: each worker advertises `nvidia.com/gpu: 8` as an extended resource. Workers 1–3 play A/B/C (rr) and workers 4–5 play D/E (binpack).

```bash
printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n%s' "$(printf -- '- role: worker\n%.0s' 1 2 3 4 5)" | kind create cluster --name rr --config -
i=0; for n in rr-worker rr-worker2 rr-worker3 rr-worker4 rr-worker5; do i=$((i+1)); z=rr; [ $i -ge 4 ] && z=binpack
  kubectl annotate node $n volcano.sh/scheduling-algorithms=$z
  kubectl patch node $n --subresource=status --type=json -p '[{"op":"add","path":"/status/capacity/nvidia.com~1gpu","value":"8"}]'; done
make vc-scheduler-image TAG=rr-test && kind load docker-image volcanosh/vc-scheduler:rr-test --name rr
helm install volcano installer/helm/chart/volcano -n volcano-system --create-namespace \
  --set basic.scheduler_image_tag_version=rr-test --set basic.image_pull_policy=IfNotPresent
# Put the tiers above (without the DRA argument) into volcano-scheduler.conf, then:
kubectl -n volcano-system create configmap volcano-scheduler-configmap --from-file=volcano-scheduler.conf --dry-run=client -o yaml | kubectl apply -f -
kubectl -n volcano-system rollout restart deploy/volcano-scheduler
for p in low:10 medium:50 high:100; do kubectl create priorityclass ${p%:*} --value=${p#*:}; done
```

Helpers (`inference J1`, `training t1 medium`, `pdb J1`, `show`):

```bash
inference() {
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: $(echo $1 | tr A-Z a-z)}
spec:
  replicas: 6
  selector: {matchLabels: {app.kubernetes.io/name: $1}}
  template:
    metadata:
      labels: {app.kubernetes.io/name: $1}
      annotations: {volcano.sh/scheduling-algorithms: "rr,binpack", volcano.sh/preemptable: "true"}
    spec:
      schedulerName: volcano
      priorityClassName: low
      terminationGracePeriodSeconds: 0
      containers: [{name: c, image: "registry.k8s.io/pause:3.10", resources: {requests: {cpu: 10m}, limits: {nvidia.com/gpu: 1}}}]
YAML
}
training() {
kubectl apply -f - <<YAML
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata: {name: $1}
spec:
  schedulerName: volcano
  minAvailable: 1
  priorityClassName: $2
  tasks:
  - {name: worker, replicas: 1, template: {metadata: {labels: {app.kubernetes.io/name: $1}, annotations: {volcano.sh/scheduling-algorithms: "binpack,rr", volcano.sh/preemptable: "false"}}, spec: {priorityClassName: $2, terminationGracePeriodSeconds: 0, containers: [{name: c, image: "registry.k8s.io/pause:3.10", resources: {requests: {cpu: 10m}, limits: {nvidia.com/gpu: 8}}}]}}}
YAML
}
pdb() { kubectl create pdb $(echo $1 | tr A-Z a-z) --selector=app.kubernetes.io/name=$1 --min-available=${2:-4}; }
show() { kubectl get pods -o custom-columns=SVC:.metadata.labels.app\\.kubernetes\\.io/name,JOB:.metadata.labels.volcano\\.sh/job-name,NODE:.spec.nodeName,PHASE:.status.phase --no-headers | sort | uniq -c; }
```

| Step | Command | Expected |
|---|---|---|
| 1 | `for j in J1 J2 J3 J4; do inference $j; pdb $j; done` | each service 2/2/2 on workers 1–3; workers 4–5 empty |
| 2 | `inference J5` | 6 × J5 on worker4; worker5 empty |
| 3 | `training t1 medium` | t1 on worker5, no evictions |
| 4 | `training t2 high` | worker1 costs each job 2/6 of its replicas, worker4 would cost J5 6/6: 2 × J1–J4 evicted from worker1; t2 on worker1; t1 stays; 2 replacements on worker4, 6 Pending |
| 5 | `training t3 high` | J2/J4 PDBs now allow 0 disruptions, so worker2/3 are skipped; t3 preempts worker4 (only candidate left) |
| 6 | `training t4 high` | no legal candidate: t4 Pending, no evictions |

### DRA variant

The same steps and expectations apply with real DRA devices from the [dra-example-driver](https://github.com/kubernetes-sigs/dra-example-driver) (8 fake `gpu.example.com` devices per node). Differences from the steps above:

```bash
# 1. The kind config needs CDI (add under the top level):
#    containerdConfigPatches:
#    - |-
#      [plugins."io.containerd.grpc.v1.cri"]
#        enable_cdi = true
# 2. Skip the node status patch; install the driver instead:
git clone --depth 1 --branch v0.5.0 https://github.com/kubernetes-sigs/dra-example-driver
helm upgrade -i --create-namespace -n dra-example-driver dra-example-driver dra-example-driver/deployments/helm/dra-example-driver --wait
# 3. In volcano-scheduler.conf, give predicates `arguments: {predicate.DynamicResourceAllocationEnable: true}`.
# 4. One claim per Pod, from templates:
for n in 1 8; do kubectl apply -f - <<YAML
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: gpu$n}
spec: {spec: {devices: {requests: [{name: gpu, exactly: {deviceClassName: gpu.example.com, allocationMode: ExactCount, count: $n}}]}}}
YAML
done
```

In the helpers, replace `limits: {nvidia.com/gpu: N}` with `claims: [{name: gpu}]` and add `resourceClaims: [{name: gpu, resourceClaimTemplateName: gpuN}]` to the Pod spec (`gpu1` for inference, `gpu8` for training). Check claim placement (one line per claim; a training claim holds all 8 devices; blank = unallocated) with `kubectl get resourceclaims -o jsonpath='{range .items[*]}{.status.allocation.devices.results[0].pool}{"\n"}{end}' | sort | uniq -c`.

Decisions are logged at `-v=4` (`workload-placement selected node`, `Considering Task`, `Try to preempt Task`). Clean up with `kind delete cluster --name rr`.
