# RR spread, Binpack overflow, and training preemption

The `rr-binpack` plugin applies to Pods labeled `volcano.sh/workload-class=inference` or `volcano.sh/workload-class=training`. GPUs may be requested either as a normal extended resource (for example `nvidia.com/gpu`) or through DRA. A DRA Pod must use an independent `ResourceClaim` (or a `ResourceClaimTemplate` that creates one claim per Pod) with `allocationMode: ExactCount`. No GPU resource name, class, or count is configured in the policy. Training Pods need a higher priority than inference: set it on the PodGroup (Volcano compares job priority across jobs) or through the Pods' `priorityClassName`. Mark inference Pods `volcano.sh/preemptable: "true"`, and configure PodDisruptionBudgets where needed.

Label any number of nodes `volcano.sh/nodegroup-name=rr` or `volcano.sh/nodegroup-name=binpack`. Inference Pods also need a stable `app.kubernetes.io/name` service label. The node label key, both zone values, workload class label and values, and service identity label are configurable; the values above are defaults. Neither node names nor device counts are configured. Other Pods retain the ordinary scheduler behavior.

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
  - name: rr-binpack
    arguments:
      serviceIdentityLabel: app.kubernetes.io/name
      nodeZoneLabel: volcano.sh/nodegroup-name
      rrZoneValue: rr
      binpackZoneValue: binpack
      workloadClassLabel: volcano.sh/workload-class
      inferenceValue: inference
      trainingValue: training
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

Inference uses the RR node with the fewest replicas of its service among nodes that pass normal predicates. If no RR node fits, it uses the most-allocated Binpack node. For extended resources this uses Volcano's binpack formula (`ResourceBinPackingScore`) over the requested extended resources. For DRA it uses the allocated fraction of matching `DeviceClass` devices, read from current `ResourceSlice` and claim state. Existing node score and name break ties. Normal resource and DRA fit checks still decide whether a candidate can actually fit.

Training first uses a fitting Binpack node. If none fits, normal preemption considers RR nodes before Binpack nodes, even when a Binpack node has fewer victims. With extended resources, Volcano's native resource accounting decides fit after the victims are evicted. With DRA, the plugin simulates the release of eligible inference Pods' exclusive, whole-device claims in a private DRA snapshot and runs the Kubernetes structured allocator for the training claim; a missing, shared, or unsupported victim claim makes that candidate ineligible. The normal Volcano victim filters, predicates, priority, and PDB checks still apply. Training Pods already running on a node are protected.

After successful preemption, Volcano evicts the victims and nominates the training Pod to the chosen node. Victim termination (and, with DRA, claim release) is asynchronous: training remains Pending until the resources are freed and the ordinary predicates can bind it. During this interval, lower-priority inference replacements cannot take the nominated node. Controllers recreate evicted Pods and they schedule normally elsewhere. Use `enableTopologyAwarePreemption: false` for the normal preempt action (the default).

For DRA, this policy requires a functioning DRA driver and the [Volcano DRA setup](how_to_enable_dra.md). End-to-end validation on a real cluster is required before production use; fake scheduler tests cannot verify driver release timing or kubelet behavior.

## Manual verification (kind, fake GPUs)

No GPUs are needed: each worker advertises `nvidia.com/gpu: 8` as an extended resource. Workers 1–3 play A/B/C (rr) and workers 4–5 play D/E (binpack).

```bash
printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n%s' "$(printf -- '- role: worker\n%.0s' 1 2 3 4 5)" | kind create cluster --name rr --config -
i=0; for n in rr-worker rr-worker2 rr-worker3 rr-worker4 rr-worker5; do i=$((i+1)); z=rr; [ $i -ge 4 ] && z=binpack
  kubectl label node $n volcano.sh/nodegroup-name=$z
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
      labels: {app.kubernetes.io/name: $1, volcano.sh/workload-class: inference}
      annotations: {volcano.sh/preemptable: "true"}
    spec:
      schedulerName: volcano
      priorityClassName: low
      terminationGracePeriodSeconds: 0
      containers: [{name: c, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: 10m}, limits: {nvidia.com/gpu: 1}}}]
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
  - {name: worker, replicas: 1, template: {metadata: {labels: {volcano.sh/workload-class: training}}, spec: {priorityClassName: $2, terminationGracePeriodSeconds: 0, containers: [{name: c, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: 10m}, limits: {nvidia.com/gpu: 8}}}]}}}
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
| 4 | `training t2 high` | 2 × J1–J4 evicted from worker1; t2 on worker1; t1 stays; 2 replacements on worker4, 6 Pending |
| 5 | `training t3 high` | J2/J4 PDBs now allow 0 disruptions, so worker2/3 are skipped; t3 preempts worker4 (last resort) |
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

Decisions are logged at `-v=4` (`rr-binpack selected node`, `Considering Task`, `Try to preempt Task`). Clean up with `kind delete cluster --name rr`.
