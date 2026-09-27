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
