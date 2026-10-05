package preempt

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/workloadplacement"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// workloadPlacementGPUFixture builds the A/B/C (rr) + D/E (binpack) reference cluster
// with 8 extended-resource GPUs per node. Groups in running are placed as in
// the reference scenario; groups in pending are created unscheduled.
func workloadPlacementGPUFixture(running, pending []string) *uthelper.TestCommonStruct {
	gpus := func(n string) api.ScalarResource { return api.ScalarResource{Name: api.GPUResourceName, Value: n} }
	fixture := workloadPlacementDRAFixture()
	fixture.Pods, fixture.PodGroups, fixture.ResourceClaims, fixture.ResourceSlices, fixture.Nodes = nil, nil, nil, nil, nil
	for _, name := range []string{"A", "B", "C", "D", "E"} {
		node := util.BuildNode(name, api.BuildResourceList("32", "64Gi",
			api.ScalarResource{Name: "pods", Value: "110"}, gpus("8")),
			nil)
		node.Annotations = map[string]string{workloadplacement.SchedulingAlgorithmsAnnotation: zoneForDRANode(name)}
		fixture.Nodes = append(fixture.Nodes, node)
	}
	add := func(group string, isRunning bool) {
		prio, replicas, gpu, class := "low", 6, "1", "inference"
		if group[0] == 'T' {
			prio, replicas, gpu, class = map[string]string{"T1": "medium", "T2": "high"}[group], 1, "8", "training"
		}
		fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(group, "test", "q", 1,
			nil, schedulingv1beta1.PodGroupInqueue, prio))
		for r := 1; r <= replicas; r++ {
			name, node := fmt.Sprintf("%s-%d", group, r), map[string]string{"J5": "D", "T1": "E"}[group]
			if replicas == 1 {
				name = group
			}
			if node == "" && group[0] == 'J' {
				node = []string{"A", "B", "C"}[(r-1)%3]
			}
			labels := map[string]string{"example.com/workload-kind": class, "placement": zoneForDRANode(node), "node-name": node,
				"app.kubernetes.io/name": group}
			if class == "inference" {
				labels[schedulingv1beta1.PodPreemptable] = "true"
			} else {
				labels[schedulingv1beta1.PodPreemptable] = "false"
			}
			phase := v1.PodRunning
			if !isRunning {
				node, phase = "", v1.PodPending
			}
			pod := util.BuildPod("test", name, node, phase,
				api.BuildResourceList("100m", "128Mi", gpus(gpu)), group, labels, nil)
			pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "rr,binpack"
			if class == "training" {
				pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "binpack,rr"
			}
			fixture.Pods = append(fixture.Pods, pod)
		}
	}
	for _, g := range running {
		add(g, true)
	}
	// A non-policy system pod on A must not affect the victim-count tie-break.
	fixture.Pods = append(fixture.Pods, util.BuildPod("kube-system", "daemon", "A", v1.PodRunning,
		api.BuildResourceList("10m", "16Mi"), "", nil, nil))
	for _, g := range pending {
		add(g, false)
	}
	fixture.PriClass = []*schedulingv1.PriorityClass{util.BuildPriorityClass("low", 10),
		util.BuildPriorityClass("medium", 50), util.BuildPriorityClass("high", 100)}
	return fixture
}

// placement counts bound/pipelined tasks of a job per node.
func placement(ssn *framework.Session, group string) map[string]int {
	counts := map[string]int{}
	for _, task := range ssn.Jobs[api.JobID("test/"+group)].Tasks {
		if task.NodeName != "" {
			counts[task.NodeName]++
		}
	}
	return counts
}

func TestWorkloadPlacementGPUAllocatePhases(t *testing.T) {
	all := []string{"J1", "J2", "J3", "J4"}
	for _, tc := range []struct {
		name             string
		running, pending []string
		binds            int
		want             map[string]map[string]int
	}{
		{"phase 1: J1-J4 spread 2/2/2 on RR", nil, all, 24, map[string]map[string]int{
			"J1": {"A": 2, "B": 2, "C": 2}, "J2": {"A": 2, "B": 2, "C": 2},
			"J3": {"A": 2, "B": 2, "C": 2}, "J4": {"A": 2, "B": 2, "C": 2}}},
		{"phase 2: J5 packs onto D", all, []string{"J5"}, 6, map[string]map[string]int{"J5": {"D": 6}}},
		{"phase 3: T1 takes free E", append(all, "J5"), []string{"T1"}, 1, map[string]map[string]int{"T1": {"E": 1}}},
		{"RR not full: inference stays on RR", []string{"J1"}, []string{"J2"}, 6, map[string]map[string]int{
			"J2": {"A": 2, "B": 2, "C": 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := workloadPlacementGPUFixture(tc.running, tc.pending)
			ssn := fixture.RegisterSession(workloadPlacementDRATiers(), nil)
			defer fixture.Close()
			fixture.ExpectBindsNum, fixture.MinimalBindCheck = tc.binds, true
			fixture.Run([]framework.Action{allocate.New()})
			if err := fixture.CheckBind(0); err != nil {
				t.Fatal(err)
			}
			for group, want := range tc.want {
				if got := placement(ssn, group); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("%s placement = %v, want %v", group, got, want)
				}
			}
		})
	}
}

func TestWorkloadPlacementGPUPreemption(t *testing.T) {
	running := []string{"J1", "J2", "J3", "J4", "J5", "T1"}
	for _, tc := range []struct {
		name               string
		pdbLabel, pdbValue string
		wantNode           string
		wantEvictions      int
	}{
		{name: "A spreads losses (2/6 per job) before D (6/6 of J5); E protected", wantNode: "A", wantEvictions: 8},
		{name: "PDB on A, use B", pdbLabel: "node-name", pdbValue: "A", wantNode: "B", wantEvictions: 8},
		{name: "PDB on A-C, only D remains", pdbLabel: "placement", pdbValue: "rr", wantNode: "D", wantEvictions: 6},
		{name: "PDB on everything, T2 stays pending", pdbLabel: "example.com/workload-kind", pdbValue: "inference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := workloadPlacementGPUFixture(running, []string{"T2"})
			ssn := fixture.RegisterSession(workloadPlacementDRATiers(), []conf.Configuration{{Name: "preempt",
				Arguments: map[string]interface{}{EnableTopologyAwarePreemptionKey: false}}})
			defer fixture.Close()
			if tc.pdbLabel != "" {
				store := ssn.InformerFactory().Policy().V1().PodDisruptionBudgets().Informer().GetStore()
				if err := store.Add(blockingDRAPDB("pdb", tc.pdbLabel, tc.pdbValue)); err != nil {
					t.Fatal(err)
				}
			}
			fixture.ExpectEvictNum = tc.wantEvictions
			for _, pod := range fixture.Pods {
				if tc.wantNode != "" && pod.Spec.NodeName == tc.wantNode && pod.Labels["example.com/workload-kind"] == "inference" {
					fixture.ExpectEvicted = append(fixture.ExpectEvicted, "test/"+pod.Name)
				}
			}
			if tc.wantNode != "" {
				fixture.ExpectPipeLined = map[string][]string{"test/T2": {tc.wantNode}}
			}
			fixture.Run([]framework.Action{New()})
			if err := fixture.CheckEvict(0); err != nil {
				t.Fatal(err)
			}
			if tc.wantNode != "" {
				if err := fixture.CheckPipelined(0); err != nil {
					t.Fatal(err)
				}
			} else if task := ssn.Jobs["test/T2"].Tasks["test-T2"]; task.Status != api.Pending {
				t.Fatalf("T2 status = %s, want Pending", task.Status)
			}
			if got := placement(ssn, "T1"); got["E"] != 1 {
				t.Fatalf("T1 moved: %v", got)
			}
		})
	}
}

// After A's victims are gone, T2 binds to A and the eight recreated replicas
// fill D's two free GPUs; the other six stay Pending.
func TestWorkloadPlacementGPUReplacements(t *testing.T) {
	fixture := workloadPlacementGPUFixture([]string{"J1", "J2", "J3", "J4", "J5", "T1"}, []string{"T2"})
	for _, pod := range fixture.Pods {
		if pod.Spec.NodeName == "A" {
			pod.Spec.NodeName, pod.Status.Phase = "", v1.PodPending
		}
		if pod.Name == "T2" {
			pod.Status.NominatedNodeName = "A"
		}
	}
	ssn := fixture.RegisterSession(workloadPlacementDRATiers(), nil)
	defer fixture.Close()
	fixture.ExpectBindsNum, fixture.MinimalBindCheck = 3, true
	fixture.Run([]framework.Action{allocate.New()})
	if err := fixture.CheckBind(0); err != nil {
		t.Fatal(err)
	}
	if got := placement(ssn, "T2"); got["A"] != 1 {
		t.Fatalf("T2 placement = %v, want A", got)
	}
	onD, pending := 0, 0
	for _, group := range []string{"J1", "J2", "J3", "J4"} {
		for _, task := range ssn.Jobs[api.JobID("test/"+group)].Tasks {
			if task.Pod.Labels["node-name"] != "A" {
				continue
			}
			switch {
			case task.Status == api.Pending:
				pending++
			case task.NodeName == "D":
				onD++
			default:
				t.Errorf("replacement %s on %s (%s)", task.Name, task.NodeName, task.Status)
			}
		}
	}
	if onD != 2 || pending != 6 {
		t.Fatalf("replacements: %d on D, %d pending; want 2 and 6", onD, pending)
	}
}

// A high-priority pod needing 4 of 8 GPUs must spread evictions by the share of
// each job's replicas lost: J1 (8 replicas) loses 2, J2 (3) and J3 (5) lose 1 each,
// rather than four replicas of a single job.
func TestWorkloadPlacementFairVictims(t *testing.T) {
	gpus := func(n string) v1.ResourceList {
		return api.BuildResourceList("100m", "128Mi", api.ScalarResource{Name: api.GPUResourceName, Value: n})
	}
	node := util.BuildNode("X", api.BuildResourceList("32", "64Gi", api.ScalarResource{Name: "pods", Value: "110"},
		api.ScalarResource{Name: api.GPUResourceName, Value: "8"}), nil)
	node.Annotations = map[string]string{workloadplacement.SchedulingAlgorithmsAnnotation: "rr"}
	fixture := workloadPlacementDRAFixture()
	fixture.Pods, fixture.PodGroups, fixture.ResourceClaims, fixture.ResourceSlices = nil, nil, nil, nil
	fixture.Nodes = []*v1.Node{node}
	for _, job := range []struct {
		name            string
		running, totals int
	}{{"J1", 4, 8}, {"J2", 2, 3}, {"J3", 2, 5}, {"T", 0, 1}} {
		prio, gpu := "low", "1"
		if job.name == "T" {
			prio, gpu = "high", "4"
		}
		fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(job.name, "test", "q", 1,
			nil, schedulingv1beta1.PodGroupInqueue, prio))
		for r := 1; r <= job.totals; r++ {
			host, phase := "X", v1.PodRunning
			if r > job.running {
				host, phase = "", v1.PodPending
			}
			pod := util.BuildPod("test", fmt.Sprintf("%s-%d", job.name, r), host, phase, gpus(gpu), job.name,
				map[string]string{schedulingv1beta1.PodPreemptable: fmt.Sprint(job.name != "T")}, nil)
			pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "rr"
			fixture.Pods = append(fixture.Pods, pod)
		}
	}
	ssn := fixture.RegisterSession(workloadPlacementDRATiers(), []conf.Configuration{{Name: "preempt",
		Arguments: map[string]interface{}{EnableTopologyAwarePreemptionKey: false}}})
	defer fixture.Close()
	fixture.Run([]framework.Action{New()})
	got := map[string]int{}
	for _, group := range []string{"J1", "J2", "J3"} {
		got[group] = len(ssn.Jobs[api.JobID("test/"+group)].TaskStatusIndex[api.Releasing])
	}
	if want := map[string]int{"J1": 2, "J2": 1, "J3": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("evictions per job = %v, want %v", got, want)
	}
}

// TestWorkloadPlacementReprievesVictims evicts the small-loss A replicas first, then B;
// B alone frees enough GPUs, so the A replicas must be reprieved.
func TestWorkloadPlacementReprievesVictims(t *testing.T) {
	gpus := func(n string) v1.ResourceList {
		return api.BuildResourceList("100m", "128Mi", api.ScalarResource{Name: api.GPUResourceName, Value: n})
	}
	node := util.BuildNode("X", api.BuildResourceList("32", "64Gi", api.ScalarResource{Name: "pods", Value: "110"},
		api.ScalarResource{Name: api.GPUResourceName, Value: "8"}), nil)
	node.Annotations = map[string]string{workloadplacement.SchedulingAlgorithmsAnnotation: "rr"}
	fixture := workloadPlacementDRAFixture()
	fixture.Pods, fixture.PodGroups, fixture.ResourceClaims, fixture.ResourceSlices = nil, nil, nil, nil
	fixture.Nodes = []*v1.Node{node}
	for _, job := range []struct {
		name, prio, gpu string
		running, totals int
	}{{"A", "low", "1", 2, 8}, {"B", "low", "6", 1, 2}, {"T", "high", "6", 0, 1}} {
		fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(job.name, "test", "q", 1,
			nil, schedulingv1beta1.PodGroupInqueue, job.prio))
		for r := 1; r <= job.totals; r++ {
			host, phase := "X", v1.PodRunning
			if r > job.running {
				host, phase = "", v1.PodPending
			}
			pod := util.BuildPod("test", fmt.Sprintf("%s-%d", job.name, r), host, phase, gpus(job.gpu), job.name,
				map[string]string{schedulingv1beta1.PodPreemptable: fmt.Sprint(job.name != "T")}, nil)
			pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "rr"
			fixture.Pods = append(fixture.Pods, pod)
		}
	}
	ssn := fixture.RegisterSession(workloadPlacementDRATiers(), []conf.Configuration{{Name: "preempt",
		Arguments: map[string]interface{}{EnableTopologyAwarePreemptionKey: false}}})
	defer fixture.Close()
	fixture.Run([]framework.Action{New()})
	got := map[string]int{}
	for _, group := range []string{"A", "B"} {
		got[group] = len(ssn.Jobs[api.JobID("test/"+group)].TaskStatusIndex[api.Releasing])
	}
	if want := map[string]int{"A": 0, "B": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("evictions per job = %v, want %v", got, want)
	}
	if task := ssn.Jobs["test/T"].Tasks["test-T-1"]; task.Status != api.Pipelined {
		t.Fatalf("T status = %s, want Pipelined", task.Status)
	}
}

// BenchmarkWorkloadPlacementPreempt runs one preempt action for a full-node
// training pod on n nodes, each filled with 8 single-GPU replicas of 16 jobs.
func BenchmarkWorkloadPlacementPreempt(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fixture := workloadPlacementDRAFixture()
				fixture.Pods, fixture.PodGroups, fixture.ResourceClaims, fixture.ResourceSlices, fixture.Nodes = nil, nil, nil, nil, nil
				gpus := func(v string) v1.ResourceList {
					return api.BuildResourceList("100m", "128Mi", api.ScalarResource{Name: api.GPUResourceName, Value: v})
				}
				for j := 0; j < 16; j++ {
					fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(fmt.Sprintf("J%d", j), "test", "q", 1,
						nil, schedulingv1beta1.PodGroupInqueue, "low"))
				}
				fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio("T", "test", "q", 1,
					nil, schedulingv1beta1.PodGroupInqueue, "high"))
				for k := 0; k < n; k++ {
					name := fmt.Sprintf("n%04d", k)
					node := util.BuildNode(name, api.BuildResourceList("32", "64Gi", api.ScalarResource{Name: "pods", Value: "110"},
						api.ScalarResource{Name: api.GPUResourceName, Value: "8"}), nil)
					node.Annotations = map[string]string{workloadplacement.SchedulingAlgorithmsAnnotation: "rr,binpack"}
					fixture.Nodes = append(fixture.Nodes, node)
					for r := 0; r < 8; r++ {
						pod := util.BuildPod("test", fmt.Sprintf("%s-%d", name, r), name, v1.PodRunning, gpus("1"),
							fmt.Sprintf("J%d", (k*8+r)%16), map[string]string{schedulingv1beta1.PodPreemptable: "true"}, nil)
						pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "rr,binpack"
						fixture.Pods = append(fixture.Pods, pod)
					}
				}
				pod := util.BuildPod("test", "T", "", v1.PodPending, gpus("8"), "T",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, nil)
				pod.Annotations[workloadplacement.SchedulingAlgorithmsAnnotation] = "binpack,rr"
				fixture.Pods = append(fixture.Pods, pod)
				ssn := fixture.RegisterSession(workloadPlacementDRATiers(), []conf.Configuration{{Name: "preempt",
					Arguments: map[string]interface{}{EnableTopologyAwarePreemptionKey: false}}})
				b.StartTimer()
				fixture.Run([]framework.Action{New()})
				b.StopTimer()
				if task := ssn.Jobs["test/T"].Tasks["test-T"]; task.Status != api.Pipelined {
					b.Fatalf("T status = %s, want Pipelined", task.Status)
				}
				fixture.Close()
			}
		})
	}
}
