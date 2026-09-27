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
	"volcano.sh/volcano/pkg/scheduler/plugins/nodegroup"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// rrBinpackGPUFixture builds the A/B/C (rr) + D/E (binpack) reference cluster
// with 8 extended-resource GPUs per node. Groups in running are placed as in
// the reference scenario; groups in pending are created unscheduled.
func rrBinpackGPUFixture(running, pending []string) *uthelper.TestCommonStruct {
	gpus := func(n string) api.ScalarResource { return api.ScalarResource{Name: api.GPUResourceName, Value: n} }
	fixture := rrBinpackDRAFixture()
	fixture.Pods, fixture.PodGroups, fixture.ResourceClaims, fixture.ResourceSlices, fixture.Nodes = nil, nil, nil, nil, nil
	for _, name := range []string{"A", "B", "C", "D", "E"} {
		fixture.Nodes = append(fixture.Nodes, util.BuildNode(name, api.BuildResourceList("32", "64Gi",
			api.ScalarResource{Name: "pods", Value: "110"}, gpus("8")),
			map[string]string{nodegroup.NodeGroupNameKey: zoneForDRANode(name)}))
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
			labels := map[string]string{"volcano.sh/workload-class": class, "placement": zoneForDRANode(node), "node-name": node}
			if class == "inference" {
				labels["app.kubernetes.io/name"] = group
				labels[schedulingv1beta1.PodPreemptable] = "true"
			}
			phase := v1.PodRunning
			if !isRunning {
				node, phase = "", v1.PodPending
			}
			fixture.Pods = append(fixture.Pods, util.BuildPod("test", name, node, phase,
				api.BuildResourceList("100m", "128Mi", gpus(gpu)), group, labels, nil))
		}
	}
	for _, g := range running {
		add(g, true)
	}
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

func TestRRBinpackGPUAllocatePhases(t *testing.T) {
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
			fixture := rrBinpackGPUFixture(tc.running, tc.pending)
			ssn := fixture.RegisterSession(rrBinpackDRATiers(), nil)
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

func TestRRBinpackGPUPreemption(t *testing.T) {
	running := []string{"J1", "J2", "J3", "J4", "J5", "T1"}
	for _, tc := range []struct {
		name               string
		pdbLabel, pdbValue string
		wantNode           string
		wantEvictions      int
	}{
		{name: "RR A before Binpack D with fewer victims; E protected", wantNode: "A", wantEvictions: 8},
		{name: "PDB on A, use B", pdbLabel: "node-name", pdbValue: "A", wantNode: "B", wantEvictions: 8},
		{name: "PDB on RR, last resort D", pdbLabel: "placement", pdbValue: "rr", wantNode: "D", wantEvictions: 6},
		{name: "PDB on everything, T2 stays pending", pdbLabel: "volcano.sh/workload-class", pdbValue: "inference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := rrBinpackGPUFixture(running, []string{"T2"})
			ssn := fixture.RegisterSession(rrBinpackDRATiers(), []conf.Configuration{{Name: "preempt",
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
				if tc.wantNode != "" && pod.Spec.NodeName == tc.wantNode {
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
func TestRRBinpackGPUReplacements(t *testing.T) {
	fixture := rrBinpackGPUFixture([]string{"J1", "J2", "J3", "J4", "J5", "T1"}, []string{"T2"})
	for _, pod := range fixture.Pods {
		if pod.Spec.NodeName == "A" {
			pod.Spec.NodeName, pod.Status.Phase = "", v1.PodPending
		}
		if pod.Name == "T2" {
			pod.Status.NominatedNodeName = "A"
		}
	}
	ssn := fixture.RegisterSession(rrBinpackDRATiers(), nil)
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
