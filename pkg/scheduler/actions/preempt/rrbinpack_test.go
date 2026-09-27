package preempt

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	resourcev1 "k8s.io/api/resource/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/nodegroup"
	"volcano.sh/volcano/pkg/scheduler/plugins/pdb"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/rrbinpack"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const draClass = "gpu.example.com"

func TestRRBinpackDRAPreemption(t *testing.T) {
	for _, tc := range []struct {
		name          string
		blockA        bool
		blockRR       bool
		blockBinpack  bool
		sharedA       bool
		missingA      bool
		wantNode      string
		wantEvictions int
	}{
		{name: "RR before fewer Binpack victims", wantNode: "A", wantEvictions: 8},
		{name: "PDB blocks A, use B", blockA: true, wantNode: "B", wantEvictions: 8},
		{name: "PDB blocks RR, use Binpack", blockRR: true, wantNode: "D", wantEvictions: 6},
		{name: "PDB blocks all, leave pending", blockRR: true, blockBinpack: true},
		{name: "shared victim claim skips A", sharedA: true, wantNode: "B", wantEvictions: 8},
		{name: "missing victim claim skips A", missingA: true, wantNode: "B", wantEvictions: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := rrBinpackDRAFixture()
			if tc.sharedA {
				for _, claim := range fixture.ResourceClaims {
					if claim.Name == "J1-1-gpu" {
						claim.Status.ReservedFor = append(claim.Status.ReservedFor,
							resourcev1.ResourceClaimConsumerReference{Resource: "pods", Name: "other", UID: "other"})
						break
					}
				}
			}
			ssn := fixture.RegisterSession(rrBinpackDRATiers(), []conf.Configuration{{Name: "preempt",
				Arguments: map[string]interface{}{EnableTopologyAwarePreemptionKey: false}}})
			defer fixture.Close()
			if tc.missingA {
				found := false
				for _, task := range ssn.Nodes["A"].Tasks {
					if task.Name == "J1-1" {
						task.ResourceClaimKeys = []string{"test/missing-gpu"}
						found = true
					}
				}
				if !found {
					t.Fatal("victim J1-1 missing from node A")
				}
			}
			store := ssn.InformerFactory().Policy().V1().PodDisruptionBudgets().Informer().GetStore()
			for _, rule := range []struct {
				enabled            bool
				name, label, value string
			}{
				{tc.blockA, "a-pdb", "node-name", "A"},
				{tc.blockRR, "rr-pdb", "placement", "rr"},
				{tc.blockBinpack, "binpack-pdb", "placement", "binpack"},
			} {
				if rule.enabled {
					if err := store.Add(blockingDRAPDB(rule.name, rule.label, rule.value)); err != nil {
						t.Fatal(err)
					}
				}
			}
			fixture.ExpectEvictNum = tc.wantEvictions
			if tc.wantNode == "A" || tc.wantNode == "B" {
				for service := 1; service <= 4; service++ {
					replicas := []int{1, 4}
					if tc.wantNode == "B" {
						replicas = []int{2, 5}
					}
					for _, replica := range replicas {
						fixture.ExpectEvicted = append(fixture.ExpectEvicted, fmt.Sprintf("test/J%d-%d", service, replica))
					}
				}
			} else if tc.wantNode == "D" {
				for replica := 1; replica <= 6; replica++ {
					fixture.ExpectEvicted = append(fixture.ExpectEvicted, fmt.Sprintf("test/J5-%d", replica))
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
				replacement := api.NewTaskInfo(util.BuildPod("test", "replacement", "", v1.PodPending,
					api.BuildResourceList("100m", "128Mi"), "J1", map[string]string{
						rrbinpack.WorkloadClassLabel: "inference", "app.kubernetes.io/name": "J1"}, nil))
				replacement.ResourceClaimKeys = []string{"test/replacement-gpu"}
				replacement.DRAResreq = map[string]*api.DRAResource{draClass: {Count: 1}}
				if err := ssn.PredicateFn(replacement, ssn.Nodes[tc.wantNode]); err == nil ||
					!strings.Contains(err.Error(), "node nominated for training") {
					t.Fatalf("replacement was not blocked on nominated node: %v", err)
				}
			} else if task := ssn.Jobs["test/T2"].Tasks["test-T2"]; task != nil && task.Status != api.Pending {
				t.Fatalf("T2 status = %s, want Pending", task.Status)
			}
		})
	}
}

func TestRRBinpackDRAAfterRelease(t *testing.T) {
	fixture := rrBinpackDRAFixture()
	for _, pod := range fixture.Pods {
		if pod.Spec.NodeName == "A" {
			pod.Spec.NodeName = ""
			pod.Status.Phase = v1.PodPending
		}
		if pod.Name == "T2" {
			pod.Status.NominatedNodeName = "A"
		}
	}
	for _, claim := range fixture.ResourceClaims {
		if len(claim.Status.ReservedFor) > 0 {
			name := claim.Status.ReservedFor[0].Name
			for _, pod := range fixture.Pods {
				if pod.Name == name && pod.Spec.NodeName == "" && pod.Name != "T2" {
					claim.Status.Allocation = nil
					claim.Status.ReservedFor = nil
					break
				}
			}
		}
	}
	ssn := fixture.RegisterSession(rrBinpackDRATiers(), nil)
	defer fixture.Close()
	fixture.ExpectBindsNum = 3
	fixture.MinimalBindCheck = true
	fixture.Run([]framework.Action{allocate.New()})
	if err := fixture.CheckBind(0); err != nil {
		t.Fatal(err)
	}
	training := ssn.Jobs["test/T2"].Tasks["test-T2"]
	if training == nil || training.NodeName != "A" || training.Status != api.Binding {
		t.Fatalf("T2 after release: %v, want Binding on A", training)
	}
	bound, pending := 0, 0
	for service := 1; service <= 4; service++ {
		for _, task := range ssn.Jobs[api.JobID(fmt.Sprintf("test/J%d", service))].Tasks {
			if task.Pod.Labels["node-name"] != "A" {
				continue
			}
			switch task.Status {
			case api.Binding:
				if task.NodeName != "D" {
					t.Errorf("replacement %s bound to %s, want D", task.Name, task.NodeName)
				}
				bound++
			case api.Pending:
				pending++
			default:
				t.Errorf("replacement %s status = %s", task.Name, task.Status)
			}
		}
	}
	if bound != 2 || pending != 6 {
		t.Fatalf("replacements: %d bound, %d pending; want 2 and 6", bound, pending)
	}
}

func TestRRBinpackDRAInitialPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, pending string
		groups        []string
		wantBinds     int
	}{
		{name: "four services spread across RR", groups: []string{"J1", "J2", "J3", "J4"},
			pending: "all", wantBinds: 24},
		{name: "J5 overflows to D", groups: []string{"J1", "J2", "J3", "J4", "J5"},
			pending: "J5", wantBinds: 6},
		{name: "T1 uses E", groups: []string{"J1", "J2", "J3", "J4", "J5", "T1"},
			pending: "T1", wantBinds: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := rrBinpackDRAFixture()
			fixture.PodGroups = slices.DeleteFunc(fixture.PodGroups, func(group *schedulingv1beta1.PodGroup) bool {
				return !slices.Contains(tc.groups, group.Name)
			})
			fixture.Pods = slices.DeleteFunc(fixture.Pods, func(pod *v1.Pod) bool {
				group := strings.SplitN(pod.Name, "-", 2)[0]
				if !slices.Contains(tc.groups, group) {
					return true
				}
				if tc.pending == "all" || tc.pending == group {
					pod.Spec.NodeName = ""
					pod.Status.Phase = v1.PodPending
				}
				return false
			})
			fixture.ResourceClaims = slices.DeleteFunc(fixture.ResourceClaims, func(claim *resourcev1.ResourceClaim) bool {
				group := strings.SplitN(claim.Name, "-", 2)[0]
				if !slices.Contains(tc.groups, group) {
					return true
				}
				if tc.pending == "all" || tc.pending == group {
					claim.Status.Allocation = nil
					claim.Status.ReservedFor = nil
				}
				return false
			})
			ssn := fixture.RegisterSession(rrBinpackDRATiers(), nil)
			defer fixture.Close()
			fixture.ExpectBindsNum = tc.wantBinds
			fixture.MinimalBindCheck = true
			fixture.Run([]framework.Action{allocate.New()})
			if err := fixture.CheckBind(0); err != nil {
				t.Fatal(err)
			}
			for _, group := range tc.groups {
				if tc.pending != "all" && tc.pending != group {
					continue
				}
				counts := map[string]int{}
				for _, task := range ssn.Jobs[api.JobID("test/"+group)].Tasks {
					if task.Status != api.Binding {
						t.Errorf("%s status = %s, want Binding", task.Name, task.Status)
					}
					counts[task.NodeName]++
				}
				switch group {
				case "J1", "J2", "J3", "J4":
					if counts["A"] != 2 || counts["B"] != 2 || counts["C"] != 2 || len(counts) != 3 {
						t.Errorf("%s distribution = %v, want 2/2/2 on A/B/C", group, counts)
					}
				case "J5":
					if counts["D"] != 6 || len(counts) != 1 {
						t.Errorf("J5 distribution = %v, want 6 on D", counts)
					}
				case "T1":
					if counts["E"] != 1 || len(counts) != 1 {
						t.Errorf("T1 distribution = %v, want E", counts)
					}
				}
			}
		})
	}
}

func blockingDRAPDB(name, label, value string) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{label: value},
		}},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
	}
}

func rrBinpackDRAFixture() *uthelper.TestCommonStruct {
	capacity := api.BuildResourceList("32", "64Gi", api.ScalarResource{Name: "pods", Value: "110"})
	fixture := &uthelper.TestCommonStruct{
		Plugins: map[string]framework.PluginBuilder{
			conformance.PluginName: conformance.New,
			gang.PluginName:        gang.New,
			priority.PluginName:    priority.New,
			pdb.PluginName:         pdb.New,
			predicates.PluginName:  predicates.New,
			rrbinpack.PluginName:   rrbinpack.New,
		},
		Queues:        []*schedulingv1beta1.Queue{util.BuildQueue("q", 1, nil)},
		DeviceClasses: []*resourcev1.DeviceClass{util.BuildDeviceClass(draClass, nil, nil)},
	}
	for _, name := range []string{"A", "B", "C", "D", "E"} {
		zone := "rr"
		if name >= "D" {
			zone = "binpack"
		}
		fixture.Nodes = append(fixture.Nodes, util.BuildNode(name, capacity, map[string]string{nodegroup.NodeGroupNameKey: zone}))
		devices := make([]resourcev1.Device, 8)
		for i := range devices {
			devices[i] = util.BuildDevice(fmt.Sprintf("gpu-%d", i), nil, nil)
		}
		fixture.ResourceSlices = append(fixture.ResourceSlices, util.BuildResourceSlice(name+"-slice", draClass, name,
			resourcev1.ResourcePool{Name: name, Generation: 1, ResourceSliceCount: 1}, devices))
	}
	addPod := func(name, group, node, class, priority string, count int64, labels map[string]string) {
		claimName := name + "-gpu"
		claim := util.BuildResourceClaim("test", claimName,
			[]resourcev1.DeviceRequest{util.BuildDeviceRequest("gpu", draClass, nil, nil, &count)}, nil, nil)
		claim.UID = types.UID("claim-" + claimName)
		if node != "" {
			results := make([]resourcev1.DeviceRequestAllocationResult, count)
			for i := range results {
				deviceNumber := int64(0)
				if count == 1 {
					if group == "J5" {
						_, _ = fmt.Sscanf(name, "J5-%d", &deviceNumber)
						deviceNumber--
					} else {
						var service, replica int
						_, _ = fmt.Sscanf(name, "J%d-%d", &service, &replica)
						deviceNumber = int64((service-1)*2 + (replica-1)/3)
					}
				} else {
					deviceNumber = int64(i)
				}
				results[i] = resourcev1.DeviceRequestAllocationResult{Request: "gpu", Driver: draClass, Pool: node,
					Device: fmt.Sprintf("gpu-%d", deviceNumber)}
			}
			claim.Status.Allocation = &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: results}}
			claim.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: name,
				UID: types.UID("test-" + name)}}
		}
		fixture.ResourceClaims = append(fixture.ResourceClaims, claim)
		labels[rrbinpack.WorkloadClassLabel] = class
		pod := util.BuildPodWithResourceClaim("test", name, node, v1.PodPending, api.BuildResourceList("100m", "128Mi"),
			group, labels, nil, []v1.ResourceClaim{{Name: "gpu", Request: "gpu"}},
			[]v1.PodResourceClaim{{Name: "gpu", ResourceClaimName: ptr.To(claimName)}})
		if node != "" {
			pod.Status.Phase = v1.PodRunning
		}
		fixture.Pods = append(fixture.Pods, pod)
	}
	for service := 1; service <= 5; service++ {
		group := fmt.Sprintf("J%d", service)
		fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(group, "test", "q", 0,
			map[string]int32{}, schedulingv1beta1.PodGroupInqueue, "low"))
		for replica := 1; replica <= 6; replica++ {
			node := "D"
			if service < 5 {
				node = []string{"A", "B", "C"}[(replica-1)%3]
			}
			addPod(fmt.Sprintf("%s-%d", group, replica), group, node, "inference", "low", 1,
				map[string]string{"app.kubernetes.io/name": group, "placement": zoneForDRANode(node), "node-name": node,
					schedulingv1beta1.PodPreemptable: "true"})
		}
	}
	for _, training := range []struct{ name, node, priority string }{{"T1", "E", "medium"}, {"T2", "", "high"}} {
		fixture.PodGroups = append(fixture.PodGroups, util.BuildPodGroupWithPrio(training.name, "test", "q", 1,
			map[string]int32{"": 1}, schedulingv1beta1.PodGroupInqueue, training.priority))
		addPod(training.name, training.name, training.node, "training", training.priority, 8, map[string]string{})
	}
	fixture.PriClass = []*schedulingv1.PriorityClass{util.BuildPriorityClass("low", 10),
		util.BuildPriorityClass("medium", 50), util.BuildPriorityClass("high", 100)}
	return fixture
}

func zoneForDRANode(name string) string {
	if name == "D" || name == "E" {
		return "binpack"
	}
	return "rr"
}

func rrBinpackDRATiers() []conf.Tier {
	yes, no := true, false
	return []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: conformance.PluginName, EnabledPreemptable: &yes},
		{Name: gang.PluginName, EnabledPreemptable: &no, EnabledJobPipelined: &yes},
		{Name: priority.PluginName, EnabledPreemptable: &yes, EnabledJobOrder: &yes, EnabledTaskOrder: &yes, EnabledJobStarving: &yes},
		{Name: pdb.PluginName, EnabledPreemptable: &yes},
		{Name: rrbinpack.PluginName, EnabledPreemptable: &yes, EnabledPredicate: &yes, EnabledBestNode: &yes},
		{Name: predicates.PluginName, EnabledPredicate: &yes,
			Arguments: framework.Arguments{predicates.DynamicResourceAllocationEnable: true}},
	}}}
}
