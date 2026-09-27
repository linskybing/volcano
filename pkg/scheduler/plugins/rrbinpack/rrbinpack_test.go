package rrbinpack

import (
	"fmt"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/types"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/nodegroup"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func policyDRATask(name, class, service string) *api.TaskInfo {
	labels := map[string]string{WorkloadClassLabel: class}
	if service != "" {
		labels[defaultServiceIdentity] = service
	}
	task := api.NewTaskInfo(util.BuildPod("test", name, "", v1.PodPending,
		api.BuildResourceList("100m", "128Mi"), "", labels, nil))
	task.ResourceClaimKeys = []string{"test/" + name + "-claim"}
	task.DRAResreq = map[string]*api.DRAResource{"gpu.example.com": {Count: 1}}
	return task
}

func TestFlexibleNodeGroups(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi", api.ScalarResource{Name: "pods", Value: "110"})
	var nodes []*v1.Node
	for i := 1; i <= 4; i++ {
		nodes = append(nodes, util.BuildNode(fmt.Sprintf("rr-%d", i), capacity,
			map[string]string{nodegroup.NodeGroupNameKey: rrZone}))
	}
	for i := 1; i <= 2; i++ {
		nodes = append(nodes, util.BuildNode(fmt.Sprintf("pack-%d", i), capacity,
			map[string]string{nodegroup.NodeGroupNameKey: binpackZone}))
	}
	fixture := &uthelper.TestCommonStruct{Nodes: nodes, Plugins: map[string]framework.PluginBuilder{PluginName: New}}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	p := New(nil).(*plugin)
	for i := 1; i <= 12; i++ {
		pending := policyDRATask(fmt.Sprintf("replica-%d", i), inferenceClass, "service")
		candidates := map[float64][]*api.NodeInfo{0: {}}
		for _, node := range ssn.Nodes {
			if p.nodeZone(node) == rrZone {
				candidates[0] = append(candidates[0], node)
			}
		}
		best := ssn.BestNodeFn(pending, candidates)
		if best == nil || p.nodeZone(best) != rrZone {
			t.Fatalf("replica %d did not use RR", i)
		}
		placed := policyDRATask(pending.Name, inferenceClass, "service")
		placed.Status = api.Running
		placed.NodeName = best.Name
		if err := best.AddTask(placed); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 4; i++ {
		if got := len(ssn.Nodes[fmt.Sprintf("rr-%d", i)].Tasks); got != 3 {
			t.Fatalf("RR node %d has %d replicas, want 3", i, got)
		}
	}
	for i := 1; i <= 2; i++ {
		if got := len(ssn.Nodes[fmt.Sprintf("pack-%d", i)].Tasks); got != 0 {
			t.Fatalf("Binpack node %d used early", i)
		}
	}
}

func TestDRAUtilizationAndConfig(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi", api.ScalarResource{Name: "pods", Value: "110"})
	fixture := &uthelper.TestCommonStruct{
		Plugins:       map[string]framework.PluginBuilder{PluginName: New},
		DeviceClasses: []*resourcev1.DeviceClass{util.BuildDeviceClass("gpu.example.com", nil, nil)},
	}
	for _, item := range []struct {
		name, zone  string
		count, used int
	}{{"x", "overflow", 4, 2}, {"y", "overflow", 8, 3}, {"z", "spread", 5, 0}} {
		fixture.Nodes = append(fixture.Nodes, util.BuildNode(item.name, capacity,
			map[string]string{"site.example/zone": item.zone}))
		devices := make([]resourcev1.Device, item.count)
		for i := range devices {
			devices[i] = util.BuildDevice(fmt.Sprintf("gpu-%d", i), nil, nil)
		}
		fixture.ResourceSlices = append(fixture.ResourceSlices, util.BuildResourceSlice(item.name+"-slice", "gpu.example.com", item.name,
			resourcev1.ResourcePool{Name: item.name, Generation: 1, ResourceSliceCount: 1}, devices))
		for i := 0; i < item.used; i++ {
			name := fmt.Sprintf("%s-claim-%d", item.name, i)
			claim := util.BuildResourceClaim("test", name,
				[]resourcev1.DeviceRequest{util.BuildDeviceRequest("gpu", "gpu.example.com", nil, nil, nil)}, nil, nil)
			claim.Status.Allocation = &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{
				Request: "gpu", Driver: "gpu.example.com", Pool: item.name, Device: fmt.Sprintf("gpu-%d", i),
			}}}}
			claim.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: name, UID: types.UID(name)}}
			fixture.ResourceClaims = append(fixture.ResourceClaims, claim)
		}
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes,
		Arguments: framework.Arguments{"nodeZoneLabel": "site.example/zone", "rrZoneValue": "spread",
			"binpackZoneValue": "overflow", "workloadClassLabel": "site.example/type",
			"inferenceValue": "serve", "trainingValue": "batch"}}}}}, nil)
	defer fixture.Close()
	task := policyDRATask("new", inferenceClass, "service")
	delete(task.Pod.Labels, WorkloadClassLabel)
	task.Pod.Labels["site.example/type"] = "serve"
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["x"], ssn.Nodes["y"], ssn.Nodes["z"]}}); best.Name != "z" {
		t.Fatalf("custom RR zone was not preferred: %s", best.Name)
	}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["x"], ssn.Nodes["y"]}}); best.Name != "x" {
		t.Fatalf("expected 2/4 utilized x before 3/8 utilized y, got %s", best.Name)
	}
}

func TestNominationBlocksReplacement(t *testing.T) {
	zone := map[string]string{nodegroup.NodeGroupNameKey: rrZone}
	training := util.BuildPod("test", "T", "", v1.PodPending, api.BuildResourceList("100m", "128Mi"), "T",
		map[string]string{WorkloadClassLabel: trainingClass}, nil)
	training.Status.NominatedNodeName = "target"
	fixture := &uthelper.TestCommonStruct{
		Nodes: []*v1.Node{util.BuildNode("target", api.BuildResourceList("16", "32Gi"), zone),
			util.BuildNode("other", api.BuildResourceList("16", "32Gi"), zone)},
		Pods:      []*v1.Pod{training},
		PodGroups: []*schedulingv1beta1.PodGroup{util.BuildPodGroupWithPrio("T", "test", "q", 1, nil, schedulingv1beta1.PodGroupInqueue, "high")},
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("q", 1, nil)},
		PriClass:  []*schedulingv1.PriorityClass{util.BuildPriorityClass("high", 100)},
		Plugins:   map[string]framework.PluginBuilder{PluginName: New},
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes}}}}, nil)
	defer fixture.Close()
	// The pod has no priority of its own: only the PodGroup priority marks T as more important.
	replacement := policyDRATask("replacement", inferenceClass, "J1")
	if err := ssn.PredicateFn(replacement, ssn.Nodes["target"]); err == nil ||
		!strings.Contains(err.Error(), "node nominated for training") {
		t.Fatalf("replacement was not blocked during nomination: %v", err)
	}
	if err := ssn.PredicateFn(replacement, ssn.Nodes["other"]); err != nil {
		t.Fatalf("replacement blocked on a node without nomination: %v", err)
	}
	replacement.Priority = 1000
	if err := ssn.PredicateFn(replacement, ssn.Nodes["target"]); err != nil {
		t.Fatalf("higher-priority inference blocked by nomination: %v", err)
	}
}

func TestExtendedResourceBinpack(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi", api.ScalarResource{Name: "pods", Value: "110"},
		api.ScalarResource{Name: api.GPUResourceName, Value: "8"})
	zone := map[string]string{nodegroup.NodeGroupNameKey: binpackZone}
	gpu := func(n string) v1.ResourceList {
		return api.BuildResourceList("100m", "128Mi", api.ScalarResource{Name: api.GPUResourceName, Value: n})
	}
	fixture := &uthelper.TestCommonStruct{
		// "a-empty" wins a name tie-break, so only the MostAllocated score can pick "b-used".
		Nodes: []*v1.Node{util.BuildNode("a-empty", capacity, zone), util.BuildNode("b-used", capacity, zone)},
		Pods: []*v1.Pod{util.BuildPod("test", "running", "b-used", v1.PodRunning, gpu("3"), "",
			map[string]string{WorkloadClassLabel: inferenceClass, defaultServiceIdentity: "other"}, nil)},
		Plugins: map[string]framework.PluginBuilder{PluginName: New},
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	task := api.NewTaskInfo(util.BuildPod("test", "new", "", v1.PodPending, gpu("1"), "",
		map[string]string{WorkloadClassLabel: inferenceClass, defaultServiceIdentity: "svc"}, nil))
	if err := ssn.PredicateFn(task, ssn.Nodes["a-empty"]); err != nil {
		t.Fatalf("extended-resource task rejected: %v", err)
	}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["a-empty"], ssn.Nodes["b-used"]}}); best.Name != "b-used" {
		t.Fatalf("want most-allocated b-used, got %s", best.Name)
	}
}
