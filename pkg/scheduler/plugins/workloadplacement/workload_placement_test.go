/*
Copyright 2026 The Volcano Authors.

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

package workloadplacement

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
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func policyDRATask(name, algorithms, service string) *api.TaskInfo {
	labels := map[string]string{}
	if service != "" {
		labels["app.kubernetes.io/name"] = service
	}
	task := api.NewTaskInfo(util.BuildPod("test", name, "", v1.PodPending,
		api.BuildResourceList("100m", "128Mi"), service, labels, nil))
	if task.Pod.Annotations == nil {
		task.Pod.Annotations = map[string]string{}
	}
	task.Pod.Annotations[SchedulingAlgorithmsAnnotation] = algorithms
	task.ResourceClaimKeys = []string{"test/" + name + "-claim"}
	task.DRAResreq = map[string]*api.DRAResource{"gpu.example.com": {Count: 1}}
	return task
}

func TestFlexibleNodeAlgorithms(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi", api.ScalarResource{Name: "pods", Value: "110"})
	var nodes []*v1.Node
	for i := 1; i <= 4; i++ {
		node := util.BuildNode(fmt.Sprintf("rr-%d", i), capacity, nil)
		node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: rrAlgorithm}
		nodes = append(nodes, node)
	}
	for i := 1; i <= 2; i++ {
		node := util.BuildNode(fmt.Sprintf("pack-%d", i), capacity, nil)
		node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: binpackAlgorithm}
		nodes = append(nodes, node)
	}
	fixture := &uthelper.TestCommonStruct{Nodes: nodes, Plugins: map[string]framework.PluginBuilder{PluginName: New}}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	for i := 1; i <= 12; i++ {
		pending := policyDRATask(fmt.Sprintf("replica-%d", i), "rr,binpack", "service")
		candidates := map[float64][]*api.NodeInfo{0: {}}
		for _, node := range ssn.Nodes {
			if _, algorithm := nodeAlgorithm(node, []string{rrAlgorithm}); algorithm == rrAlgorithm {
				candidates[0] = append(candidates[0], node)
			}
		}
		best := ssn.BestNodeFn(pending, candidates)
		if best == nil || best.Node.Annotations[SchedulingAlgorithmsAnnotation] != rrAlgorithm {
			t.Fatalf("replica %d did not use RR", i)
		}
		placed := policyDRATask(pending.Name, "rr,binpack", "service")
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

func TestDRAUtilization(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi", api.ScalarResource{Name: "pods", Value: "110"})
	fixture := &uthelper.TestCommonStruct{
		Plugins:       map[string]framework.PluginBuilder{PluginName: New},
		DeviceClasses: []*resourcev1.DeviceClass{util.BuildDeviceClass("gpu.example.com", nil, nil)},
	}
	for _, item := range []struct {
		name, algorithm string
		count, used     int
	}{{"x", binpackAlgorithm, 4, 2}, {"y", binpackAlgorithm, 8, 3}, {"z", rrAlgorithm, 5, 0}} {
		node := util.BuildNode(item.name, capacity, nil)
		node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: item.algorithm}
		fixture.Nodes = append(fixture.Nodes, node)
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
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	task := policyDRATask("new", "rr,binpack", "service")
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["x"], ssn.Nodes["y"], ssn.Nodes["z"]}}); best.Name != "z" {
		t.Fatalf("custom RR zone was not preferred: %s", best.Name)
	}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["x"], ssn.Nodes["y"]}}); best.Name != "x" {
		t.Fatalf("expected 2/4 utilized x before 3/8 utilized y, got %s", best.Name)
	}
}

func TestNominationBlocksReplacement(t *testing.T) {
	training := util.BuildPod("test", "T", "", v1.PodPending, api.BuildResourceList("100m", "128Mi"), "T",
		map[string]string{"app.kubernetes.io/name": "T"}, nil)
	training.Annotations[SchedulingAlgorithmsAnnotation] = "rr"
	training.Status.NominatedNodeName = "target"
	target := util.BuildNode("target", api.BuildResourceList("16", "32Gi"), nil)
	other := util.BuildNode("other", api.BuildResourceList("16", "32Gi"), nil)
	target.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: rrAlgorithm}
	other.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: rrAlgorithm}
	fixture := &uthelper.TestCommonStruct{
		Nodes:     []*v1.Node{target, other},
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
	replacement := policyDRATask("replacement", "rr,binpack", "J1")
	if err := ssn.PredicateFn(replacement, ssn.Nodes["target"]); err == nil ||
		!strings.Contains(err.Error(), "node nominated for higher-priority workload") {
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
	zone := map[string]string{SchedulingAlgorithmsAnnotation: binpackAlgorithm}
	gpu := func(n string) v1.ResourceList {
		return api.BuildResourceList("100m", "128Mi", api.ScalarResource{Name: api.GPUResourceName, Value: n})
	}
	fixture := &uthelper.TestCommonStruct{
		// "a-empty" wins a name tie-break, so only the MostAllocated score can pick "b-used".
		Nodes: []*v1.Node{util.BuildNode("a-empty", capacity, nil), util.BuildNode("b-used", capacity, nil)},
		Pods: []*v1.Pod{util.BuildPod("test", "running", "b-used", v1.PodRunning, gpu("3"), "",
			map[string]string{"app.kubernetes.io/name": "other"}, nil)},
		Plugins: map[string]framework.PluginBuilder{PluginName: New},
	}
	for _, node := range fixture.Nodes {
		node.Annotations = zone
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	task := api.NewTaskInfo(util.BuildPod("test", "new", "", v1.PodPending, gpu("1"), "",
		map[string]string{"app.kubernetes.io/name": "svc"}, nil))
	if task.Pod.Annotations == nil {
		task.Pod.Annotations = map[string]string{}
	}
	task.Pod.Annotations[SchedulingAlgorithmsAnnotation] = binpackAlgorithm
	if err := ssn.PredicateFn(task, ssn.Nodes["a-empty"]); err != nil {
		t.Fatalf("extended-resource task rejected: %v", err)
	}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["a-empty"], ssn.Nodes["b-used"]}}); best.Name != "b-used" {
		t.Fatalf("want most-allocated b-used, got %s", best.Name)
	}
}

func TestWorkloadAlgorithms(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi")
	nodes := []*v1.Node{
		util.BuildNode("rr", capacity, nil),
		util.BuildNode("pack", capacity, nil),
		util.BuildNode("mixed", capacity, nil),
		util.BuildNode("plain", capacity, nil),
	}
	nodes[0].Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "rr"}
	nodes[1].Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "binpack"}
	nodes[2].Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "rr,binpack"}
	fixture := &uthelper.TestCommonStruct{Nodes: nodes, Plugins: map[string]framework.PluginBuilder{PluginName: New}}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	// Same group name in another namespace is a different job.
	foreign := api.NewTaskInfo(util.BuildPod("other", "same-group", "mixed", v1.PodRunning,
		api.BuildResourceList("1", "1Gi"), "work", nil, nil))
	foreign.Status = api.Running
	if err := ssn.Nodes["mixed"].AddTask(foreign); err != nil {
		t.Fatal(err)
	}

	task := api.NewTaskInfo(util.BuildPod("test", "work", "", v1.PodPending,
		api.BuildResourceList("1", "1Gi"), "work", nil, nil))
	task.Pod.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "rr,binpack"}
	scores := map[float64][]*api.NodeInfo{0: {ssn.Nodes["rr"], ssn.Nodes["pack"], ssn.Nodes["mixed"]}}
	if best := ssn.BestNodeFn(task, scores); best.Name != "mixed" {
		t.Fatalf("normal order chose %s, want mixed RR node", best.Name)
	}
	if rank, algorithm := nodeAlgorithm(ssn.Nodes["mixed"], []string{"binpack", "rr"}); rank != 0 || algorithm != "binpack" {
		t.Fatalf("mixed node matched %s at rank %d, want first workload preference", algorithm, rank)
	}
	if err := ssn.PredicateFn(task, ssn.Nodes["plain"]); err == nil ||
		!strings.Contains(err.Error(), "no matching scheduling algorithm") {
		t.Fatalf("annotated workload accepted unannotated node: %v", err)
	}
	task.Pod.Annotations[SchedulingAlgorithmsAnnotation] = "rr"
	if err := ssn.PredicateFn(task, ssn.Nodes["pack"]); err == nil {
		t.Fatal("RR-only workload accepted Binpack-only node")
	}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["pack"]}}); best != nil {
		t.Fatalf("RR-only workload chose unmatched node %s", best.Name)
	}
	delete(task.Pod.Annotations, SchedulingAlgorithmsAnnotation)
	if err := ssn.PredicateFn(task, ssn.Nodes["plain"]); err != nil {
		t.Fatalf("unconfigured workload was restricted: %v", err)
	}
}

func TestInvalidWorkloadAlgorithms(t *testing.T) {
	node := util.BuildNode("node", api.BuildResourceList("16", "32Gi"), nil)
	node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "rr,binpack"}
	fixture := &uthelper.TestCommonStruct{
		Nodes: []*v1.Node{node}, Plugins: map[string]framework.PluginBuilder{PluginName: New},
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes}}}}, nil)
	defer fixture.Close()
	for _, value := range []string{"random", "rr,rr", "", "rr,"} {
		t.Run(value, func(t *testing.T) {
			task := api.NewTaskInfo(util.BuildPod("test", "bad", "", v1.PodPending,
				api.BuildResourceList("1", "1Gi"), "g", nil, nil))
			task.Pod.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: value}
			if err := ssn.PredicateFn(task, ssn.Nodes["node"]); err == nil {
				t.Fatal("invalid workload policy was accepted")
			}
		})
	}
}

func TestCPUAndMemoryBinpack(t *testing.T) {
	capacity := api.BuildResourceList("16", "32Gi")
	empty := util.BuildNode("a-empty", capacity, nil)
	used := util.BuildNode("b-used", capacity, nil)
	for _, node := range []*v1.Node{empty, used} {
		node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "binpack"}
	}
	fixture := &uthelper.TestCommonStruct{
		Nodes: []*v1.Node{empty, used},
		Pods: []*v1.Pod{util.BuildPod("test", "running", "b-used", v1.PodRunning,
			api.BuildResourceList("4", "8Gi"), "", nil, nil)},
		Plugins: map[string]framework.PluginBuilder{PluginName: New},
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	defer fixture.Close()
	task := api.NewTaskInfo(util.BuildPod("test", "new", "", v1.PodPending,
		api.BuildResourceList("1", "1Gi"), "", nil, nil))
	task.Pod.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: "binpack"}
	if best := ssn.BestNodeFn(task, map[float64][]*api.NodeInfo{0: {ssn.Nodes["a-empty"], ssn.Nodes["b-used"]}}); best.Name != "b-used" {
		t.Fatalf("CPU/memory Binpack chose %s, want b-used", best.Name)
	}
}
