package workloadplacement

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// benchSession builds n nodes running 16 single-replica-per-node pods from 16 jobs,
// optionally with 16 DRA devices per node of which 8 are allocated.
func benchSession(b *testing.B, n int, algorithm string, dra bool) (*framework.Session, *uthelper.TestCommonStruct, map[float64][]*api.NodeInfo) {
	capacity := api.BuildResourceList("64", "256Gi", api.ScalarResource{Name: "pods", Value: "110"})
	fixture := &uthelper.TestCommonStruct{Plugins: map[string]framework.PluginBuilder{PluginName: New}}
	if dra {
		fixture.DeviceClasses = []*resourcev1.DeviceClass{util.BuildDeviceClass("gpu.example.com", nil, nil)}
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("n%05d", i)
		node := util.BuildNode(name, capacity, nil)
		node.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: algorithm}
		fixture.Nodes = append(fixture.Nodes, node)
		for j := 0; j < 16; j++ {
			fixture.Pods = append(fixture.Pods, util.BuildPod("test", fmt.Sprintf("%s-p%d", name, j), name, v1.PodRunning,
				api.BuildResourceList("1", "1Gi"), fmt.Sprintf("job-%d", (i+j)%16), map[string]string{"app": fmt.Sprintf("job-%d", (i+j)%16)}, nil))
		}
		if !dra {
			continue
		}
		devices := make([]resourcev1.Device, 16)
		for d := range devices {
			devices[d] = util.BuildDevice(fmt.Sprintf("gpu-%d", d), nil, nil)
		}
		fixture.ResourceSlices = append(fixture.ResourceSlices, util.BuildResourceSlice(name+"-slice", "gpu.example.com", name,
			resourcev1.ResourcePool{Name: name, Generation: 1, ResourceSliceCount: 1}, devices))
		for d := 0; d < i%9; d++ {
			claim := util.BuildResourceClaim("test", fmt.Sprintf("%s-c%d", name, d),
				[]resourcev1.DeviceRequest{util.BuildDeviceRequest("gpu", "gpu.example.com", nil, nil, nil)}, nil, nil)
			claim.Status.Allocation = &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{
				Request: "gpu", Driver: "gpu.example.com", Pool: name, Device: fmt.Sprintf("gpu-%d", d),
			}}}}
			fixture.ResourceClaims = append(fixture.ResourceClaims, claim)
		}
	}
	yes := true
	ssn := fixture.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: PluginName,
		EnabledPredicate: &yes, EnabledBestNode: &yes}}}}, nil)
	scores := map[float64][]*api.NodeInfo{}
	for i, node := range ssn.NodeList {
		scores[float64(i%10)] = append(scores[float64(i%10)], node)
	}
	return ssn, fixture, scores
}

func BenchmarkBestNode(b *testing.B) {
	for _, n := range []int{100, 1000} {
		for _, tc := range []struct {
			name, algorithm string
			dra             bool
		}{{"rr", rrAlgorithm, false}, {"binpack", binpackAlgorithm, false}, {"binpackDRA", binpackAlgorithm, true}} {
			b.Run(fmt.Sprintf("%s/nodes=%d", tc.name, n), func(b *testing.B) {
				ssn, fixture, scores := benchSession(b, n, tc.algorithm, tc.dra)
				defer fixture.Close()
				task := api.NewTaskInfo(util.BuildPod("test", "new", "", v1.PodPending,
					api.BuildResourceList("1", "1Gi"), "job-3", map[string]string{"app": "job-3"}, nil))
				task.Pod.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: tc.algorithm}
				if tc.dra {
					task.ResourceClaimKeys = []string{"test/new-claim"}
					task.DRAResreq = map[string]*api.DRAResource{"gpu.example.com": {Count: 1}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if ssn.BestNodeFn(task, scores) == nil {
						b.Fatal("no node")
					}
				}
			})
		}
	}
}

func FuzzTaskPolicy(f *testing.F) {
	for _, seed := range []string{"rr", "binpack", "rr,binpack", "binpack, rr", "rr,rr", "", ",", "random"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		task := api.NewTaskInfo(util.BuildPod("test", "p", "", v1.PodPending, api.BuildResourceList("1", "1Gi"), "g", nil, nil))
		task.Pod.Annotations = map[string]string{SchedulingAlgorithmsAnnotation: value}
		algorithms, configured, err := taskPolicy(task)
		if !configured {
			t.Fatal("annotated task reported as unconfigured")
		}
		if err == nil && (len(algorithms) == 0 || len(algorithms) > 2) {
			t.Fatalf("%q parsed to %v", value, algorithms)
		}
	})
}
