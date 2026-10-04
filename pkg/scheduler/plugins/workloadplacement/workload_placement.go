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
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
)

const (
	PluginName                     = "workload-placement"
	SchedulingAlgorithmsAnnotation = "volcano.sh/scheduling-algorithms"
	rrAlgorithm                    = "rr"
	binpackAlgorithm               = "binpack"
)

type plugin struct {
	celCache *cel.Cache
	matched  map[classSlice][]matchedDevice
}

type classSlice struct {
	class string
	slice *resourcev1.ResourceSlice
}

type matchedDevice struct {
	device *resourcev1.Device
	id     structured.DeviceID
}

type usage struct{ used, total int }

func New(framework.Arguments) framework.Plugin { return &plugin{} }

func (p *plugin) Name() string { return PluginName }

func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	p.celCache, p.matched = cel.NewCache(10, cel.Features{}), map[classSlice][]matchedDevice{}
	ssn.AddDRAPreemptionEligibleFn(p.Name(), func(task *api.TaskInfo) bool {
		_, configured, err := taskPolicy(task)
		return configured && err == nil && len(task.DRAResreq) > 0
	})
	jobPriority := func(task *api.TaskInfo) int32 {
		if job := ssn.Jobs[task.Job]; job != nil {
			return job.Priority
		}
		return task.Priority
	}
	nominated := map[string]int32{}
	for _, job := range ssn.Jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			node := task.Pod.Status.NominatedNodeName
			if node == "" {
				continue
			}
			if prio, ok := nominated[node]; !ok || job.Priority > prio {
				nominated[node] = job.Priority
			}
		}
	}
	reservedForHigherPriority := func(task *api.TaskInfo, node *api.NodeInfo) bool {
		prio := jobPriority(task)
		if nominee, ok := nominated[node.Name]; ok && nominee > prio {
			return true
		}
		for _, occupant := range node.Tasks {
			if occupant.Status == api.Pipelined && jobPriority(occupant) > prio {
				return true
			}
		}
		return false
	}

	// Also used for preemption: the framework falls back to predicateFns there.
	ssn.AddPredicateFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		order, configured, err := taskPolicy(task)
		if !configured {
			return nil
		}
		if err != nil {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: err.Error()})
		}
		if _, algorithm := nodeAlgorithm(node, order); algorithm == "" {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: "node has no matching scheduling algorithm"})
		}
		if reservedForHigherPriority(task, node) {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: "node nominated for higher-priority workload"})
		}
		return nil
	})

	ssn.AddBestNodeFn(p.Name(), func(task *api.TaskInfo, scores map[float64][]*api.NodeInfo) *api.NodeInfo {
		order, configured, err := taskPolicy(task)
		if !configured || err != nil {
			return nil
		}
		var pack func(*api.NodeInfo) float64
		var best *api.NodeInfo
		bestRank, bestKey, bestScore := len(order), 0.0, 0.0
		for score, nodes := range scores {
			for _, node := range nodes {
				rank, algorithm := nodeAlgorithm(node, order)
				if algorithm == "" || rank > bestRank {
					continue
				}
				// Higher is better: fewer replicas of the job for RR, higher utilization for Binpack.
				var key float64
				if algorithm == rrAlgorithm {
					key = -float64(jobReplicas(node, task.Job))
				} else {
					if pack == nil {
						pack = p.packScorer(ssn, task)
					}
					key = pack(node)
				}
				if best == nil || rank < bestRank || key > bestKey || key == bestKey &&
					(score > bestScore || score == bestScore && node.Name < best.Name) {
					best, bestRank, bestKey, bestScore = node, rank, key, score
				}
			}
		}
		if best != nil {
			klog.V(4).Infof("workload-placement selected node %s for task %s/%s", best.Name, task.Namespace, task.Name)
		}
		return best
	})

	f := newFairness(ssn)
	ssn.AddPreemptOrderFns(p.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) []*api.NodeInfo {
		if _, configured, err := taskPolicy(task); !configured || err != nil {
			return nil
		}
		dra, devices := p.draUsage(ssn, task), int64(0)
		for _, req := range task.DRAResreq {
			devices += req.Count
		}
		type cost struct {
			prio      int32
			loss      float64
			evictions int
		}
		costs := make(map[string]cost, len(nodes))
		for _, node := range nodes {
			var candidates []*api.TaskInfo
			for _, occupant := range node.Tasks {
				if api.PreemptableStatus(occupant.Status) && occupant.Preemptable && occupant.Job != task.Job {
					candidates = append(candidates, occupant.Clone())
				}
			}
			// ponytail: estimates fit from requests and free DRA devices; the preempt action still verifies it.
			free, need := node.FutureIdle(), devices
			if dra != nil {
				u := dra(node)
				need -= int64(u.total - u.used)
			}
			fits := func() bool { return task.InitResreq.LessEqual(free, api.Zero) && need <= 0 }
			c, evicted := cost{prio: math.MinInt32}, map[api.JobID]int{}
			for _, victim := range f.order(ssn.Preemptable(task, candidates), jobPriority) {
				if fits() {
					break
				}
				free.Add(victim.Resreq)
				for _, req := range victim.DRAResreq {
					need -= req.Count
				}
				evicted[victim.Job]++
				c.prio, c.loss = max(c.prio, jobPriority(victim)), max(c.loss, f.loss(victim.Job, evicted[victim.Job]))
				c.evictions++
			}
			if !fits() {
				c.loss = math.Inf(1)
			}
			costs[node.Name] = c
		}
		ordered := slices.Clone(nodes)
		sort.Slice(ordered, func(i, j int) bool {
			a, b := costs[ordered[i].Name], costs[ordered[j].Name]
			if a.prio != b.prio {
				return a.prio < b.prio
			}
			if a.loss != b.loss {
				return a.loss < b.loss
			}
			if a.evictions != b.evictions {
				return a.evictions < b.evictions
			}
			return ordered[i].Name < ordered[j].Name
		})
		return ordered
	}, func(task *api.TaskInfo, victims []*api.TaskInfo) []*api.TaskInfo {
		if _, configured, err := taskPolicy(task); !configured || err != nil {
			return nil
		}
		return f.order(victims, jobPriority)
	})
}

func (p *plugin) OnSessionClose(*framework.Session) {}

// fairness tracks the replicas each job has and how many of them are being evicted.
type fairness struct{ total, lost map[api.JobID]int }

func newFairness(ssn *framework.Session) *fairness {
	f := &fairness{total: map[api.JobID]int{}, lost: map[api.JobID]int{}}
	for id, job := range ssn.Jobs {
		f.total[id], f.lost[id] = len(job.Tasks), len(job.TaskStatusIndex[api.Releasing])
	}
	// Statement.Evict deallocates a Releasing task; only unevict re-allocates a Running one.
	ssn.AddEventHandler(&framework.EventHandler{
		DeallocateFunc: func(event *framework.Event) {
			if event.Task.Status == api.Releasing {
				f.lost[event.Task.Job]++
			}
		},
		AllocateFunc: func(event *framework.Event) {
			if event.Task.Status == api.Running {
				f.lost[event.Task.Job]--
			}
		},
	})
	return f
}

// loss is the fraction of the job's replicas gone after k more evictions.
func (f *fairness) loss(job api.JobID, k int) float64 {
	return float64(f.lost[job]+k) / float64(max(f.total[job], 1))
}

// order evicts lower-priority victims first and, within a priority, always takes the
// next victim from the job that would lose the smallest fraction of its replicas.
func (f *fairness) order(victims []*api.TaskInfo, priority func(*api.TaskInfo) int32) []*api.TaskInfo {
	seen := map[api.JobID]int{}
	keys := make(map[api.TaskID]float64, len(victims))
	for _, victim := range victims {
		seen[victim.Job]++
		keys[victim.UID] = f.loss(victim.Job, seen[victim.Job])
	}
	ordered := slices.Clone(victims)
	sort.SliceStable(ordered, func(i, j int) bool {
		if a, b := priority(ordered[i]), priority(ordered[j]); a != b {
			return a < b
		}
		return keys[ordered[i].UID] < keys[ordered[j].UID]
	})
	return ordered
}

func taskPolicy(task *api.TaskInfo) ([]string, bool, error) {
	value, configured := task.Pod.Annotations[SchedulingAlgorithmsAnnotation]
	if !configured {
		return nil, false, nil
	}
	var algorithms []string
	for item := range strings.SplitSeq(value, ",") {
		algorithm := strings.TrimSpace(item)
		if algorithm != rrAlgorithm && algorithm != binpackAlgorithm || slices.Contains(algorithms, algorithm) {
			return nil, true, fmt.Errorf("%s: unsupported or duplicate algorithm %q", SchedulingAlgorithmsAnnotation, algorithm)
		}
		algorithms = append(algorithms, algorithm)
	}
	return algorithms, true, nil
}

func nodeAlgorithm(node *api.NodeInfo, order []string) (int, string) {
	if node.Node == nil {
		return len(order), ""
	}
	supported := node.Node.Annotations[SchedulingAlgorithmsAnnotation]
	for i, algorithm := range order {
		for item := range strings.SplitSeq(supported, ",") {
			if strings.TrimSpace(item) == algorithm {
				return i, algorithm
			}
		}
	}
	return len(order), ""
}

func jobReplicas(node *api.NodeInfo, job api.JobID) int {
	count := 0
	for _, task := range node.Tasks {
		if task.Status != api.Releasing && task.Job == job {
			count++
		}
	}
	return count
}

// packScorer averages utilization for resources requested by the task.
func (p *plugin) packScorer(ssn *framework.Session, task *api.TaskInfo) func(*api.NodeInfo) float64 {
	var names []v1.ResourceName
	for _, name := range task.Resreq.ResourceNames() {
		if name != v1.ResourcePods && task.Resreq.Get(name) > 0 {
			names = append(names, name)
		}
	}
	dra := p.draUsage(ssn, task)
	return func(node *api.NodeInfo) float64 {
		score, count := 0.0, 0
		for _, name := range names {
			s, err := binpack.ResourceBinPackingScore(task.Resreq.Get(name), node.Allocatable.Get(name), node.Used.Get(name), 1)
			if err == nil && node.Allocatable.Get(name) > 0 {
				score += s
				count++
			}
		}
		if dra != nil {
			if u := dra(node); u.total > 0 {
				score += float64(u.used) / float64(u.total)
				count++
			}
		}
		if count == 0 {
			return 0
		}
		return score / float64(count)
	}
}

// draUsage counts, per node, the devices matching the task's DRA classes and how many are allocated.
func (p *plugin) draUsage(ssn *framework.Session, task *api.TaskInfo) func(*api.NodeInfo) usage {
	manager := ssn.SharedDRAManager()
	if len(task.DRAResreq) == 0 || manager == nil {
		return nil
	}
	resourceSlices, err := manager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		klog.V(4).Infof("workload-placement cannot list DRA ResourceSlices: %v", err)
		return nil
	}
	allocated, err := manager.ResourceClaims().GatherAllocatedState()
	if err != nil || allocated == nil {
		klog.V(4).Infof("workload-placement cannot read DRA allocations: %v", err)
		return nil
	}
	var classes []*resourcev1.DeviceClass
	for name := range task.DRAResreq {
		if class, err := manager.DeviceClasses().Get(name); err == nil {
			classes = append(classes, class)
		}
	}
	count := func(u *usage, slice *resourcev1.ResourceSlice, node *v1.Node) {
		for _, class := range classes {
			for _, d := range p.devices(class, slice) {
				if node != nil && !draDeviceOnNode(node, slice, d.device) {
					continue
				}
				u.total++
				if structured.IsDeviceAllocated(d.id, allocated) {
					u.used++
				}
			}
		}
	}
	// Node-local slices are indexed once; only selector-based slices are matched per node.
	byNode := map[string]usage{}
	var shared []*resourcev1.ResourceSlice
	for _, slice := range resourceSlices {
		// NodeName excludes the other node selection fields, so such a slice belongs to one node.
		name := ptr.Deref(slice.Spec.NodeName, "")
		if name == "" {
			shared = append(shared, slice)
			continue
		}
		u := byNode[name]
		count(&u, slice, nil)
		byNode[name] = u
	}
	return func(node *api.NodeInfo) usage {
		u := byNode[node.Name]
		if node.Node == nil {
			return u
		}
		for _, slice := range shared {
			count(&u, slice, node.Node)
		}
		return u
	}
}

func draDeviceOnNode(node *v1.Node, slice *resourcev1.ResourceSlice, device *resourcev1.Device) bool {
	if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
		ok, err := structured.NodeMatches(structured.Features{}, node, ptr.Deref(device.NodeName, ""),
			ptr.Deref(device.AllNodes, false), device.NodeSelector)
		return err == nil && ok
	}
	ok, err := structured.NodeMatches(structured.Features{}, node, ptr.Deref(slice.Spec.NodeName, ""),
		ptr.Deref(slice.Spec.AllNodes, false), slice.Spec.NodeSelector)
	return err == nil && ok
}

// devices returns the slice's devices matching the class; results are cached per session.
func (p *plugin) devices(class *resourcev1.DeviceClass, slice *resourcev1.ResourceSlice) []matchedDevice {
	key := classSlice{class.Name, slice}
	matched, ok := p.matched[key]
	if !ok {
		for i := range slice.Spec.Devices {
			if device := &slice.Spec.Devices[i]; p.celMatch(class, slice, device) {
				matched = append(matched, matchedDevice{device, structured.MakeDeviceID(slice.Spec.Driver, slice.Spec.Pool.Name, device.Name)})
			}
		}
		p.matched[key] = matched
	}
	return matched
}

func (p *plugin) celMatch(class *resourcev1.DeviceClass, slice *resourcev1.ResourceSlice, device *resourcev1.Device) bool {
	for _, selector := range class.Spec.Selectors {
		if selector.CEL == nil {
			return false
		}
		compiled := p.celCache.GetOrCompile(selector.CEL.Expression)
		if compiled.Error != nil {
			return false
		}
		match, _, err := compiled.DeviceMatches(context.Background(), cel.Device{
			Driver: slice.Spec.Driver, Attributes: device.Attributes, Capacity: device.Capacity,
		})
		if err != nil || !match {
			return false
		}
	}
	return true
}
