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

package rrbinpack

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
	"volcano.sh/volcano/pkg/scheduler/plugins/nodegroup"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	PluginName             = "rr-binpack"
	WorkloadClassLabel     = "volcano.sh/workload-class"
	ServiceIdentityArg     = "serviceIdentityLabel"
	defaultServiceIdentity = "app.kubernetes.io/name"
	rrZone                 = "rr"
	binpackZone            = "binpack"
	inferenceClass         = "inference"
	trainingClass          = "training"
)

type plugin struct {
	serviceIdentityLabel string
	nodeZoneLabel        string
	rrZoneValue          string
	binpackZoneValue     string
	workloadClassLabel   string
	inferenceValue       string
	trainingValue        string
}

func New(args framework.Arguments) framework.Plugin {
	p := &plugin{
		serviceIdentityLabel: defaultServiceIdentity,
		nodeZoneLabel:        nodegroup.NodeGroupNameKey,
		rrZoneValue:          rrZone, binpackZoneValue: binpackZone,
		workloadClassLabel: WorkloadClassLabel,
		inferenceValue:     inferenceClass, trainingValue: trainingClass,
	}
	args.GetString(&p.serviceIdentityLabel, ServiceIdentityArg)
	for key, value := range map[string]*string{
		"nodeZoneLabel": &p.nodeZoneLabel, "rrZoneValue": &p.rrZoneValue,
		"binpackZoneValue": &p.binpackZoneValue, "workloadClassLabel": &p.workloadClassLabel,
		"inferenceValue": &p.inferenceValue, "trainingValue": &p.trainingValue,
	} {
		args.GetString(value, key)
	}
	if p.serviceIdentityLabel == "" || p.nodeZoneLabel == "" || p.workloadClassLabel == "" ||
		p.rrZoneValue == "" || p.binpackZoneValue == "" || p.inferenceValue == "" || p.trainingValue == "" ||
		p.rrZoneValue == p.binpackZoneValue || p.inferenceValue == p.trainingValue {
		klog.Fatalf("invalid rr-binpack plugin arguments: labels and values must be nonempty and zone/class values distinct")
	}
	return p
}

func (p *plugin) Name() string { return PluginName }

func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddDRAPreemptionEligibleFn(p.Name(), func(task *api.TaskInfo) bool {
		return p.workloadClass(task) == p.trainingValue && len(task.DRAResreq) > 0
	})
	// Volcano's priority plugin preempts across jobs by job priority, so the
	// nomination guard compares the same value.
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
			if node == "" || p.workloadClass(task) != p.trainingValue {
				continue
			}
			if prio, ok := nominated[node]; !ok || job.Priority > prio {
				nominated[node] = job.Priority
			}
		}
	}
	reservedForTraining := func(inference *api.TaskInfo, node *api.NodeInfo) bool {
		prio := jobPriority(inference)
		if nominee, ok := nominated[node.Name]; ok && nominee > prio {
			return true
		}
		for _, task := range node.Tasks { // ssn.Pipeline adds pipelined tasks to the node
			if task.Status == api.Pipelined && p.workloadClass(task) == p.trainingValue && jobPriority(task) > prio {
				return true
			}
		}
		return false
	}

	ssn.AddPredicateFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		class := p.workloadClass(task)
		if class != p.inferenceValue && class != p.trainingValue {
			return nil
		}
		if class == p.inferenceValue && task.Pod.Labels[p.serviceIdentityLabel] == "" {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: fmt.Sprintf("inference task requires %s label", p.serviceIdentityLabel)})
		}
		if zone := p.nodeZone(node); zone != p.rrZoneValue && zone != p.binpackZoneValue {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: "node has no rr or binpack group"})
		}
		if class == p.trainingValue && p.hasTraining(node) && !task.InitResreq.LessEqual(node.Idle, api.Zero) {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: "running training workload cannot be preempted"})
		}
		if class == p.inferenceValue && reservedForTraining(task, node) {
			return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.UnschedulableAndUnresolvable,
				Reason: "node nominated for training"})
		}
		return nil
	})

	// BestNode receives only nodes that passed the normal predicates. It is also
	// used by normal preemption to try RR nodes before Binpack nodes.
	ssn.AddBestNodeFn(p.Name(), func(task *api.TaskInfo, scores map[float64][]*api.NodeInfo) *api.NodeInfo {
		class := p.workloadClass(task)
		if class != p.inferenceValue && class != p.trainingValue {
			return nil
		}
		var packScore func(*api.NodeInfo) float64 // built once per call, only if a Binpack node is seen
		var best *api.NodeInfo
		bestRank, bestCount, bestPack, bestScore := 5, 0, 0.0, 0.0
		for score, nodes := range scores {
			for _, node := range nodes {
				zone := p.nodeZone(node)
				if zone != p.rrZoneValue && zone != p.binpackZoneValue {
					continue
				}
				rank, count, pack := 0, 0, 0.0
				if class == p.inferenceValue {
					if zone == p.rrZoneValue {
						count = p.serviceReplicas(node, task.Namespace, task.Pod.Labels[p.serviceIdentityLabel])
					} else {
						rank = 1
						if packScore == nil {
							packScore = p.packScorer(ssn, task)
						}
						pack = packScore(node)
					}
				} else {
					fitsNow := task.InitResreq.LessEqual(node.Idle, api.Zero) && ssn.PredicateFn(task, node) == nil
					switch {
					case p.hasTraining(node):
						rank = 4
					case zone == p.binpackZoneValue && fitsNow:
						rank = 0
					case zone == p.rrZoneValue && fitsNow:
						rank = 1
					case zone == p.rrZoneValue:
						rank = 2
					default:
						rank = 3
					}
					count = len(node.Tasks)
				}
				if best == nil || rank < bestRank || rank == bestRank && (class == p.inferenceValue && zone == p.rrZoneValue &&
					(count < bestCount || count == bestCount && node.Name < best.Name) ||
					class == p.inferenceValue && zone == p.binpackZoneValue && (pack > bestPack || pack == bestPack &&
						(score > bestScore || score == bestScore && node.Name < best.Name)) ||
					class == p.trainingValue && (count < bestCount || count == bestCount &&
						(score > bestScore || score == bestScore && node.Name < best.Name))) {
					best, bestRank, bestCount, bestPack, bestScore = node, rank, count, pack, score
				}
			}
		}
		if best != nil {
			klog.V(4).Infof("rr-binpack selected node %s for %s task %s/%s", best.Name, class, task.Namespace, task.Name)
		}
		return best
	})

	filterVictims := func(preemptor *api.TaskInfo, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		if p.workloadClass(preemptor) != p.trainingValue {
			return nil, util.Abstain
		}
		victims := make([]*api.TaskInfo, 0, len(candidates))
		for _, candidate := range candidates {
			if p.workloadClass(candidate) == p.inferenceValue {
				victims = append(victims, candidate)
			}
		}
		return victims, util.Permit
	}
	ssn.AddPreemptableFn(p.Name(), filterVictims)
	ssn.AddReclaimableFn(p.Name(), filterVictims)
}

func (p *plugin) OnSessionClose(*framework.Session) {}

func (p *plugin) workloadClass(task *api.TaskInfo) string {
	return task.Pod.Labels[p.workloadClassLabel]
}

func (p *plugin) nodeZone(node *api.NodeInfo) string {
	if node.Node == nil {
		return ""
	}
	return node.Node.Labels[p.nodeZoneLabel]
}

func (p *plugin) serviceReplicas(node *api.NodeInfo, namespace, service string) int {
	count := 0
	for _, task := range node.Tasks {
		if task.Status != api.Releasing && task.Namespace == namespace &&
			p.workloadClass(task) == p.inferenceValue && task.Pod.Labels[p.serviceIdentityLabel] == service {
			count++
		}
	}
	return count
}

func (p *plugin) hasTraining(node *api.NodeInfo) bool {
	for _, task := range node.Tasks {
		if task.Status != api.Releasing && p.workloadClass(task) == p.trainingValue {
			return true
		}
	}
	return false
}

// packScorer returns a MostAllocated-style score for Binpack nodes that already
// passed the normal predicates. DRA tasks use the allocated fraction of devices
// in their DeviceClasses; other tasks use Volcano's binpack formula over their
// requested extended resources (e.g. nvidia.com/gpu).
func (p *plugin) packScorer(ssn *framework.Session, task *api.TaskInfo) func(*api.NodeInfo) float64 {
	if len(task.DRAResreq) == 0 {
		return func(node *api.NodeInfo) float64 {
			score := 0.0
			for name, request := range task.Resreq.ScalarResources {
				s, _ := binpack.ResourceBinPackingScore(request, node.Allocatable.Get(name), node.Used.Get(name), 1)
				score += s
			}
			return score
		}
	}
	none := func(*api.NodeInfo) float64 { return 0 }
	manager := ssn.SharedDRAManager()
	if manager == nil {
		return none
	}
	slices, err := manager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		klog.V(4).Infof("rr-binpack cannot list DRA ResourceSlices: %v", err)
		return none
	}
	allocated, err := manager.ResourceClaims().GatherAllocatedState()
	if err != nil || allocated == nil {
		klog.V(4).Infof("rr-binpack cannot read DRA allocations: %v", err)
		return none
	}
	var classes []*resourcev1.DeviceClass
	for className := range task.DRAResreq {
		if class, err := manager.DeviceClasses().Get(className); err == nil {
			classes = append(classes, class)
		}
	}
	cache := cel.NewCache(10, cel.Features{})
	return func(node *api.NodeInfo) float64 {
		if node.Node == nil {
			return 0
		}
		used, total := 0, 0
		for _, class := range classes {
			for _, slice := range slices {
				for _, device := range slice.Spec.Devices {
					if !draDeviceOnNode(node.Node, slice, &device) || !draDeviceMatchesClass(class, slice, &device, cache) {
						continue
					}
					total++
					if structured.IsDeviceAllocated(structured.MakeDeviceID(slice.Spec.Driver, slice.Spec.Pool.Name, device.Name), allocated) {
						used++
					}
				}
			}
		}
		if total == 0 {
			return 0
		}
		return float64(used) / float64(total)
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

func draDeviceMatchesClass(class *resourcev1.DeviceClass, slice *resourcev1.ResourceSlice, device *resourcev1.Device, cache *cel.Cache) bool {
	for _, selector := range class.Spec.Selectors {
		if selector.CEL == nil {
			return false
		}
		compiled := cache.GetOrCompile(selector.CEL.Expression)
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
