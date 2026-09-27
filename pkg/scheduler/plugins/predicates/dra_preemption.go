package predicates

import (
	"context"
	"fmt"
	"strings"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/utils/ptr"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func hasReleasingDRAClaim(node *api.NodeInfo) bool {
	for _, task := range node.Tasks {
		if task.Status == api.Releasing && len(task.ResourceClaimKeys) > 0 {
			return true
		}
	}
	return false
}

// draFitsAfterEviction simulates only the release of claims owned by tentative
// victims. It never changes the shared DRA tracker or an API ResourceClaim.
func (pp *PredicatesPlugin) draFitsAfterEviction(ssn *framework.Session, task *api.TaskInfo, node *api.NodeInfo) error {
	fail := func(reason string) error {
		return api.NewFitErrWithStatus(task, node, &api.Status{Code: api.Unschedulable, Reason: reason})
	}
	manager := ssn.SharedDRAManager()
	if manager == nil || node.Node == nil {
		return fail("DRA manager or node unavailable")
	}
	state, err := manager.ResourceClaims().GatherAllocatedState()
	if err != nil || state == nil {
		return fail(fmt.Sprintf("cannot read DRA allocations: %v", err))
	}
	snapshot := *state
	snapshot.AllocatedDevices = state.AllocatedDevices.Clone()
	snapshot.AllocatedSharedDeviceIDs = state.AllocatedSharedDeviceIDs.Clone()
	snapshot.AggregatedCapacity = state.AggregatedCapacity.Clone()

	for _, victim := range node.Tasks {
		if victim.Status != api.Releasing {
			continue
		}
		if len(victim.ResourceClaimKeys) == 0 {
			return fail(fmt.Sprintf("victim %s has no DRA claim", victim.Name))
		}
		for _, key := range victim.ResourceClaimKeys {
			claim, err := draClaimByKey(ssn, key)
			if err != nil || claim.Status.Allocation == nil || !exclusiveExactlyCountClaim(claim) ||
				len(claim.Status.ReservedFor) != 1 || string(claim.Status.ReservedFor[0].UID) != string(victim.UID) ||
				!exactAllocationResults(claim) {
				return fail(fmt.Sprintf("victim claim %s is missing, shared or unsupported", key))
			}
			for _, device := range claim.Status.Allocation.Devices.Results {
				id := structured.MakeDeviceID(device.Driver, device.Pool, device.Device)
				if !snapshot.AllocatedDevices.Has(id) {
					return fail(fmt.Sprintf("victim device for claim %s is not allocated", key))
				}
				snapshot.AllocatedDevices.Delete(id)
			}
		}
	}

	claims := make([]*resourcev1.ResourceClaim, 0, len(task.ResourceClaimKeys))
	for _, key := range task.ResourceClaimKeys {
		claim, err := draClaimByKey(ssn, key)
		if err != nil || !exclusiveExactlyCountClaim(claim) || claim.Status.Allocation != nil {
			return fail(fmt.Sprintf("training claim %s is missing or unsupported", key))
		}
		claims = append(claims, claim)
	}
	if len(claims) == 0 {
		return fail("training task has no DRA claims")
	}
	slices, err := manager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		return fail(fmt.Sprintf("cannot read DRA ResourceSlices: %v", err))
	}
	allocator, err := structured.NewAllocator(context.Background(), dynamicresources.AllocatorFeatures(pp.features),
		snapshot, manager.DeviceClasses(), slices, cel.NewCache(10, cel.Features{
			EnableConsumableCapacity: pp.features.EnableDRAConsumableCapacity,
		}))
	if err != nil {
		return fail(fmt.Sprintf("cannot simulate DRA allocation: %v", err))
	}
	results, err := allocator.Allocate(context.Background(), node.Node, claims)
	if err != nil || len(results) != len(claims) {
		return fail(fmt.Sprintf("DRA claims do not fit after eviction: %v", err))
	}
	return nil
}

func draClaimByKey(ssn *framework.Session, key string) (*resourcev1.ResourceClaim, error) {
	namespace, name, ok := strings.Cut(key, "/")
	if !ok || namespace == "" || name == "" {
		return nil, fmt.Errorf("invalid ResourceClaim key %q", key)
	}
	return ssn.SharedDRAManager().ResourceClaims().Get(namespace, name)
}

func exclusiveExactlyCountClaim(claim *resourcev1.ResourceClaim) bool {
	if claim == nil || len(claim.Spec.Devices.Requests) == 0 {
		return false
	}
	for _, request := range claim.Spec.Devices.Requests {
		if request.Exactly == nil || request.Exactly.AllocationMode != "" &&
			request.Exactly.AllocationMode != resourcev1.DeviceAllocationModeExactCount ||
			request.Exactly.Count < 0 || request.Exactly.Capacity != nil ||
			ptr.Deref(request.Exactly.AdminAccess, false) || request.Exactly.DeviceClassName == "" {
			return false
		}
	}
	return true
}

func exactAllocationResults(claim *resourcev1.ResourceClaim) bool {
	counts := make(map[string]int64, len(claim.Spec.Devices.Requests))
	for _, request := range claim.Spec.Devices.Requests {
		count := request.Exactly.Count
		if count == 0 {
			count = 1
		}
		counts[request.Name] = count
	}
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.ShareID != nil || result.ConsumedCapacity != nil || ptr.Deref(result.AdminAccess, false) ||
			result.Driver == "" || result.Pool == "" || result.Device == "" || counts[result.Request] == 0 {
			return false
		}
		counts[result.Request]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
