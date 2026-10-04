package workloadplacement

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// Every prefix of the fair order must reach the smallest possible worst-case job loss,
// checked against brute force over all victim subsets of that size.
func TestFairnessOrderIsOptimal(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 300; round++ {
		f := &fairness{total: map[api.JobID]int{}, lost: map[api.JobID]int{}}
		var victims []*api.TaskInfo
		for j := 0; j < 1+rng.Intn(4); j++ {
			job := api.JobID(fmt.Sprintf("j%d", j))
			running := 1 + rng.Intn(4)
			f.total[job], f.lost[job] = running+rng.Intn(5), rng.Intn(3)
			for r := 0; r < running; r++ {
				victims = append(victims, &api.TaskInfo{UID: api.TaskID(fmt.Sprintf("%s-%d", job, r)), Job: job})
			}
		}
		rng.Shuffle(len(victims), func(i, j int) { victims[i], victims[j] = victims[j], victims[i] })
		ordered := f.order(victims, func(*api.TaskInfo) int32 { return 0 })
		worst := func(set []*api.TaskInfo) float64 {
			k, w := map[api.JobID]int{}, 0.0
			for _, v := range set {
				k[v.Job]++
				w = max(w, f.loss(v.Job, k[v.Job]))
			}
			return w
		}
		for size := 1; size <= len(victims); size++ {
			best := math.Inf(1)
			for mask := 0; mask < 1<<len(victims); mask++ {
				var set []*api.TaskInfo
				for i, v := range victims {
					if mask&(1<<i) != 0 {
						set = append(set, v)
					}
				}
				if len(set) == size {
					best = min(best, worst(set))
				}
			}
			if got := worst(ordered[:size]); got != best {
				t.Fatalf("round %d: %d victims lose %v, optimum %v", round, size, got, best)
			}
		}
	}
}

func TestFairnessOrderKeepsPriority(t *testing.T) {
	f := &fairness{total: map[api.JobID]int{"low": 1, "high": 100}, lost: map[api.JobID]int{}}
	victims := []*api.TaskInfo{{UID: "h", Job: "high"}, {UID: "l", Job: "low"}}
	prio := map[api.JobID]int32{"low": 1, "high": 2}
	if got := f.order(victims, func(t *api.TaskInfo) int32 { return prio[t.Job] }); got[0].UID != "l" {
		t.Fatalf("higher-priority victim %s evicted first", got[0].UID)
	}
}
