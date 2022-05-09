package enhancepreemption

import (
	v1 "k8s.io/api/core/v1"
)

const maxQueueSize int = 10
const maxPreempteesSize int = 32

func intMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

type preempteePodInfo struct {
	res  v1.ResourceList
	cost float64
}

type queueElem struct {
	idxPath []int
	gxCost  float64
	hxNeed  [3]int64
}

type astarElem struct {
	current int
	path    []int
	freeRes [3]int64
	gxCost  float64
	hxNeed  float64
}

func resourceToInt(res v1.ResourceList) [3]int64 {
	result := [3]int64{0, 0, 0}
	cpu, ok1 := res.Cpu().AsInt64()
	if ok1 {
		result[0] = cpu
	}
	mem, ok2 := res.Memory().AsInt64()
	if ok2 {
		result[1] = mem / 1000000000
	}

	v, ok3 := res["nvidia.com/gpu"]
	gpu := v.AsApproximateFloat64()
	if ok3 {
		result[2] = int64(gpu * 100)
	}
	return result
}

func preempteesReleaseEnoughResource(elem queueElem) bool {
	return elem.hxNeed[0] <= 0 && elem.hxNeed[1] <= 0 && elem.hxNeed[2] <= 0
}

func NodePreemptionSolver(preemptorRes v1.ResourceList, preempteesInfo []preempteePodInfo, node v1.ResourceList) ([]int, float64) {
	preempteeRes := make([][3]int64, 0)
	preempteeCost := make([]float64, 0)
	for _, res := range preempteesInfo {
		preempteeRes = append(preempteeRes, resourceToInt(res.res))
		preempteeCost = append(preempteeCost, res.cost)
	}
	//path, cost := bfsNodePreemptionCore(resourceToInt(preemptorRes), preempteeRes, preempteeCost, resourceToInt(node))
	path, cost := astarNodePreemptionCore(resourceToInt(preemptorRes), preempteeRes, preempteeCost, resourceToInt(node))

	return path, cost
}
