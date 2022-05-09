package enhancepreemption

import (
	"math"
	"sort"
)

/*
	freeRes = node_resource - running_resource
	targetRes = preemptor - freeRes
	needRes = max(targetRes - sum(candidatesRes), 0)
	needRes eq [0,0,0]，break。
	needMoreRes = needRes_before - needRes_addPreemptee

	Select a group of candidates, satisfy the resource req with lowest cost.

	Using Astar algorithm for preemptees candidates search.

		f(x) = g(x) + h(x)

	g(x): g(x) is the total costs, can be priority, running time, resource used already...
    h(x): If we define heuristic function for how much resource we still need, it's hard to compare with cost
			because they have different unit (cost and cpu/mem/gpu), so a normalization for raw h(x) is required.

	So here h(x) is based on ROI ratio。We define:

		h(x) = Cost_addPreemptee / needMoreRes

	In this heuristic function, the lower cost, the more effect releasing, the better candidate preemptee it is.

*/
func astarNodePreemptionCore(preemptor [3]int64, preemptees [][3]int64, costs []float64, node [3]int64) ([]int, float64) {
	result := astarElem{
		current: -1,
		path:    []int{},
		gxCost:  math.MaxFloat64,
	}

	q, isEnough := initAstarQueue(node, preemptor, preemptees, costs, &result)
	if isEnough {
		return []int{}, 0
	}

	i := 0
	for len(q) > 0 {
		newQ := make([]astarElem, 0)
		for _, current := range q {
			newElem := addNewPreempteeToQueue(current, preemptees, preemptor, costs, &result)
			newQ = append(newQ, newElem...)
		}
		sortQueueByFx(newQ)
		q = newQ[0:intMin(len(newQ), maxQueueSize)]
		i++
	}
	return result.path, result.gxCost
}

func calculateHx(freeResource [3]int64, preemptorResource [3]int64, preempteeResource [3]int64, preempteeCost float64) float64 {
	hx := 0.
	for i := 0; i < 3; i++ {
		beforeNeedResource := preemptorResource[i] - freeResource[i]
		stillNeedResource := preemptorResource[i] - freeResource[i] - preempteeResource[i]
		resourceMore := math.Max(float64(beforeNeedResource), 0) - math.Max(float64(stillNeedResource), 0)
		if resourceMore <= 0 {
			continue
		}
		hx += preempteeCost / resourceMore
	}
	return hx
}

func addNewPreempteeToQueue(current astarElem, preemptees [][3]int64, preemptor [3]int64, costs []float64, result *astarElem) []astarElem {
	astarQueue := make([]astarElem, 0)
	for nextIdx := current.current + 1; nextIdx < len(preemptees); nextIdx++ {
		if current.gxCost+costs[nextIdx] >= result.gxCost || len(current.path) >= maxPreempteesSize {
			continue
		}
		currentFreeRes := [3]int64{
			current.freeRes[0] + preemptees[nextIdx][0],
			current.freeRes[1] + preemptees[nextIdx][1],
			current.freeRes[2] + preemptees[nextIdx][2],
		}
		currentPath := make([]int, len(current.path))
		copy(currentPath, current.path)
		elem := astarElem{
			current: nextIdx,
			path:    append(currentPath, nextIdx),
			freeRes: currentFreeRes,
			gxCost:  current.gxCost + costs[nextIdx],
			hxNeed:  calculateHx(currentFreeRes, preemptor, preemptees[nextIdx], costs[nextIdx]), //[3]int64{preemptor[0]-freeResource[0]-p[0], preemptor[1]-freeResource[1]-p[1], preemptor[2]-freeResource[2]-p[2]},
		}
		if isFreeResourceEnough(currentFreeRes, preemptor) {
			if elem.gxCost < result.gxCost {
				*result = astarElem{
					path:   elem.path,
					gxCost: elem.gxCost,
				}
			}
			continue
		}
		astarQueue = append(astarQueue, elem)
	}
	sortQueueByFx(astarQueue)
	return astarQueue[0:intMin(len(astarQueue), maxQueueSize)]
}

func isFreeResourceEnough(currentFreeRes [3]int64, preemptor [3]int64) bool {
	return currentFreeRes[0] >= preemptor[0] && currentFreeRes[1] >= preemptor[1] && currentFreeRes[2] >= preemptor[2]
}

func initAstarQueue(node [3]int64, preemptor [3]int64, preemptees [][3]int64, costs []float64, result *astarElem) ([]astarElem, bool) {
	freeResource := [3]int64{node[0], node[1], node[2]}
	astarQueue := make([]astarElem, len(preemptees))
	for _, p := range preemptees {
		for j := 0; j < 3; j++ {
			freeResource[j] -= p[j]
		}
	}
	if isFreeResourceEnough(freeResource, preemptor) {
		return astarQueue, true
	}

	initElem := astarElem{
		current: -1,
		freeRes: freeResource,
	}
	astarQueue = addNewPreempteeToQueue(initElem, preemptees, preemptor, costs, result)
	return astarQueue, false
}

// Here, the randomness for compare two float is important
//   if use v = f(x_i) - f(x_j), return v < 0,
//   it will be the stable sort, with it's raw order.
// And it's randomness will loss
func sortQueueByFx(lists []astarElem) {
	sort.Slice(lists, func(i, j int) bool {
		return lists[i].gxCost+lists[i].hxNeed < lists[j].gxCost+lists[j].hxNeed
	})
}

func arrayCompare(arr1 []int, arr2 []int) bool {
	length := intMin(len(arr1), len(arr2))
	for i := 0; i < length; i++ {
		if arr1[i] != arr2[i] {
			return arr1[i] < arr2[i]
		}
	}
	return len(arr1) < len(arr2)
}
