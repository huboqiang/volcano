package enhancepreemption

import (
	"fmt"
	"math"
)

func bfsNodePreemptionCore(preemptor [3]int64, preemptees [][3]int64, costs []float64, node [3]int64) ([]int, float64) {
	fmt.Println(preemptor, preemptees, costs, node)
	bfsQueue, isEnough := initBfsQueue(node, preemptor, preemptees, costs)
	if isEnough {
		return []int{}, 0
	}
	bestElem := queueElem{
		[]int{},
		math.MaxFloat64,
		[3]int64{},
	}
	for len(bfsQueue) > 0 {
		newQueue := make([]queueElem, 0)
		for _, elem := range bfsQueue {
			if preempteesReleaseEnoughResource(elem) {
				if elem.gxCost < bestElem.gxCost {
					bestElem = elem
				}
				continue
			}
			for idx := elem.idxPath[len(elem.idxPath)-1] + 1; idx < len(preemptees); idx++ {
				if elem.gxCost+costs[idx] > bestElem.gxCost {
					continue
				}

				idxPath := make([]int, len(elem.idxPath))
				copy(idxPath, elem.idxPath)
				newElem := queueElem{
					append(idxPath, idx),
					elem.gxCost + costs[idx],
					[3]int64{
						elem.hxNeed[0] - preemptees[idx][0],
						elem.hxNeed[1] - preemptees[idx][1],
						elem.hxNeed[2] - preemptees[idx][2],
					},
				}
				newQueue = append(newQueue, newElem)
			}
		}
		bfsQueue = newQueue
	}

	fmt.Println("Got:", bestElem)
	return bestElem.idxPath, bestElem.gxCost
}

func initBfsQueue(node [3]int64, preemptor [3]int64, preemptees [][3]int64, cost []float64) ([]queueElem, bool) {
	freeResource := [3]int64{node[0], node[1], node[2]}
	bfsQueue := make([]queueElem, len(preemptees))
	for _, p := range preemptees {
		for j := 0; j < 3; j++ {
			freeResource[j] -= p[j]
		}
	}

	if preemptor[0] < freeResource[0] && preemptor[1] < freeResource[1] && preemptor[2] < freeResource[2] {
		return bfsQueue, true
	}
	for i, p := range preemptees {
		elem := queueElem{
			idxPath: []int{i},
			gxCost:  cost[i],
			hxNeed:  [3]int64{preemptor[0] - freeResource[0] - p[0], preemptor[1] - freeResource[1] - p[1], preemptor[2] - freeResource[2] - p[2]},
		}
		bfsQueue[i] = elem
	}
	return bfsQueue, false
}
