/*
Copyright 2022 The Volcano Authors.
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

package enhancepreemption

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
	"math"
	"time"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName is name of plugin
	PluginName = "enhancepreempt"
	taskOrder  = "task-order"
)

type enhancePreemptionPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return priority plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &enhancePreemptionPlugin{pluginArguments: arguments}
}

func (pp *enhancePreemptionPlugin) Name() string {
	return PluginName
}

func getTaskRes(task *api.TaskInfo) v1.ResourceList {
	sMem := fmt.Sprintf("%d", int(task.Resreq.Memory))
	sCpu := fmt.Sprintf("%dm", int(task.Resreq.MilliCPU))
	cpu := resource.MustParse(sCpu)
	mem := resource.MustParse(sMem)
	gpu := 0.
	if gpuReq, ok := task.Resreq.ScalarResources[api.GPUResourceName]; ok {
		gpu = gpuReq
	}
	return v1.ResourceList{
		v1.ResourceCPU:      cpu,
		v1.ResourceMemory:   mem,
		api.GPUResourceName: resource.MustParse(fmt.Sprintf("%f", gpu)),
	}

}

type costWeight struct {
	jobPriorityWeight  float64
	taskPriorityWeight float64
	runningTimeWeight  float64
	resUsedWeight      []float64
}

func getTaskCost(ssn *framework.Session, task *api.TaskInfo) float64 {
	w := costWeight{
		1,
		1,
		1,
		[]float64{2, 1, 10},
	}
	cost := float64(task.Priority) * w.taskPriorityWeight
	jobInfo, ok := ssn.Jobs[task.Job]
	if ok {
		cost += float64(jobInfo.Priority) * w.jobPriorityWeight
	}

	if task.Pod.Status.Phase == v1.PodRunning {
		for _, c := range task.Pod.Status.Conditions {
			if c.Type == v1.PodScheduled && c.Status == v1.ConditionTrue {
				dt := time.Now().Sub(c.LastTransitionTime.Time).Seconds()
				res := resourceToInt(getTaskRes(task))
				cost += dt * w.runningTimeWeight
				for i := 0; i < 3; i++ {
					cost += dt * float64(res[i]) * w.resUsedWeight[i]
				}
			}
		}
	}

	return math.MaxFloat64 - cost
}

type nodeInfo struct {
	taskOrder []int
	nodeCost  float64
}

func (pp *enhancePreemptionPlugin) OnSessionOpen(ssn *framework.Session) {

	nodeOrderFn := func(preemptor *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		preempteeInfo := make([]preempteePodInfo, 0)
		for taskId, preemptee := range node.Tasks {
			podInfo := preempteePodInfo{
				taskId,
				getTaskRes(preemptee),
				getTaskCost(ssn, preemptee),
			}
			preempteeInfo = append(preempteeInfo, podInfo)
		}
		order, cost := NodePreemptionSolver(getTaskRes(preemptor), preempteeInfo, node.Node.Status.Allocatable)
		for orderVal, idx := range order {
			taskId := preempteeInfo[idx].taskId
			task, ok := node.Tasks[taskId]
			if task != nil && ok {
				if task.Pod.ObjectMeta.Annotations == nil {
					task.Pod.ObjectMeta.Annotations = make(map[string]string, 0)
				}
				task.Pod.ObjectMeta.Annotations[taskOrder] = fmt.Sprintf("%04d", orderVal)
			}
		}
		return cost, nil
	}

	// Add Task Order function
	ssn.AddNodeOrderFn(pp.Name(), nodeOrderFn)

	taskOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.TaskInfo)
		rv := r.(*api.TaskInfo)
		rankL, okL := lv.Pod.ObjectMeta.Annotations[taskOrder]
		if !okL {
			rankL = "9999"
		}
		rankR, okR := rv.Pod.ObjectMeta.Annotations[taskOrder]
		if !okR {
			rankR = "9999"
		}
		klog.V(4).Infof("Priority TaskOrder: <%v/%v> orderRank is %v, <%v/%v> orderRank is %v",
			lv.Namespace, lv.Name, rankL, rv.Namespace, rv.Name, rankR)

		if rankL == rankR {
			return 0
		}

		if rankL > rankR {
			return -1
		}

		return 1
	}

	// Add Task Order function
	ssn.AddTaskOrderFn(pp.Name(), taskOrderFn)

	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)
		klog.V(4).Infof("Priority JobOrderFn: <%v/%v> priority: %d, <%v/%v> priority: %d",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority > rv.Priority {
			return -1
		}

		if lv.Priority < rv.Priority {
			return 1
		}

		return 0
	}

	ssn.AddJobOrderFn(pp.Name(), jobOrderFn)

	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		preemptorJob := ssn.Jobs[preemptor.Job]

		var victims []*api.TaskInfo
		for _, preemptee := range preemptees {
			preempteeJob := ssn.Jobs[preemptee.Job]
			if preempteeJob.UID != preemptorJob.UID {
				if preempteeJob.Priority >= preemptorJob.Priority { // Preemption between Jobs within Queue
					klog.V(4).Infof("Can not preempt task <%v/%v>"+
						"because preemptee job has greater or equal job priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preempteeJob.Priority, preemptorJob.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			} else { // same job's different tasks should compare task's priority
				if preemptee.Priority >= preemptor.Priority {
					klog.V(4).Infof("Can not preempt task <%v/%v>"+
						"because preemptee task has greater or equal task priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preemptee.Priority, preemptor.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			}
		}

		klog.V(4).Infof("Victims from Priority plugins are %+v", victims)
		return victims, util.Permit
	}
	ssn.AddPreemptableFn(pp.Name(), preemptableFn)

	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		return ji.ReadyTaskNum()+ji.WaitingTaskNum() < int32(len(ji.Tasks))
	}
	ssn.AddJobStarvingFns(pp.Name(), jobStarvingFn)

	ssn.AddJobEnqueueableFn(pp.Name(), func(obj interface{}) int {
		return util.Permit
	})

}

func (pp *enhancePreemptionPlugin) OnSessionClose(ssn *framework.Session) {}
