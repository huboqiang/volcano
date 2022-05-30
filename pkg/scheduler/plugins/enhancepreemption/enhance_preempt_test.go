/*
Copyright 2018 The Kubernetes Authors.

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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	preempt2 "volcano.sh/volcano/pkg/scheduler/actions/preempt"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func buildPodWithRunningTime(namespace, name, nodeName string, p v1.PodPhase, req v1.ResourceList, groupName string,
	labels map[string]string, selector map[string]string, podScheduleTime time.Time) *v1.Pod {
	pod := util.BuildPod(namespace, name, nodeName, p, req, groupName, labels, selector)
	pod.Status.Conditions = []v1.PodCondition{
		{
			Type:               v1.PodScheduled,
			LastTransitionTime: metav1.NewTime(podScheduleTime),
			Status:             v1.ConditionTrue,
		},
	}
	return pod
}

func TestPreempt(t *testing.T) {
	framework.RegisterPluginBuilder("conformance", conformance.New)
	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder(priority.PluginName, priority.New)
	framework.RegisterPluginBuilder(PluginName, New)

	options.ServerOpts = &options.ServerOption{
		MinNodesToFind:             100,
		MinPercentageOfNodesToFind: 5,
		PercentageOfNodesToFind:    100,
	}
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*schedulingv1beta1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*schedulingv1beta1.Queue
		expected  int
	}{
		{
			name: "preempt task with lowest priority in different nodes",
			podGroups: []*schedulingv1beta1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "low-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "mid-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg3",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg4",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
			},
			// There are 3 cpus and 3G of memory idle and 3 tasks running each consuming 1 cpu and 1G of memory.
			// Big task requiring 5 cpus and 5G of memory should preempt 2 of 3 running tasks to fit into the node.
			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1-1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg3", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-1", "n2", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3-1", "n3", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee1-2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg3", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-2", "n2", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3-2", "n3", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),

				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("6", "6G"), "pg4", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("6", "6G"), make(map[string]string)),
				util.BuildNode("n2", util.BuildResourceList("6", "6G"), make(map[string]string)),
				util.BuildNode("n3", util.BuildResourceList("6", "6G"), make(map[string]string)),
			},
			queues: []*schedulingv1beta1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: schedulingv1beta1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 2,
		},
		{
			name: "preempt task with shortest running time in different nodes",
			podGroups: []*schedulingv1beta1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "low-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "mid-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg3",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         0,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg4",
						Namespace: "c1",
					},
					Spec: schedulingv1beta1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
					Status: schedulingv1beta1.PodGroupStatus{
						Phase: schedulingv1beta1.PodGroupInqueue,
					},
				},
			},
			// There are 3 cpus and 3G of memory idle and 3 tasks running each consuming 1 cpu and 1G of memory.
			// Big task requiring 5 cpus and 5G of memory should preempt 2 of 3 running tasks to fit into the node.
			pods: []*v1.Pod{
				buildPodWithRunningTime("c1", "preemptee1-1", "n1", v1.PodRunning,
					util.BuildResourceList("1", "1G"), "pg2",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string), time.Now().Add(time.Second*-800),
				),

				buildPodWithRunningTime("c1", "preemptee2-1", "n2", v1.PodRunning,
					util.BuildResourceList("1", "1G"), "pg2",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string), time.Now().Add(time.Second*-80),
				),

				buildPodWithRunningTime("c1", "preemptee3-1", "n3", v1.PodRunning,
					util.BuildResourceList("1", "1G"), "pg2",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string), time.Now().Add(time.Second*-8),
				),

				util.BuildPod("c1", "preemptor1", "", v1.PodPending,
					util.BuildResourceList("6", "6G"), "pg4", make(map[string]string), make(map[string]string),
				),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("6", "6G"), make(map[string]string)),
				util.BuildNode("n2", util.BuildResourceList("6", "6G"), make(map[string]string)),
				util.BuildNode("n3", util.BuildResourceList("6", "6G"), make(map[string]string)),
			},
			queues: []*schedulingv1beta1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: schedulingv1beta1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 1,
		},
	}

	preempt := preempt2.New()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binder := &util.FakeBinder{
				Binds:   map[string]string{},
				Channel: make(chan string),
			}
			evictor := &util.FakeEvictor{
				Channel: make(chan string),
			}
			schedulerCache := &cache.SchedulerCache{
				Nodes:           make(map[string]*api.NodeInfo),
				Jobs:            make(map[api.JobID]*api.JobInfo),
				Queues:          make(map[api.QueueID]*api.QueueInfo),
				Binder:          binder,
				Evictor:         evictor,
				StatusUpdater:   &util.FakeStatusUpdater{},
				VolumeBinder:    &util.FakeVolumeBinder{},
				PriorityClasses: make(map[string]*schedulingv1.PriorityClass),

				Recorder: record.NewFakeRecorder(100),
			}
			schedulerCache.PriorityClasses["high-priority"] = &schedulingv1.PriorityClass{
				Value: 100000,
			}
			schedulerCache.PriorityClasses["mid-priority"] = &schedulingv1.PriorityClass{
				Value: 1000,
			}
			schedulerCache.PriorityClasses["low-priority"] = &schedulingv1.PriorityClass{
				Value: 10,
			}
			for _, node := range test.nodes {
				schedulerCache.AddNode(node)
			}
			for _, pod := range test.pods {
				schedulerCache.AddPod(pod)
			}

			for _, ss := range test.podGroups {
				schedulerCache.AddPodGroupV1beta1(ss)
			}

			for _, q := range test.queues {
				schedulerCache.AddQueueV1beta1(q)
			}

			trueValue := true
			ssn := framework.OpenSession(schedulerCache, []conf.Tier{
				{
					Plugins: []conf.PluginOption{
						{
							Name:               "conformance",
							EnabledPreemptable: &trueValue,
						},
						{
							Name:               PluginName,
							EnabledNodeOrder:   &trueValue,
							EnabledPreemptable: &trueValue,
							EnabledJobOrder:    &trueValue,
							EnabledTaskOrder:   &trueValue,
						},
						{
							Name:                "gang",
							EnabledPreemptable:  &trueValue,
							EnabledJobPipelined: &trueValue,
							EnabledJobStarving:  &trueValue,
						},
					},
				},
			}, nil)
			defer framework.CloseSession(ssn)

			preempt.Execute(ssn)

			for i := 0; i < test.expected; i++ {
				select {
				case <-evictor.Channel:
				case <-time.After(time.Second):
					t.Errorf("not enough evictions")
				}
			}
			select {
			case key, opened := <-evictor.Channel:
				if opened {
					t.Errorf("unexpected eviction: %s", key)
				}
			case <-time.After(50 * time.Millisecond):
				// TODO: Active waiting here is not optimal, but there is no better way currently.
				//	 Ideally we would like to wait for evict and bind request goroutines to finish first.
			}
		})
	}
}
