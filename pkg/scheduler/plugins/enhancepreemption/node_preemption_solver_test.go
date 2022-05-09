package enhancepreemption

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"math"
	"reflect"
	"testing"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func makeRepeatPods(n int, cost float64, cpu string, mem string, gpu string) []preempteePodInfo {
	pod := preempteePodInfo{
		util.BuildResourceListWithGPU(cpu, mem, gpu),
		cost,
	}
	res := make([]preempteePodInfo, n)
	for i := 0; i < n; i++ {
		res[i] = pod
	}
	return res
}

func Test_NodePreemptionSolver(t *testing.T) {
	type args struct {
		preemptorRes   v1.ResourceList
		preempteesInfo []preempteePodInfo
		node           v1.ResourceList
	}
	tests := []struct {
		name  string
		args  args
		want  []int
		want1 float64
	}{
		{
			"case1",
			args{
				util.BuildResourceListWithGPU("5", "10G", "0"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						4,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						6,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						9,
					},
					{
						util.BuildResourceListWithGPU("6", "12G", "0"),
						17,
					},
				},
				util.BuildResourceListWithGPU("12", "24G", "0"),
			},
			[]int{0, 2, 3},
			16,
		},

		{
			"case2",
			args{
				util.BuildResourceListWithGPU("5", "10G", "0"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						4,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						6,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						9,
					},
					{
						util.BuildResourceListWithGPU("6", "12G", "0"),
						15,
					},
				},
				util.BuildResourceListWithGPU("12", "24G", "0"),
			},
			[]int{4},
			15,
		},

		{
			"case3, a little more plus free is enough",
			args{
				util.BuildResourceListWithGPU("5", "10G", "0"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						4,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						6,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						9,
					},
					{
						util.BuildResourceListWithGPU("6", "12G", "0"),
						16,
					},
				},
				util.BuildResourceListWithGPU("15", "30G", "0"),
			},
			[]int{0, 1},
			5,
		},

		{
			"case4, free, no need to preempt",
			args{
				util.BuildResourceListWithGPU("5", "10G", "0"),
				[]preempteePodInfo{},
				util.BuildResourceListWithGPU("16", "32G", "1"),
			},
			[]int{},
			0,
		},
		{
			"case5, bigger than node",
			args{
				util.BuildResourceListWithGPU("15", "30G", "0"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						2,
					},
				},
				util.BuildResourceListWithGPU("4", "8G", "0"),
			},
			[]int{},
			math.MaxFloat64,
		},
		{
			"case6.1, node do not have GPU",
			args{
				util.BuildResourceListWithGPU("1", "2G", "1"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("2", "4G", "0"),
						2,
					},
				},
				util.BuildResourceListWithGPU("4", "8G", "0"),
			},
			[]int{},
			math.MaxFloat64,
		},
		{
			"case6.2, GPU preemption",
			args{
				util.BuildResourceListWithGPU("1", "7G", "0.8"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "1G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("2", "1G", "0.5"),
						20,
					},
				},
				util.BuildResourceListWithGPU("4", "8G", "1"),
			},
			[]int{1},
			20,
		},
		{
			"case6.3, CPU preemption",
			args{
				util.BuildResourceListWithGPU("1", "7G", "0.5"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "1G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("2", "1G", "0.5"),
						20,
					},
				},
				util.BuildResourceListWithGPU("4", "8G", "1"),
			},
			[]int{0},
			1,
		},
		{
			"case6.4, GPU+CPU preemption",
			args{
				util.BuildResourceListWithGPU("1", "7G", "0.8"),
				[]preempteePodInfo{
					{
						util.BuildResourceListWithGPU("1", "2G", "0"),
						1,
					},
					{
						util.BuildResourceListWithGPU("2", "1G", "0.5"),
						20,
					},
				},
				util.BuildResourceListWithGPU("4", "8G", "1"),
			},
			[]int{0, 1},
			21,
		},
		{
			"case6.5, preempt all",
			args{
				util.BuildResourceListWithGPU("4", "8G", "4"),
				append(makeRepeatPods(4, 10, "1", "2G", "1")),
				util.BuildResourceListWithGPU("4", "8G", "4"),
			},
			[]int{0, 1, 2, 3},
			40,
		},
		{
			"case7.1, large case, still free resource",
			args{
				util.BuildResourceListWithGPU("512", "1024G", "10"),
				append(makeRepeatPods(10, 10, "1", "2G", "1"), makeRepeatPods(10, 1, "1", "2G", "0")...),
				util.BuildResourceListWithGPU("1024", "2048G", "1024"),
			},
			[]int{},
			0,
		},
		{
			"case7.2, large Case. Too slow For BFS for higher preemptor requirment. ",
			args{
				util.BuildResourceListWithGPU("3", "4G", "5"),
				append(makeRepeatPods(512, 1, "1", "2G", "1"), makeRepeatPods(512, 10, "1", "2G", "0")...),
				util.BuildResourceListWithGPU("1024", "2048G", "1024"),
			},
			[]int{509, 510, 511},
			3,
		},
		{
			fmt.Sprintf("case7.3, large Case. Can preempt max %d pods, so cannot preempt.", maxPreempteesSize),
			args{
				util.BuildResourceListWithGPU("1024", "2048G", "1024"),
				append(makeRepeatPods(512, 10, "1", "2G", "1"), makeRepeatPods(512, 1, "1", "2G", "0")...),
				util.BuildResourceListWithGPU("1024", "2048G", "1024"),
			},
			[]int{},
			math.MaxFloat64,
		},
		{
			"case7.4, large Case. Too slow For BFS for higher preemptor requirment.",
			args{
				util.BuildResourceListWithGPU("3", "4G", "5"),
				append(makeRepeatPods(512, 10, "1", "2G", "1"), makeRepeatPods(512, 1, "1", "2G", "0")...),
				util.BuildResourceListWithGPU("1024", "2048G", "1024"),
			},
			[]int{512, 513, 514},
			3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := NodePreemptionSolver(tt.args.preemptorRes, tt.args.preempteesInfo, tt.args.node)
			if len(tt.name) > 7 && tt.name[0:8] == "case7.4," {
				if arrayCompare(got, tt.want) {
					t.Errorf("NodePreemptionSolver() got = %v, but right answer min(arr) is %v", got, tt.want)
				}
			} else if len(tt.name) > 7 && tt.name[0:8] == "case7.2," {
				if !arrayCompare(got, tt.want) {
					t.Errorf("NodePreemptionSolver() got = %v, but right answer max(arr) is %v", got, tt.want)
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NodePreemptionSolver() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("NodePreemptionSolver() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
