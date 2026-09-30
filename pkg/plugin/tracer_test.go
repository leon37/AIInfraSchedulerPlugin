package plugin

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func rl(pairs ...string) v1.ResourceList {
	list := make(v1.ResourceList)
	for i := 0; i+1 < len(pairs); i += 2 {
		list[v1.ResourceName(pairs[i])] = resource.MustParse(pairs[i+1])
	}
	return list
}

func TestMaxPodsFit(t *testing.T) {
	cases := []struct {
		name      string
		available v1.ResourceList
		perPod    v1.ResourceList
		want      int
	}{
		{"只看 CPU：16 核放 8 核的 Pod", rl("cpu", "16"), rl("cpu", "8"), 2},
		{"CPU 毫核整除向下取整", rl("cpu", "1500m"), rl("cpu", "500m"), 3},
		{"只申请 GPU：2 张卡放 1 卡的 Pod", rl("cpu", "16", "example.com/gpu", "2"), rl("example.com/gpu", "1"), 2},
		{"多资源取最小：CPU 够 4 个，GPU 只够 1 个", rl("cpu", "16", "example.com/gpu", "1"), rl("cpu", "4", "example.com/gpu", "1"), 1},
		{"节点上没有这种资源", rl("cpu", "16"), rl("example.com/gpu", "1"), 0},
		{"剩余为负（已超额）", rl("cpu", "-2"), rl("cpu", "1"), 0},
		{"内存按字节整除", rl("memory", "3Gi"), rl("memory", "1Gi"), 3},
		{"申请量为 0 的资源不参与", rl("cpu", "16", "memory", "0"), rl("cpu", "8", "memory", "0"), 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := maxPodsFit(c.available, c.perPod); got != c.want {
				t.Fatalf("maxPodsFit(%v, %v) = %d, want %d", c.available, c.perPod, got, c.want)
			}
		})
	}
}

func TestPodRequestsSumsContainers(t *testing.T) {
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{
		{Resources: v1.ResourceRequirements{Requests: rl("cpu", "1", "example.com/gpu", "1")}},
		{Resources: v1.ResourceRequirements{Requests: rl("cpu", "500m")}},
	}}}
	got := podRequests(pod)
	if cpu := got[v1.ResourceCPU]; cpu.MilliValue() != 1500 {
		t.Fatalf("cpu = %v, want 1500m", cpu.String())
	}
	if gpu := got["example.com/gpu"]; gpu.Value() != 1 {
		t.Fatalf("gpu = %v, want 1", gpu.String())
	}
}

func TestAddResourceListWritesBack(t *testing.T) {
	dst := rl("cpu", "2")
	addResourceList(dst, rl("cpu", "3", "example.com/gpu", "1"))
	if cpu := dst[v1.ResourceCPU]; cpu.MilliValue() != 5000 {
		t.Fatalf("cpu = %v, want 5", cpu.String())
	}
	if gpu := dst["example.com/gpu"]; gpu.Value() != 1 {
		t.Fatalf("gpu = %v, want 1", gpu.String())
	}
}
