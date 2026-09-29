package plugin

import (
	"context"
	"sort"
	"time"

	"github.com/leon37/AIInfraSchedulerPlugin/pkg/apis"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	v3 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	v2 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const TracerPluginName = "tracer-plugin"
const preemptionPlanState = "preemption-plan"

var podGroupGVR = schema.GroupVersionResource{
	Group:    "batch.example.com",
	Version:  "v1",
	Resource: "podgroups", // 小写复数
}

// 给你实现 happy-path 要用的 API 线索(框架的,我给;逻辑你写)
//
// - 工厂 New(ctx, obj, h) 里那个 h framework.Handle 要存到你的插件结构体上——它是你够到框架内部的把手。
// - 释放被扣的同伴:h.IterateOverWaitingPods(func(wp framework.WaitingPod) { ... }) 遍历所有 Wait 中的 Pod;wp.GetPod() 拿到 Pod(读它的 gang label 过滤);wp.Allow(Name) 放行某个被扣的 Pod。
// - 读标识:pod.Labels[...] / pod.Annotations[...]。
//
// 任务:实现 Permit 的 happy path(只管"来齐了"这条)
//
// 读出当前 Pod 的 gang-id 和 minMember → 数这个 gang 当前到了几个(含自己)→
// - < minMember → 返回 Wait(超时)(扣住);
// - == minMember → 用 IterateOverWaitingPods 把这个 gang 里被扣的同伴逐个 Allow,然后对自己返回 Allow(nil, 0)。
type TracerPlugin struct {
	handle    framework.Handle
	podLister v2.PodLister
	dyn       *dynamic.DynamicClient
}

func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	dyn, err := dynamic.NewForConfig(h.KubeConfig())
	if err != nil {
		return nil, err
	}
	return &TracerPlugin{
		handle:    h,
		podLister: h.SharedInformerFactory().Core().V1().Pods().Lister(),
		dyn:       dyn,
	}, nil
}

func (t *TracerPlugin) Name() string {
	return TracerPluginName
}

func (t *TracerPlugin) Reserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) *framework.Status {
	klog.InfoS("tracer-plugin Reserve", "pod", p.Name, "node", nodeName)
	return nil
}

func (t *TracerPlugin) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {
	klog.InfoS("tracer-plugin Unreserve", "pod", p.Name, "node", nodeName)
	t.handle.IterateOverWaitingPods(func(waiting framework.WaitingPod) {
		pod := waiting.GetPod()
		if !isPodSamePodGroup(p, pod) {
			return
		}
		waiting.Reject(t.Name(), "pod unreserved")
	})
}

func (t *TracerPlugin) Permit(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	klog.InfoS("tracer-plugin Permit", "pod", p.Name, "node", nodeName)
	podGroupName, ok := p.Labels["pod-group"]
	if !ok {
		return framework.NewStatus(framework.Success), 0
	}

	obj, err := t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).Get(ctx, podGroupName, v3.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return framework.NewStatus(framework.Unschedulable, err.Error()), 0
		}
		return framework.NewStatus(framework.Error, err.Error()), 0
	}

	if failed, reason := t.checkAndMarkGangFailed(ctx, p, obj); failed {
		return framework.NewStatus(framework.Unschedulable, reason), 0
	}

	round, _, err := unstructured.NestedInt64(obj.Object, "status", "round")
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error()), 0
	}

	minMember, found, err := unstructured.NestedInt64(obj.Object, "spec", "minMember")
	if err != nil || !found {
		return framework.NewStatus(framework.Error, "pod group minMember not found"), 0
	}

	var curWaiting int32
	t.handle.IterateOverWaitingPods(func(waiting framework.WaitingPod) {
		pod := waiting.GetPod()
		if !isPodSamePodGroup(p, pod) {
			return
		}
		curWaiting++
	})

	if curWaiting+1 >= int32(minMember) {
		t.handle.IterateOverWaitingPods(func(waiting framework.WaitingPod) {
			pod := waiting.GetPod()
			if !isPodSamePodGroup(p, pod) {
				return
			}
			waiting.Allow(t.Name())
		})
		return framework.NewStatus(framework.Success), 0
	}

	if curWaiting == 0 {
		err = unstructured.SetNestedField(obj.Object, round+1, "status", "round")
		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error()), 0
		}
		_, err = t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).UpdateStatus(ctx, obj, v3.UpdateOptions{})
		if err != nil {
			return framework.NewStatus(framework.Unschedulable, err.Error()), 0
		}
	}

	return framework.NewStatus(framework.Wait), 60 * time.Second
}

func isPodSamePodGroup(cur, target *v1.Pod) bool {
	labels := cur.Labels
	pgName, ok := labels["pod-group"]
	if !ok {
		return false
	}

	targetLabels := target.Labels
	targetPgName, ok := targetLabels["pod-group"]
	if !ok {
		return false
	}

	return pgName == targetPgName
}

type preemptionPlan struct {
	plans map[string]int64
}

func (p *preemptionPlan) Clone() framework.StateData {
	c := preemptionPlan{
		plans: make(map[string]int64),
	}
	for k, v := range p.plans {
		c.plans[k] = v
	}
	return &c
}

func (t *TracerPlugin) PreFilter(ctx context.Context, state *framework.CycleState, p *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	podGroupName, ok := p.Labels["pod-group"]
	if !ok {
		return nil, framework.NewStatus(framework.Success)
	}
	obj, err := t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).Get(ctx, podGroupName, v3.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, framework.NewStatus(framework.Unschedulable, err.Error())
		}
		return nil, framework.NewStatus(framework.Error, err.Error())
	}

	if failed, reason := t.checkAndMarkGangFailed(ctx, p, obj); failed {
		return nil, framework.NewStatus(framework.Unschedulable, reason)
	}

	details, found, err := unstructured.NestedSlice(obj.Object, "status", "preemptionDetail")
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}
	if !found {
		return nil, framework.NewStatus(framework.Success)
	}
	plans := make(map[string]int64)
	for _, d := range details {
		detail, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		plans[detail["nodeName"].(string)] += detail["count"].(int64)
	}
	klog.InfoS("preemption plan preemption detail", "plans", plans)

	pfState := &preemptionPlan{
		plans: plans,
	}
	state.Write(preemptionPlanState, pfState)

	return nil, framework.NewStatus(framework.Success)
}

func (t *TracerPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (t *TracerPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.Info("TracerPlugin Filter")
	_, ok := pod.Labels["pod-group"]
	if !ok {
		return framework.NewStatus(framework.Success)
	}

	data, err := state.Read(preemptionPlanState)
	if err != nil {
		return framework.NewStatus(framework.Success)
	}
	preemptionPlanData, ok := data.(*preemptionPlan)
	if !ok {
		return framework.NewStatus(framework.Error)
	}

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	curNodePlanCount := preemptionPlanData.plans[node.Name]
	var curExisting int
	for _, existingPod := range nodeInfo.Pods {
		if isPodSamePodGroup(existingPod.Pod, pod) {
			curExisting++
		}
	}
	if int64(curExisting) >= curNodePlanCount {
		return framework.NewStatus(framework.Unschedulable)
	}
	return framework.NewStatus(framework.Success)
}

func (t *TracerPlugin) InferencePreemption(ctx context.Context, p *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	nodes, err := t.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}

	victimGang := make(map[string][]*v1.Pod)
	podNodeMap := make(map[string]string)
	gangPriorityMap := make(map[string]int32)
	nominatingInfo := new(framework.NominatingInfo)
	for _, node := range nodes {
		nodeIns := node.Node()
		pods := node.Pods
		for _, pod := range pods {
			podIns := pod.Pod
			pgName, ok := podIns.Labels["pod-group"]
			if !ok {
				continue
			}
			if *podIns.Spec.Priority < *p.Spec.Priority {
				victimGang[pgName] = append(victimGang[pgName], podIns)
				podNodeMap[podIns.Name] = nodeIns.Name
				gangPriorityMap[pgName] = *podIns.Spec.Priority
			}
		}
	}

	var resourcesRequests = make(v1.ResourceList)
	for _, container := range p.Spec.Containers {
		for resourceName, resourceReq := range container.Resources.Requests {
			org := resourcesRequests[resourceName].DeepCopy()
			org.Add(resourceReq)
			resourcesRequests[resourceName] = org
		}
	}

	type gangNodeGroup struct {
		gangName string
		nodeName string
		podCount int32
	}

	candidatesGroup := make([]gangNodeGroup, 0)
	for gang, pods := range victimGang {
		for _, node := range nodes {
			nodeIns := node.Node()
			status := filteredNodeStatusMap[nodeIns.Name]
			if status.Code() == framework.UnschedulableAndUnresolvable {
				continue
			}
			curNodeAvailableResource := calculateNodeAvailableResource(node)
			var podCount int32
			for _, pod := range pods {
				if podNodeMap[pod.Name] == nodeIns.Name {
					podCount++
					for _, container := range pod.Spec.Containers {
						for resourceName, resourceReq := range container.Resources.Requests {
							org := curNodeAvailableResource[resourceName].DeepCopy()
							org.Add(resourceReq)
							curNodeAvailableResource[resourceName] = org
						}
					}
				}
			}
			if podCount <= 0 {
				continue
			}

			if !isResourceEnough(resourcesRequests, curNodeAvailableResource) {
				continue
			}
			candidatesGroup = append(candidatesGroup, gangNodeGroup{
				gangName: gang,
				nodeName: nodeIns.Name,
				podCount: podCount,
			})
		}
	}

	if len(candidatesGroup) <= 0 {
		return nil, framework.NewStatus(framework.Unschedulable)
	}

	sort.Slice(candidatesGroup, func(i, j int) bool {
		if gangPriorityMap[candidatesGroup[i].gangName] == gangPriorityMap[candidatesGroup[j].gangName] {
			return candidatesGroup[i].podCount < candidatesGroup[j].podCount
		}
		return gangPriorityMap[candidatesGroup[i].gangName] < gangPriorityMap[candidatesGroup[j].gangName]
	})
	curVictimGang := candidatesGroup[0]
	klog.Infof("find victim gang candidates candidatesGang %v", curVictimGang)

	nominatingInfo.NominatingMode = framework.ModeOverride
	nominatingInfo.NominatedNodeName = curVictimGang.nodeName
	victimPodGroupObj, err := t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).Get(ctx, curVictimGang.gangName, v3.GetOptions{})
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}

	preemptedInfo := map[string]interface{}{
		"jobName": p.GetLabels()["jobName"],
		"jobType": string(apis.JobTypeInference),
	}

	err = unstructured.SetNestedField(victimPodGroupObj.Object, preemptedInfo, "status", "preemptedBy")
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}
	_, err = t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).UpdateStatus(ctx, victimPodGroupObj, v3.UpdateOptions{})
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}

	return &framework.PostFilterResult{
		NominatingInfo: nominatingInfo,
	}, framework.NewStatus(framework.Success)
}

func isResourceEnough(a, b v1.ResourceList) bool {
	for rn, rq := range a {
		valueB := b[rn]
		if rq.Cmp(valueB) > 0 {
			return false
		}
	}
	return true
}

func calculateNodeAvailableResource(nodeInfo *framework.NodeInfo) v1.ResourceList {
	nodeAvailableResource := &framework.Resource{
		MilliCPU:         nodeInfo.Allocatable.MilliCPU - nodeInfo.Requested.MilliCPU,
		Memory:           nodeInfo.Allocatable.Memory - nodeInfo.Requested.Memory,
		EphemeralStorage: nodeInfo.Allocatable.EphemeralStorage - nodeInfo.Requested.EphemeralStorage,
		ScalarResources:  make(map[v1.ResourceName]int64),
	}
	for resourceName := range nodeInfo.Allocatable.ScalarResources {
		nodeAvailableResource.ScalarResources[resourceName] = nodeInfo.Allocatable.ScalarResources[resourceName] - nodeInfo.Requested.ScalarResources[resourceName]
	}
	var nodeAllocatableResourceList = make(v1.ResourceList)
	nodeAllocatableResourceList[v1.ResourceCPU] = *resource.NewMilliQuantity(nodeAvailableResource.MilliCPU, resource.DecimalSI)
	nodeAllocatableResourceList[v1.ResourceMemory] = *resource.NewQuantity(nodeAvailableResource.Memory, resource.BinarySI)
	nodeAllocatableResourceList[v1.ResourceEphemeralStorage] = *resource.NewQuantity(nodeAvailableResource.EphemeralStorage, resource.BinarySI)
	for rn, rq := range nodeAvailableResource.ScalarResources {
		nodeAllocatableResourceList[rn] = *resource.NewQuantity(rq, resource.DecimalSI)
	}
	return nodeAllocatableResourceList
}

func (t *TracerPlugin) PostFilter(ctx context.Context, state *framework.CycleState, p *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	klog.Infof("PostFilter called: pod=%s", p.Name)
	jobType, ok := p.Labels["jobType"]
	if !ok {
		return nil, framework.NewStatus(framework.Success)
	}
	if jobType == string(apis.JobTypeInference) {
		return t.InferencePreemption(ctx, p, filteredNodeStatusMap)
	}

	podGroupName, ok := p.Labels["pod-group"]
	if !ok {
		return nil, framework.NewStatus(framework.Unschedulable)
	}

	obj, err := t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).Get(ctx, podGroupName, v3.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, framework.NewStatus(framework.Unschedulable)
		}
		return nil, framework.NewStatus(framework.Error, err.Error())
	}
	minMember, found, err := unstructured.NestedInt64(obj.Object, "spec", "minMember")
	if err != nil || !found {
		return nil, framework.NewStatus(framework.Error, "pod group minMember not found")
	}

	if failed, reason := t.checkAndMarkGangFailed(ctx, p, obj); failed {
		return nil, framework.NewStatus(framework.Unschedulable, reason)
	}

	preemptionDetail, found, err := unstructured.NestedSlice(obj.Object, "status", "preemptionDetail")
	if err != nil {
		return nil, framework.NewStatus(framework.Error, "pod group preemptionDetail not found")
	}

	if !found {
		nodes, err := t.handle.SnapshotSharedLister().NodeInfos().List()
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}

		victimGang := make(map[string][]*v1.Pod)
		podNodeMap := make(map[string]string)
		gangPriorityMap := make(map[string]int32)
		nodeExistingPods := make(map[string]int)
		for _, node := range nodes {
			nodeIns := node.Node()
			pods := node.Pods
			for _, pod := range pods {
				podIns := pod.Pod
				pgName, ok := podIns.Labels["pod-group"]
				if !ok {
					continue
				}
				if *podIns.Spec.Priority < *p.Spec.Priority {
					victimGang[pgName] = append(victimGang[pgName], podIns)
					podNodeMap[podIns.Name] = nodeIns.Name
					gangPriorityMap[pgName] = *podIns.Spec.Priority
				}
				if isPodSamePodGroup(pod.Pod, p) {
					nodeExistingPods[node.Node().Name]++
				}
			}
		}

		curJobPodsNeeded := int(minMember)

		t.handle.IterateOverWaitingPods(func(waiting framework.WaitingPod) {
			pod := waiting.GetPod()
			if !isPodSamePodGroup(p, pod) {
				return
			}

			curJobPodsNeeded--
		})

		var singlePodResourcesRequests int64
		for _, container := range p.Spec.Containers {
			singlePodResourcesRequests += container.Resources.Requests.Cpu().MilliValue()
		}
		if singlePodResourcesRequests <= 0 {
			return nil, framework.NewStatus(framework.Unschedulable, "preemption planning skipped: no cpu request")
		}
		candidatesGang := make([]string, 0)
		plans := make(map[string]map[string]int)
		for gang, pods := range victimGang {
			plans[gang] = make(map[string]int)
			canRelease := false
			curGangJobPodsNeeded := curJobPodsNeeded
			for _, node := range nodes {
				var totalReplaceCount int
				for _, replaceCount := range plans[gang] {
					totalReplaceCount += replaceCount
				}
				nodeIns := node.Node()
				status := filteredNodeStatusMap[nodeIns.Name]
				if status.Code() == framework.UnschedulableAndUnresolvable {
					continue
				}
				curNodeAvailableResourceQuantity := calculateNodeAvailableCPU(node)
				for _, pod := range pods {
					if podNodeMap[pod.Name] == nodeIns.Name {
						for _, container := range pod.Spec.Containers {
							curNodeAvailableResourceQuantity += container.Resources.Requests.Cpu().MilliValue()
						}
					}
				}

				curNodePodsPlaceCount := min(int(curNodeAvailableResourceQuantity/singlePodResourcesRequests), curGangJobPodsNeeded-totalReplaceCount)
				if curNodePodsPlaceCount == 0 {
					continue
				}
				plans[gang][nodeIns.Name] = curNodePodsPlaceCount

				klog.Infof("node %s gang %s curAvailableCPU %d singlePodResourcesRequests %d", node.Node().Name, gang, curNodeAvailableResourceQuantity, singlePodResourcesRequests)

				if curNodePodsPlaceCount+totalReplaceCount >= curGangJobPodsNeeded {
					canRelease = true
					break
				}
			}
			if canRelease {
				candidatesGang = append(candidatesGang, gang)
			} else {
				delete(plans, gang)
			}
		}

		if len(candidatesGang) <= 0 {
			return nil, framework.NewStatus(framework.Unschedulable)
		}

		sort.Slice(candidatesGang, func(i, j int) bool {
			return gangPriorityMap[candidatesGang[i]] < gangPriorityMap[candidatesGang[j]]
		})
		curVictimGang := candidatesGang[0]
		klog.Infof("find victim gang candidates candidatesGang %v", curVictimGang)
		preemptionDetail = make([]interface{}, 0)

		for node, count := range plans[curVictimGang] {
			preemptionDetail = append(preemptionDetail, map[string]interface{}{
				"nodeName": node,
				"count":    int64(count) + int64(nodeExistingPods[node]),
			})
		}

		victimPodGroupObj, err := t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).Get(ctx, curVictimGang, v3.GetOptions{})
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}
		preemptedByInfo := map[string]interface{}{
			"jobName": p.Labels["jobName"],
			"jobType": string(apis.JobTypeTrain),
		}

		err = unstructured.SetNestedField(victimPodGroupObj.Object, preemptedByInfo, "status", "preemptedBy")
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}
		_, err = t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).UpdateStatus(ctx, victimPodGroupObj, v3.UpdateOptions{})
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}

		err = unstructured.SetNestedSlice(obj.Object, preemptionDetail, "status", "preemptionDetail")
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}
		_, err = t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).UpdateStatus(ctx, obj, v3.UpdateOptions{})
		if err != nil {
			return nil, framework.NewStatus(framework.Error, err.Error())
		}

		return nil, framework.NewStatus(framework.Unschedulable)
	}

	// TODO: 根据targetPlan和当前node上的同job的pod计算这个pod是否能被放到node上。

	return nil, framework.NewStatus(framework.Success)
}

func calculateNodeAvailableCPU(nodeInfo *framework.NodeInfo) int64 {
	return nodeInfo.Allocatable.MilliCPU - nodeInfo.Requested.MilliCPU
}

func (t *TracerPlugin) checkAndMarkGangFailed(ctx context.Context, p *v1.Pod, obj *unstructured.Unstructured) (bool, string) {
	failed, found, err := unstructured.NestedBool(obj.Object, "status", "failed")
	if err != nil {
		return true, err.Error()
	}
	if found && failed {
		return true, "pod group fail"
	}
	scheduleTimeout, found, err := unstructured.NestedInt64(obj.Object, "spec", "scheduleTimeoutSeconds")
	if err != nil || !found {
		return true, "pod group scheduleTimeoutSeconds not found"
	}

	creationTimestamp := obj.GetCreationTimestamp()
	if creationTimestamp.IsZero() {
		return true, "pod group creationTimestamp not found"
	} else if time.Since(creationTimestamp.Time) > time.Duration(scheduleTimeout)*time.Second {
		err = unstructured.SetNestedField(obj.Object, true, "status", "failed")
		if err != nil {
			return true, err.Error()
		}
		err = unstructured.SetNestedField(obj.Object, "gang schedule timeout failed", "status", "reason")
		if err != nil {
			return true, err.Error()
		}
		_, err = t.dyn.Resource(podGroupGVR).Namespace(p.Namespace).UpdateStatus(ctx, obj, v3.UpdateOptions{})
		if err != nil {
			return true, err.Error()
		}
		return true, "gang schedule timeout"
	}
	return false, ""
}
