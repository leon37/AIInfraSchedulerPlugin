# 最小 GPU 拓扑感知调度策略文档
## 这个调度策略到底想优化什么现象？
    避免只由scheduler根据宏观资源统计数据给pod分配node，导致分配到pod的node因为NUMA、拓扑分布等细节问题影响pod的运行效率。

##  scheduler 如果想比默认总量视图做得更好，至少还需要从 node 拿到哪些额外信息？
    设备拓扑、NUMA 归属、健康状态、局部可分配情况

##  哪些条件一旦不满足，就不该让节点继续进入候选集？
    资源总量不够、必要拓扑条件不满足、设备健康状态不满足

##  如果多个节点都没被淘汰，最后该按什么偏好给它们排序？
    更少跨 NUMA、更优设备局部性、更少潜在碎片化

## 这些调度所需信息，分别来自哪一层？
    capacity/available的信息scheduler可以直接从api-server拿到。
    NUMA归属、局部可分配情况、健康状态等是node/kubelet才能拿到的。
    device plugin 是原始设备事实来源，kubelet/devicemanager 负责接住并汇总

## 可选因素池（判断 哪些信息应该优先进入 Filter，哪些信息更适合留给 Score）
- 节点上该类 GPU 的总量是否满足 Pod 请求-Filter
- 当前健康可用的 GPU 数量是否满足请求-Filter
- GPU 是否位于允许的 NUMA 节点集合内-Filter
- 是否必须跨 NUMA 才能凑齐所需设备-依 policy 决定属于前置淘汰还是后续比较
- 当前节点上的 GPU 局部碎片化程度-Score
- 设备是否已经被其他 Pod 占用到难以形成连续可用组合-Filter
- GPU 与 CPU / 内存请求的拓扑亲和是否较好-Score
- GPU 与网卡 / PCIe / NVLink 的局部性是否更优-Score
- 节点当前是否还能满足某些必须的拓扑 policy-Filter
- 候选节点之间谁的 topology hint 更优-Score
- 候选节点之间谁更可能减少后续资源碎片化-Score
- 候选节点之间谁更可能给后续大任务留下更完整的资源-Score
- 设备健康状态是否稳定-Filter
- 该节点是否存在明显不满足条件的设备分配约束-Filter
- 在多个都可运行的节点里，哪个节点的本地设备布局更“整齐”-Score

## 当前练习项目中的最小模拟输入
    - Filter: 我选GPU 的总量是否满足 Pod 请求，拿 node-label来模拟, GPU-Capacity=8G/12G
    - Score: 候选节点之间谁的 topology hint 更优，拿 node-label来模拟, hint-score=80/100

## 问题定义
    默认 scheduler 主要基于 node 级 `capacity/allocatable` 总量视图做资源适配判断，但真实 GPU / NUMA / topology-aware 调度的关键难点在于：资源是否“总量够”并不等于这些资源在 node 内部的拓扑分布就满足当前 policy。

    一个典型场景是：某个 Pod 需要的两类资源在这台 node 上分别落在不同 NUMA 节点，而 node 本地 `TopologyManager` 当前 policy 又要求 `single-numa-node`。这时 scheduler 可能仅凭总量足够就先把 Pod 绑定到该 node，但 kubelet 在本地 topology admit 阶段会发现资源组合不成立或不再满足高质量分配要求。

    问题的根源不是 scheduler 不会比较数字，而是默认 node 视图缺少 node 内部更细粒度的拓扑与局部资源分布信息。这些信息最终会在 node/kubelet 本地汇成 topology 相关决策输入，因此高质量 GPU 调度必须同时处理“调度前的总量视图”和“节点侧的本地拓扑准入”之间的信息差。

## 设计目标
    这轮学习里最需要避免的坏结果是：Pod 被绑定到总量看起来足够、但 node 本地拓扑条件并不成立的节点，从而导致后续 topology admit 或高质量分配无法成立。

    自定义 `Filter/Score` 的价值不在于替代默认调度器，而在于在默认正式资源适配之上补充默认总量视图表达不出来或表达不够的额外约束与偏好，让调度判断能推进到更细粒度的策略层。

## 输入信息分层
    scheduler 默认最稳定能拿到的是 node 对象上的正式资源总量视图，也就是 `Node.Status.Capacity` 和 `Node.Status.Allocatable`。

    node/kubelet 本地还掌握更细粒度的拓扑决策输入，例如 `TopologyHint` 这类信息；它们在 kubelet `TopologyManager` 一侧形成和消费，不属于默认 scheduler 天然可见的总量视图。

    在当前练习项目里，这类默认 scheduler 看不到、但又希望提前介入判断的信息，只能先用 node label 或 Pod annotation 做粗粒度近似。

## 约束分层
    最典型的前置淘汰条件是：节点连 Pod 的正式资源请求都无法满足，或者连最基本的必要拓扑条件都不成立。

    在候选节点都未被淘汰之后，最典型的排序依据是：资源组合是否更倾向收敛在同一个 NUMA 节点内，从而减少跨 NUMA 带来的局部性损失。

## 最小对象模型
    当前这类调度问题最小需要围绕两类对象来组织：`Pod` 负责表达请求侧输入，`Node` 负责表达供给侧输入。

    scheduler 能稳定读取的是 `Pod` 和 `Node` 对象层上已经显式暴露出来的数据；但 `Node` 背后还隐藏着 kubelet 本地才掌握的拓扑、设备健康、局部分配和 admit 现实，这正是默认总量视图与真实调度现实之间的信息差来源。

## 输入优先级与解释权
    在当前 toy plugin 的输入优先级里，`Pod.Spec.Containers[*].Resources.Requests` 是未来应保留和继续靠拢的正式对象入口，`pluginConfig` 中的默认阈值则属于策略层应该长期保留的静态基线配置。

    Pod annotation 只应被看作当前实验阶段的临时兼容层：它可以作为 toy plugin 的外挂输入帮助快速验证思路，但不应被当成正式资源请求入口，否则会混淆插件自定义解释输入与 Kubernetes 原生资源模型之间的边界。

## 最小决策流程
    从当前已经验证过的链路看，一个 Pod 能否在某个 node 上高质量落地，至少会经历这样一条最小决策流程：`kube-apiserver/etcd` 持久化对象 -> scheduler 运行默认 `Fit/NodeResourcesFit` 等 in-tree 插件完成正式资源适配 -> 自定义 `Filter/Score` 追加策略层约束与偏好 -> kubelet 节点侧 `TopologyManager` 基于本地 topology/policy 做 admit -> `devicemanager` / device plugin 在分配阶段给出具体设备绑定与运行时注入信息。

    这条流程说明高质量 GPU 调度不是单一阶段完成的：中心调度先做正式资源适配与候选排序，节点本地再做 topology admit 与设备分配落地；任何只看其中一层的设计，都会忽略另一个层面的失败条件或性能代价。

## 实现分层映射
    明显属于正式资源适配的问题，例如 node 当前可分配 GPU 容量根本无法满足 Pod 请求，应优先落在默认 `Fit/NodeResourcesFit` 这类 in-tree 资源校验链，而不是留到后面的自定义 `Score` 再做软处理。

    默认 `Fit` 不表达、但节点一旦不满足就必须直接淘汰的必要条件，更适合落在自定义 `Filter`，例如某个节点虽然总量资源够，但已经不满足策略要求的必要拓扑条件。

    当多个节点都还能满足基本约束并通过 admit 时，更适合落在 `Score` 的是候选节点之间的 topology 优劣比较，例如比较 `TopologyHint` 所代表的局部性好坏，而不是把这种偏好前移成硬淘汰。

## 策略输入表（第一版）
| 输入项 | 当前主要来源 | scheduler 默认是否稳定可见 | 当前实验中的近似方式 | 更适合落在哪一层 |
| --- | --- | --- | --- | --- |
| Pod 正式资源请求 | `Pod.Spec.Containers[*].Resources.Requests` | 是 | 直接读取对象字段；早期用 annotation 临时兼容 | 默认 `Fit/NodeResourcesFit` |
| Node 正式总量资源 | `Node.Status.Capacity/Allocatable` | 是 | 直接读取对象字段；toy 实验曾用 node label 粗近似 | 默认 `Fit/NodeResourcesFit` |
| Node 必要拓扑条件 | node/kubelet 本地拓扑与 policy 现实 | 否 | node label 粗粒度模拟 | 自定义 `Filter` |
| 候选节点拓扑优劣 | node 本地 topology hint / 局部性现实 | 否 | node label 整数优先级模拟 | 自定义 `Score` |
| 节点侧 admit 结果 | `TopologyManager` 基于本地 provider hint 与 policy 的决策 | 否 | 当前无法被静态 metadata 准确模拟，只能做粗近似 | kubelet 节点侧 |

## 调度流程图（文字版）
1. Pod 被提交到 `kube-apiserver` 并持久化到 `etcd`，请求侧输入首先以对象字段形式稳定存在。
2. scheduler 读取 Pod 与 Node 对象，先由默认 `Fit/NodeResourcesFit` 这类 in-tree 插件检查正式资源请求是否满足 node 的 `capacity/allocatable` 视图。
3. 对于已经通过正式资源适配的节点，再由自定义 `Filter` 追加默认总量视图不表达或表达不够的必要约束，例如粗粒度拓扑前置条件。
4. 在剩余候选节点之间，自定义 `Score` 再根据局部性、拓扑优劣、碎片化风险等偏好做排序；当前 toy 实验里这些输入仍只能用静态 metadata 粗粒度近似。
5. Pod 被绑定到某个 node 之后，真正的 node 本地现实才开始继续发挥作用：kubelet `TopologyManager` 基于本地 provider hint 与 policy 做 admit，`devicemanager` / device plugin 再完成设备分配与运行时注入。
6. 因此，高质量 GPU 调度不是单个插件或单个阶段可以独立完成的，而是“正式资源适配 -> 自定义约束/偏好 -> 节点侧 topology admit -> 设备分配落地”这一整条链共同决定的结果。
