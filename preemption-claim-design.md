# 抢占—认领(claim)设计 & 扩展点职责速查地图

> 用途:一页纸的速查。忘了"谁负责啥、这个数怎么算"时瞄这里,不用在脑子里重建整条链。
> 配套:`gang-scheduling-design.md`(gang 那一半)。

## 1. 一句话钥匙

**PreFilter 每 pod 跑一次,管"pod 整体行不行";Filter 每节点跑一次,管"这个节点行不行"。**
做一次的、跟具体节点无关的 → PreFilter;逐节点比对的 → Filter。

## 2. 扩展点职责(本插件)

| 扩展点 | 调用频率 | 在本插件里干什么 |
|--------|----------|------------------|
| PreFilter | 每 pod 一次 | ① gang 这一轮认输判断(round 到顶就短路整个 pod)；② 读抢占方 PodGroup 的 preemptionDetail,建 targetPlan 写进 CycleState 给 Filter 用 |
| Filter | 每节点一次 | claim 否决:节点不在 targetPlan → 拒;节点上本组 pod 数 ≥ targetPlan[node] → 拒;否则放行。**planless 的普通 pod 一律 no-op 放行** |
| PostFilter | Filter 全挂后才跑 | 生成抢占 plan(见 §3)、在 victim PodGroup 写 preemptedBy、在抢占方 PodGroup 写 preemptionDetail,然后返回 Unschedulable(本轮不 bind,pod 重排) |
| Reserve / Unreserve | 选中节点后 / 失败时 | gang 占位;失败时级联把整组 deny |
| Permit | 进入等待室时 | gang 等待室(凑齐 minMember 才放行)+ round 计数(空房第一名自增) |

## 3. claim 判定(核心)

**plan 存哪:** 抢占方**自己**的 PodGroup `status.preemptionDetail`,schema = `[]{nodeName, count}`。

**count[N] 的定义(分母):** 节点 N 最终应容纳的**本组 pod 总数**
```
count[N] = 腾空后的额外容量(额外能塞几个) + 该节点上已存在的同组 waiting pod 数(baseline)
```
- 额外容量:`(idle + victim 在 N 上的 pod 释放的 CPU) / 单 pod 请求`。只有它参与 PostFilter 的 canRelease/预算判断。
- baseline:PostFilter 里从 snapshot 数 `isPodSamePodGroup` 的 pod,**只在写 preemptionDetail 那一刻叠加**,不许漏进 canRelease 那套算术。

**分子:** Filter 时,当前 `nodeInfo` 上本组(`isPodSamePodGroup`)的 pod 数。此刻直接从入参拿,不用再要 snapshot。

**认领 N ⟺ 分子 < count[N] 且 N ∈ targetPlan。**

**为什么分子分母都含 baseline:** 一个 pre-existing 的 waiting 兄弟和一个刚认领的新 pod,在 snapshot 里都是"assume 在 N 上的本组 pod",无字段可分。分子剔不掉它们,就让分母也含它们,两边抵消 → 既基线自洽、又靠 snapshot 自愈。

## 4. 数据流

```
PostFilter(Filter全挂) ── 写 preemptionDetail 到抢占方 PodGroup ──┐
                                                                  │(持久)
                          victim 腾空、抢占方重排                  │
PreFilter(每pod一次) ── 读 preemptionDetail → 建 targetPlan → 写 CycleState
                                                                  │(本轮)
Filter(每节点一次) ── 读 CycleState 的 targetPlan + 数 nodeInfo 本组 pod → 否决/放行
```
- 持久真相 = PodGroup;实时真相 = snapshot;CycleState = 这一轮的工作副本(每 pod 每次尝试新建、丢弃,不跨重试)。
- CycleState 存值须实现框架的 StateData 接口(带 Clone()),裸 map 塞不进去。

## 5. 已定 / 未解决

- 已定:claim 判定住 **PreFilter(读)+ Filter(逐节点否决)**;count = 额外 + baseline。
- 未解决(搁置):**防偷**——Filter 只把自己这组约束到预留节点,拦不住第三方 pod 在抢占方回来前偷走腾出的空位。
- 未解决(搁置):PostFilter 的 victim 选择目前会挑到优先级并非最低的一组(只要它腾出的空间够);plan 失效后的重算/复核。
