# Gang 调度设计文档（mini Volcano）

> 本文档记录 `AIInfraSchedulerPlugin`（调度器插件侧）与 `AIInfraTrainJob`（控制器侧）协同实现的 gang 调度机制：为什么这么设计、每一跳为什么在那一刻发生、以及与生产实现的边界。面向「能逐行扛追问」的目标编写。

---

## 1. 背景与目标

分布式训练（DDP）的 N 个 worker 必须**同时在场**才能组起进程组、建立 all-reduce 通信环。缺一个，其余的全是死重——占着资源却永远进不了训练。

默认调度器是**逐个 Pod**调度的，没有「这一批必须一起成功」的概念。于是会出现 **partial-scheduling deadlock**：作业 A 抢到了一半节点、作业 B 抢到了另一半，两个半截 gang 互相攥着对方缺的那块，谁都跑不起来，谁也不让。

目标：实现 **all-or-nothing** 的 gang 调度——一个 gang 的所有成员要么一起拿到资源跑起来，要么一个都不占、整组退避；并在持续摆不下时**有界放弃**，把一个「无限挂起」变成一个「明确失败」交给上层。

---

## 2. 概念前提：数据层 vs 控制层

理解后面所有设计，先立这个 K8s 地基。系统被切成两层：

- **声明式数据层（名词）**：`Pod`、`PodGroup`、`Node`、`TrainJob`、`Queue`……全是 etcd 里的数据记录，apiserver 是门面。每条记录 = `spec`（期望状态）+ `status`（观测状态）。它们本身什么都不做。
- **主动控制层（动词）**：`scheduler`、`kubelet`、各种 `controller`……是**进程**，是跑着的控制循环。它们 watch 数据层对象，把现实往 `spec` 拽，再把观测写回 `status`。

对象是名词，控制器是动词，中间靠控制循环缝合：**进程盯着对象 → 改现实 → 写回对象**。

辨析三个易混点：

| 东西 | 本质 | 说明 |
|---|---|---|
| `PodGroup` | 纯数据对象 | 背后**没有进程**，就是一块记账板。「会不会重启」这个问题不成立——数据不重启，只有读写它的进程会重启。|
| `Pod` | 数据对象 | Pod 对象**不是**正在跑的容器。真正的进程是节点上的容器，由 kubelet + containerd 拉起。Pod 对象是图纸，容器是盖好的楼，kubelet 是照图施工的包工头。|
| `scheduler` | 进程（actor） | **不是**数据对象。它以 pod 形式跑（所以也有一个代表它的 Pod 对象），但角色是主动那一侧：watch「没分配节点的 Pod」→ 算落点 → 把节点名写进 Pod。|

这层分层是后面所有设计的根：**round 存在 PodGroup（数据层）里，所以 scheduler（控制层进程）崩溃重启，round 毫发无伤。**

---

## 3. 整体架构：三个角色 + PodGroup 契约

```
TrainJob 控制器 ──创建──> PodGroup(spec: minMember, scheduleMaxLimit)
       │                      ▲              │
       │创建带 pod-group        │ watch        │ 调度器读 spec / 写 status
       ▼ label 的 worker pods   │             ▼
   worker Pods ───────> 调度器插件(Permit/Unreserve/PreFilter)
       ▲                                      │
       └────────TrainJob 控制器 watch PodGroup.status.failed ────┘
                置 TrainJob Failed + 清掉本 attempt 的 worker pods
```

**PodGroup 是通用调度器与应用专属控制器之间的「契约接缝」。** 调度器的词汇表里只有 `Pod` 和 `PodGroup` 这种通用概念，**它根本不知道「TrainJob」是什么**。它只往 PodGroup 写一个通用裁决（`failed`），应用侧控制器再把它翻译成应用语义（TrainJob 置 Failed、清掉它自己那批 pod）。这正是 Volcano 的设计——调度器只认 PodGroup，不认业务 CRD。

字段约定：

| 字段 | 来源 | 含义 |
|---|---|---|
| `spec.minMember` | = `TrainJob.spec.worldSize` | gang 需要凑齐的成员数 |
| `spec.scheduleMaxLimit` | = `TrainJob.spec.scheduleMaxCount` | 最多尝试几轮，超过即认输 |
| `status.round` | 调度器写 | 已**开始**的轮数 |
| `status.failed` | 调度器写 | 认输终态标志（物化） |
| `status.reason` | 调度器写 | 失败原因，给人看 |

- PodGroup 命名 `<trainjob>-attempt-<n>`，**每个 attempt 一个**，靠 OwnerReference 挂在 TrainJob 下，attempt/job 消失时被 GC 级联回收。
- worker pod 带**通用** `pod-group` label（值 = PodGroup 名），调度器只认这个 label，与 TrainJob 解耦。

---

## 4. gang 生命周期

### 4.1 持有：为什么扣在 Permit，而不是准入阶段拦截

一个 gang 的 pod 被调度器**一个一个、间隔着**送进来。先到的 pod 此刻判断不了同伴最终能不能凑齐。问题是：把这些「已到、未凑齐」的 pod 扣在哪、怎么扣？

**结论：扣在 Permit 扩展点，靠返回 `Wait` 把 pod 挂进框架的等待室（`waitingPods` map）。** 凑齐时由最后那个 pod 用 `IterateOverWaitingPods` 遍历、按 `pod-group` label 过滤出同组成员，整组 `Allow`。

**为什么不能在 PreFilter 直接拒掉？** 两层原因，缺一层都讲不透：

1. **结构层**：PreFilter 是个「准入/踢走」的二元闸门，**没有持有缓冲区**。组 gang 的本质需求是让 N 个 pod 在某处「同时以挂起态共存」，才能一起数、一起放。Permit 的 `waitingPods` map 就是这个共存缓冲区——返回 `Wait` 的 pod **还活着、还攥着自己的 Reserve（资源占位）、留在调度器内存里**，同伴逐个累积，凑齐者一次性放行。而 unschedulableQ **不是共存缓冲区**：进去就是「这轮先放弃」，将来各自独立地重走一遍调度，从不在同一个决策瞬间「同时在场」被一起数到。
2. **唤醒机制层**：躺在 unschedulableQ 的 pod，只有它订阅的某个集群事件触发时才会被塞回 activeQ。而「我同 gang 的兄弟被创建/变得可调度」**根本没被接成唤醒事件**——vanilla 1.29 里通用 Pod add handler 对 queueing hint 是「Do nothing」，插件也没有一条 hint 把「兄弟出现」映射成「叫醒我」。于是这些 pod 只能等 5 分钟兜底 flush 一起醒，醒来又一个个评估，谁都数不到其他人（其他人在队列里、不算在场），再次被拒。

**结果就是 anti-phase thrashing**：成员彼此错相、永远拼不齐同一个瞬间，反复 Reserve/Unreserve 抖动。

**Permit 等待室一刀解决两层**：它本身是共存缓冲区（大家都活着挂在表里），且凑齐者**主动遍历、主动放行**，完全不依赖任何唤醒事件。

### 4.2 凑齐放行

每个进入 Permit 的 pod：数同组在等待室里的成员数 `curWaiting`，若 `curWaiting + 1 >= minMember`，说明加上自己刚好凑齐 → 遍历等待室把同组成员逐个 `Allow`，自己返回 `Success`。整组随后进入 binding cycle 真正绑定。

### 4.3 超时整组回收 + 级联收敛

等待室凑不齐，某成员的 Permit `Wait` 超时（60s）触发 `Unreserve`。

**为什么一个成员超时要拆掉整个 gang、把已攒的同伴全部轰走？** 因为 gang 是 all-or-nothing：缺一个，其余全是死重。如果只踢超时那个、留其余继续等，制造的不是浪费而是**资源死锁**——这半截 gang 攥着的资源本够另一个 gang 跑起来，两个半截互相攥着对方缺的那块，谁都进不去。所以「整组放行」必须配对偶的「整组回收」：一个成员宣告这轮拼不齐，就立刻把已攒资源全吐出来，让位给能真正跑起来的作业。

**级联怎么收敛、不会无限递归？** `Unreserve` 里去 `Reject` 同组其他 waiting pod，被 Reject 的 pod 自己又触发 `Unreserve`……收敛靠三条叠加：

1. **删除先于自身遍历**：框架处理 Reject 时，`WaitOnPermit` 解开带着 `defer waitingPods.remove(pod.UID)`，这个 remove 发生在该 pod 自己的 `Unreserve` 去遍历等待室**之前**。所以轮到任何 pod 的 Unreserve 遍历时，它自己、以及刚 Reject 它的那个都已不在 map 里。
2. **集合单调递减**：每个 pod 最多被处理一次，绝不回头 Reject 一个已走的。
3. **first-wins 幂等**：waitingPod 的状态 channel 带 1 缓冲、first-wins，万一同一 pod 被 Reject 两次，第二次 send 直接丢弃，no-op。

### 4.4 轮次计数：为什么 round 存 CRD、怎么做到每轮恰好加一次

每攒一轮、超时散一次，`round` 加一。两个设计选择：

**为什么 round 存 PodGroup.status（持久化），而不是调度器进程内存里搁个计数器？** 两条叠加理由，PodGroup 是唯一同时满足的：

- **持久性（durability）**：round 是**累积历史**，无法从任何当前在场状态推算；pod 一轮轮地来了又走、调度器进程也可能重启。需要一个跟进程生命周期解耦的持久存储——etcd（经 CRD）正是。
- **身份/粒度（identity）**：round 是「每个 gang 一份」的事实，需要一个身份和粒度都正好等于「一个 gang」的家。**Pod 太细**（一个 pod 只是一个成员、每 attempt 重建，且 N 个对称又可丢弃，凭什么让某个扛这个数）；**调度器太全局**（服务全集群所有 pod，眼里没有「gang」维度，塞进内存还会被重启清掉）。PodGroup 不偏不倚：一个对象 == 一个 gang，靠 OwnerReference 跟 attempt 一起生死。

**怎么保证每轮恰好自增一次，而不是被 N 个 pod 各加一遍？** Permit 里判 `curWaiting == 0`：等待室里同组成员为 0，说明**当前 pod 是这一轮第一个被调度到的**，由它自增；否则不加。

这条**为什么不竞态**？因为 **scheduling cycle 是单线程串行的**——调度器一次只把一个 pod 从 PreFilter 走到 Permit，处理完才轮下一个。同 gang 的 N 个 pod 流经 Permit 是前后脚、绝不并发。所以「读 curWaiting → 判断是否为 0 → 自增」这一串中间插不进另一个同组 pod，**不用加锁，串行性白送原子性**。

### 4.5 认输闸门

`round` 加到 `scheduleMaxLimit` 即认输。

**为什么要有上限、会认输，而不是无限重试？** gang 调度失败分两种：

- **暂时性**：资源被别的作业占着，跑完会腾出来——重试有意义。
- **持续性**：就算集群空着也摆不下（requests 太大 / WorldSize 超过节点能塞的数）——重试永远没用。

固定 round 上限是**粗糙但有效**的办法：不去精确区分两者，用「N 轮还不成就别耗了」一刀切掉持续性那种。无限重试会让 pod 一轮轮去 Reserve、空占资源（Wait 期间持有 Reserve 达 60s），还卡住别的能跑的低优先级 gang（部分死锁）。认输还把「无限挂起、用户永远等不到结果」变成一个**明确终态 Failed**——可观测性收益。

**顺序：PreFilter 里为什么必须先放「加入进行中轮次」再判认输？** PreFilter 先查 `curWaiting > 0`，有同伴在等就直接 `Success` 放行，把认输判断让位。原因：`round` 是在这一轮**开始时**（空房第一个 pod）就加到上限的，此刻这轮**正在进行、还可能成功**，被保护的是**正挂在等待室里 Wait 的同伴**（不是 Running——真 Running 说明已凑齐成功，走不到认输）。若一来就先判 `round >= limit` 认输，会把这窝合法进行中的同伴连锅端掉。

**「顺序」与「阈值」是两根正交的轴，别缠在一起**：

- **阈值 `>=` vs `>`**：决定预算是 N 轮还是 N+1 轮。配合「round 计已开始轮数」，`>= limit` 正好给 `limit` 轮，与 spec 一致。
- **顺序（先查 curWaiting）**：决定「进行中的最后一轮能不能跑完」。

**锁队友那个 bug 是靠「顺序」修的，不是靠把 `>=` 改成 `>`**——改阈值只是把被锁死的那一轮往后挪一格，bug 还在。

### 4.6 交接控制器：failed 物化与单写者边界

调度器认输后**只往 PodGroup.status 写 `failed=true` + `reason` 就收手**，不碰 TrainJob、不删 pod。

**为什么不直接动 TrainJob 或删 pod？**

- 调度器是**全集群共享的通用组件**，词汇表里只有 Pod / PodGroup，不知道「TrainJob」。让它背上具体应用的知识，分层就烂了。
- worker pod 的生命周期归 TrainJob 控制器（靠 OwnerReference 拥有），调度器去删 pod 等于**两个写者抢同一份生命周期**。
- 正确切分：scheduler 只负责放置、写绑定结果；pod 增删归创建它的控制器；TrainJob 状态归它自己的控制器 watch PodGroup 后改。**PodGroup 就是让这三者解耦的接缝。**

**为什么「认输」要单独物化成 `failed` 字段，而不是让控制器拿 `round >= scheduleMaxLimit` 自己算？**

- **derive-vs-store 闭环**：`round >= limit` **分不清**「正在跑合法最后一轮、还可能 Running」和「已认输」。只有 scheduler 这一个角色知道「我这是认输、不是在跑最后一轮」——它手里有全部上下文，下游控制器重建不出。所以这个「知道」必须由 scheduler 在决策那一刻**盖章进对象**。
- **level-triggered 控制器**：K8s 控制器是看当前状态对账的，可能观测到任何中间态、也可能错过瞬时跳变。让它拿 `round==limit` 这种**可推导但有歧义**的代理去判失败，很可能在「最后一轮还在进行」时误判冤杀。专门的 `failed` 标志是个**无歧义的 level 终态**：`true` = 终态、动手；`false`/不存在 = 没结束、别碰。

控制器侧反应：watch 到 `PodGroup.status.failed == true` → 把 TrainJob 置 `Failed`、写 `lastFailure.reason`、删掉本 attempt 的 worker pod。

**写一次守卫**：认输是一次性闸门。pod 进 unschedulableQ 后每次唤醒（5 分钟 flush 等）会重跑 PreFilter，若不加守卫会反复 `SetNestedField + UpdateStatus` 写同样的 `failed=true`（apiserver no-op，但白耗 API 往返）。守卫做法：写之前先读 `status.failed`，已 `true` 就直接返回 Unschedulable、跳过所有写。

---

## 5. 一个隐蔽的坑：status 字段必须 `+optional`

`round` / `failed` / `reason` 三个 status 字段若不加 `// +optional`，controller-gen 会把它们全标成 **required**。后果：调度器只增量写 `round` 的 `UpdateStatus` 提交的 status 缺 `failed`/`reason` → apiserver 校验打回 → Permit 静默返回 Unschedulable → **gang 永远组不成、`round` 一次都写不进、给-up 的 klog 一行不打**。

修法：三个 status 字段全加 `+optional`，`make manifests && make install`。

**根因**：status 是**观测状态**，由不同角色在不同时刻**增量写入**（调度器写 round；认输时写 failed/reason；控制器读）。强制它们一起写，本身就违背 status 语义——schema 必须允许各字段各自缺席。

---

## 6. 实验验证（两组对照）

### 6.1 摆不下 → 认输 → TrainJob Failed（2026-06-15）

配置：`WorldSize=3`、每 worker `cpu: 9`、`scheduleMaxLimit=2`、集群 2 个 16 核 worker 节点。9 核 > 节点一半 → 每节点只塞 1 个 → 上限 2 个 < 需要的 3 个，持续摆不下。

观测：

```
19:29:21  rank-0、rank-1 各 Reserve+Permit 进等待室（2 个在等，<3，挂住）
19:30:23  两个一起 Unreserve（Permit 60s 超时）→ 第一轮散  round 0→1
19:30:24  rank-2 进等待室；19:30:25 rank-0 再进（又攒到 2）
19:31:26  两个一起超时 Unreserve → 第二轮散  round 1→2
19:31:26  rank-1 跑 PreFilter，round(2) >= limit(2) → "gang gave up, rejecting"
```

结果：`TrainJob.status.phase = Failed`、`lastFailure.reason = gang failed`、3 个 worker pod 被清掉；`PodGroup.status = {failed:true, reason:"gang failed", round:2}`。每轮 ≈ 一个 60s Permit 超时，round 恰好计满 2 轮后认输，无早退、无 livelock。

### 6.2 放得下 → 整组放行 → Running（2026-06-16）

配置：同上但每 worker 改 `cpu: 5`（每节点可塞 3 个，总容量 6 ≫ 需求 3）。

观测：`14:06:33` rank-0/1/2 在同一个 17ms 窗口内全部 Reserve+Permit → `curWaiting+1` 凑到 3 → 整组 Allow → bind。**无一次 Unreserve、无 gang gave up、round 没动。**

结果：`TrainJob.status = {phase: Running, readyWorkers: 3, runningWorkers: 3}`。证明认输逻辑不冤杀正常作业。

**分水岭**：request 超过节点一半 → 单节点只塞 1 个，总容量被压到「节点数」；request ≤ 1/3 节点 → 容量翻几倍。

---

## 7. 与生产实现的边界（mini 版取舍）

主动声明这些边界，是「知道自己在做减法」的体现：

| 维度 | 本项目（mini） | 生产（Volcano/Kueue） |
|---|---|---|
| 读写 PodGroup | `dynamic` client + `unstructured.NestedInt64`/`found`/逐字段 err | typed client + informer/lister，编译期消灭这类错误 |
| 重试策略 | 固定 round 上限，超过即 Failed | 事件驱动 requeue + 指数 backoff + 抢占 |
| 重试状态 | 持久化进 PodGroup.status.round | coscheduling 甚至不持久化，进程内 TTL backoff |
| status 写入 | 在调度热路径（Permit/PreFilter）里写 | 热路径只读，单独 controller 写 status |
| 状态码策略 | 散落在各 return | helper 集中（如 `getPodGroup`/`markGangFailed`） |

> 选 `dynamic` 是刻意取舍：避免对钉死的 0.29.2 scheduler 模块做 go.mod 手术。代价是冗长的 found/err 处理。

---

## 8. 已知遗留

- **Permit 认输块缺与 PreFilter 对称的 write-once 守卫**：靠 PreFilter 先跑兜着，非 live bug；但 Permit 进认输块本就是 curWaiting 的 TOCTOU 抢跑路径（前提失效场景），建议补上对称守卫，不依赖「PreFilter 一定先拦住」这个外部前提。
- **可选重构**：把 `getPodGroup(ctx,ns,name) (*PodGroup, *Status)` 与 `markGangFailed(ctx,pg)` 抽成 helper，集中状态码策略，治「状态返回太多种、容易返回错」的冗余感。
