# 设计文档：统一 IO 引擎 (Generic Driver Model)

## 1. 核心目标与约束

本项目的目标是重构 `io-engine` 以同时支持 **Linux AIO** 和 **io_uring**，遵循以下严格约束：

1.  **调度逻辑不变**：保留原有的多队列（Embedded Lists）、优先级（Priority）和公平调度（Fair Scheduling）逻辑。
2.  **批量处理不变**：保留从用户提交的各个队列中取 io event 并且批量提交的逻辑。
3.  **零运行时开销**：
    *   **拒绝 Enum 抽象**：在热路径（Hot Path）中不使用 `enum Backend { Aio, Uring }` 进行分发。
    *   **初始化时决断**：根据配置初始化特定的 Driver（AIO 或 Uring），之后的工作线程完全运行在特定类型的上下文中。
4.  **双线程模型**：
    *   **Submitter**：负责从队列 poll 事件并提交给内核（通过 channel 控制预算）。
    *   **Completer**：负责从内核收割事件并执行回调。
5.  **Slot 差异化**：
    *   `IoTaskSlot` 由 AIO 和 Uring 分别实现。
    *   **AIO 版本**：必须包含 `iocb` 结构体（保持原有内存布局优化）。
    *   **Uring 版本**：不需要 `iocb`，结构更轻量。

## 2. 架构设计

核心思想是将 **调度/队列逻辑** 与 **驱动/执行逻辑** 解耦，通过 **泛型（Generics）** 来实现代码复用，而不是通过 Trait Objects 或 Enums。

### 2.1. 目录结构

```text
src/
├── lib.rs
├── scheduler/
│   ├── mod.rs
│   ├── common.rs       # 提取公共的 Multi-Queue, Budget Channel, Polling Algorithm
│   ├── context.rs      # 定义泛型的 IoContext<S>
│   ├── tasks.rs        # 定义通用的 IOEvent 结构
│   ├── aio.rs          # AIO 实现 (AioSlot, AioDriver)
│   └── uring.rs        # Uring 实现 (UringSlot, UringDriver)
└── ...
```

### 2.2. 公共调度层 (The Common Layer)

我们将原有的调度逻辑提取为泛型结构。

**1. 泛型上下文 (`IoContext<S>`)**
`IoContext` 不再直接依赖具体的 Slot 实现，而是接受一个泛型参数 `S` (Slot)。

```rust
// src/scheduler/context.rs

pub struct IoContext<S: SlotTrait> {
    // 任务队列、信号量、Waker 等公共设施
    pub(crate) queues: SharedQueues, 
    // 具体的 Slot 数组，由 Driver 管理或在此持有
    pub(crate) slots: Vec<UnsafeCell<S>>, 
    // ...
}

// 定义 Slot 必须满足的最小契约（如果有的话，主要是复位和设置 Event）
pub trait SlotTrait {
    fn set_event<C>(&mut self, event: IOEvent<C>);
    fn take_event(&mut self) -> Option<IOEvent<...>>;
}
```

**2. 公共轮询逻辑 (`scheduler/common.rs`)**
提取 `submitter` 线程中 "遍历多级队列、计算配额、取出 Event" 的逻辑为公共函数。

```rust
pub fn poll_queues_and_batch<S>(
    ctx: &IoContext<S>, 
    batch_size: usize
) -> Vec<usize> {
    // 1. 遍历 queues
    // 2. 找到由 event 的 slot
    // 3. 返回 slot index 的列表供 Driver 提交
}
```

### 2.3. 驱动层实现 (The Driver Layer)

AIO 和 Uring 分别实现各自的 `Driver` 和 `Slot`。

#### A. AIO 实现 (`src/scheduler/aio.rs`)

这是对现有代码的适配。

1.  **Slot 定义**:
    ```rust
    pub struct AioSlot<C: IOCallbackCustom> {
        pub event: Option<Box<IOEvent<C>>>,
        pub iocb: iocb, // AIO 特有：直接内嵌 iocb，保持原有内存布局
    }
    ```

2.  **Worker 实现**:
    *   **Start 函数**: 初始化 `IoContext<AioSlot>`。
    *   **Submitter Thread**: 
        *   调用公共的 `poll_queues_and_batch` 获取 indices。
        *   遍历 indices，直接操作 `slots[index].iocb` 进行 setup。
        *   调用 `io_submit`。
    *   **Completer Thread**:
        *   调用 `io_getevents`。
        *   根据返回的 `iocb` 指针或 user_data 找到 index。
        *   执行 `slots[index]` 的回调。

#### B. Uring 实现 (`src/scheduler/uring.rs`)

1.  **Slot 定义**:
    ```rust
    pub struct UringSlot<C: IOCallbackCustom> {
        pub event: Option<Box<IOEvent<C>>>,
        // Uring 不需要 iocb，可能需要保存一些 op 相关的临时状态，或者为空
    }
    ```

2.  **Worker 实现**:
    *   **Start 函数**: 初始化 `io_uring` 实例和 `IoContext<UringSlot>`。
    *   **Submitter Thread**:
        *   调用公共的 `poll_queues_and_batch` 获取 indices。
        *   获取 `ring.submission().available()`。
        *   遍历 indices，生成 SQE (Submission Queue Entry)，设置 `user_data = index`。
        *   调用 `ring.submit()`。
    *   **Completer Thread**:
        *   调用 `ring.submit_and_wait()` (或 peek)。
        *   遍历 CQE (Completion Queue Entry)。
        *   读取 `user_data` 获取 index。
        *   执行 `slots[index]` 的回调。

### 2.4. 统一入口

对外暴露的 `IoEngine` 可以是一个 enum 或者是根据编译/配置决定的类型别名，但在运行时，它包含的是具体的 Handle。

```rust
// 简化的对外接口示意
pub enum IoEngineEnum {
    Aio(IoHandle<AioSlot>),
    Uring(IoHandle<UringSlot>),
}

// 或者，如果用户不需要在一个进程内混用，可以直接通过 Builder 返回 opaque handle
impl IoEngineBuilder {
    pub fn build(self) -> Result<IoEngineEnum> {
        if self.use_uring && uring_is_supported() {
            // 启动 Uring Driver，返回 Uring 变体
        } else {
            // 启动 AIO Driver，返回 AIO 变体
        }
    }
}
```
*注意：虽然对外接口可能是 Enum，但这个 Enum 只在 submit 任务的最外层（channel send）使用一次。内部的 Worker 线程是完全隔离的，不存在 Enum 匹配开销。*

## 3. 重构步骤

1.  **Phase 1: 提取公共组件**
    *   创建 `src/scheduler/common.rs`。
    *   将 `context.rs` 中的 `EmbeddedList` 遍历逻辑、公平调度算法、以及 Budget Channel 控制逻辑移动到 `common.rs`。
    *   确保这些逻辑不依赖于具体的 `iocb`。

2.  **Phase 2: 泛型化 Context**
    *   修改 `src/scheduler/context.rs`，引入泛型 `S`。
    *   定义 `TaskSlot` trait (或者仅定义基本结构)，确保 Context 可以管理 Slot 的生命周期。

3.  **Phase 3: 适配 AIO (Legacy Migration)**
    *   将现有的 `aio.rs` 逻辑适配到新的泛型 Context 上。
    *   定义 `AioSlot` 包含 `iocb`。
    *   确保单元测试通过。

4.  **Phase 4: 实现 Uring**
    *   引入 `io-uring` crate。
    *   实现 `UringSlot` (无 `iocb`)。
    *   复制并修改 Worker 逻辑以适应 `io_uring` 的 submit/complete 接口。

5.  **Phase 5: 入口整合**
    *   实现自动检测逻辑（检查 Kernel 版本或创建 Ring 的能力）。
    *   提供统一的 Builder 接口。

## 4. 关键点总结

*   **解耦**: `Context` 负责**存**任务，`Driver` 负责**取**任务并**执行**。
*   **性能**: `AioSlot` 保持了 `iocb` 的连续内存布局，这对 `io_submit` 的性能至关重要。`UringSlot` 避免了该开销。
*   **一致性**: 调度算法复用同一套代码，保证了两种后端的行为（优先级、公平性）完全一致。
