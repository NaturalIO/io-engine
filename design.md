# 设计文档：统一 IO 引擎 (Shared Context Model)

## 1. 核心目标与约束

本项目的目标是重构 `io-engine` 以同时支持 **Linux AIO** 和 **io_uring**，并遵循以下严格约束：

1.  **对上层透明**：用户使用的 `IOContext` 接口保持不变（API 兼容），不需要感知底层使用的是 AIO 还是 io_uring。
2.  **IoContext 无泛型**：对外暴露的 `IOContext` 结构体**不引入**后端类型的泛型参数（如 `IOContext<S>`），仅保留回调泛型 `C`。
3.  **零运行时开销**：
    *   **拒绝 Enum 分发**：工作线程（Worker）内部不使用 Enum 判断后端类型。
    *   **独立 Slot 定义**：`IoTaskSlot` 由 AIO 和 io_uring 分别定义。AIO 版本保留 `iocb` 以维持内存布局优化；io_uring 版本则轻量化实现。
4.  **双线程模型**：保持 Submitter 和 Poller 双线程模型。
5.  **调度逻辑复用**：原有的多队列（优先级/读/写）、公平调度、Budget 控制逻辑提取为公共模块。

## 2. 架构设计

核心思想是将 **数据提交（用户侧）** 与 **任务执行（Worker 侧）** 完全分离。用户侧只与一个共享的、无后端状态的上下文交互；Worker 侧则持有具体的驱动和 Slot 状态。

### 2.1. 目录结构调整

将通用组件移至顶层，`scheduler` 目录专注于调度和后端实现。

```text
src/
├── lib.rs
├── tasks.rs            # 原 scheduler/tasks.rs (仅保留 IOEvent)
├── callback_worker.rs  # 原 scheduler/callback_worker.rs
├── embedded_list.rs    # 原 scheduler/embedded_list.rs
├── merge.rs            # 原 scheduler/merge.rs
└── scheduler/
    ├── mod.rs
    ├── context.rs      # 定义 IOContext (对外) 和 IoSharedContext (共享)
    ├── common.rs       # 提取公共调度逻辑 (Queue Polling, Budget)
    ├── aio.rs          # AIO Driver & Slot 实现
    └── uring.rs        # io_uring Driver & Slot 实现
```

### 2.2. 共享上下文 (`IoSharedContext`)

这是连接用户和 Worker 的桥梁。它不包含任何后端特定的数据结构（如 `aio_context_t` 或 `io_uring` 实例），也不包含 `slots`。

```rust
// src/scheduler/context.rs

pub struct IoSharedContext<C: IOCallbackCustom> {
    // 配置
    pub depth: usize,
    
    // 状态
    pub running: AtomicBool,
    pub total_count: AtomicUsize,      // Pending 任务总数
    pub free_slots_count: AtomicUsize, // 空闲 Slot 计数
    
    // 队列 (多级队列)
    pub prio_queue: SegQueue<Box<IOEvent<C>>>,
    pub read_queue: SegQueue<Box<IOEvent<C>>>,
    pub write_queue: SegQueue<Box<IOEvent<C>>>,
    pub prio_count: AtomicUsize,
    pub read_count: AtomicUsize,
    pub write_count: AtomicUsize,
    
    // 辅助
    pub cb_workers: IOWorkers<C>,
    pub noti_sender: Sender<()>, // 用于唤醒 Submitter
}

// 对外接口
pub struct IOContext<C: IOCallbackCustom> {
    pub(crate) inner: Arc<IoSharedContext<C>>,
    pub(crate) noti_sender: Sender<()>,
}
```

### 2.3. 驱动层 (Drivers)

Worker 线程的逻辑被封装在各自的 Driver 模块中。每个 Driver 拥有自己的 `start` 方法，启动线程并运行 loop。

#### A. AIO Driver (`src/scheduler/aio.rs`)

```rust
// AIO 专有的 Slot，包含 iocb
struct AioSlot<C: IOCallbackCustom> {
    event: Option<Box<IOEvent<C>>>,
    iocb: iocb, 
}

pub struct AioDriver;

impl AioDriver {
    // 启动 Submitter 和 Poller 线程
    pub fn start<C>(ctx: Arc<IoSharedContext<C>>) -> io::Result<()> {
        // 1. 初始化 aio_context
        // 2. 分配 Vec<AioSlot>
        // 3. 启动线程，传入 ctx 和 slots
    }
}
```

#### B. io_uring Driver (`src/scheduler/uring.rs`)

```rust
// Uring 专有的 Slot，更加轻量
struct UringSlot<C: IOCallbackCustom> {
    event: Option<Box<IOEvent<C>>>,
}

pub struct UringDriver;

impl UringDriver {
    pub fn start<C>(ctx: Arc<IoSharedContext<C>>) -> io::Result<()> {
        // 1. 初始化 io_uring
        // 2. 分配 Vec<UringSlot>
        // 3. 启动线程
    }
}
```

### 2.4. 公共调度逻辑 (`src/scheduler/common.rs`)

将原 `worker_submit` 中复杂的队列遍历和配额计算逻辑提取出来。

```rust
pub fn poll_request_from_queues<C>(
    ctx: &IoSharedContext<C>, 
    events_out: &mut VecDeque<Box<IOEvent<C>>>
) {
    // 实现原有的 EmbeddedList 遍历、优先级控制、Budget 逻辑
    // 从 ctx.queues 中取出 event 放入 events_out
}
```

### 2.5. 初始化流程

`IOContext::new` 充当工厂方法。

```rust
impl<C> IOContext<C> {
    pub fn new(depth: usize, cbs: &IOWorkers<C>) -> Result<Arc<Self>, io::Error> {
        // 1. 创建 IoSharedContext
        let shared = Arc::new(IoSharedContext { ... });
        
        // 2. 决策使用哪种后端
        if uring_supported() {
            UringDriver::start(shared.clone())?;
        } else {
            AioDriver::start(shared.clone())?;
        }
        
        // 3. 返回 IOContext
        Ok(Arc::new(Self { inner: shared, ... }))
    }
}
```

## 3. 重构步骤

1.  **文件移动**:
    *   将 `scheduler/{tasks.rs, callback_worker.rs, embedded_list.rs, merge.rs}` 移动到 `src/` 根目录。
    *   修正 `lib.rs` 和各模块的 `use` 引用。

2.  **定义 SharedContext**:
    *   修改 `scheduler/context.rs`。
    *   剥离 `IOContextInner` 中的 backend 字段（`context`, `slots`）。
    *   保留队列和计数器。

3.  **提取 Common Logic**:
    *   创建 `scheduler/common.rs`。
    *   将原 `worker_submit` 中的队列处理逻辑迁移至此。

4.  **适配 AIO**:
    *   在 `scheduler/aio.rs` 中重新实现 Worker Loop。
    *   定义 `AioSlot` (包含 `iocb`)。
    *   使用 `IoSharedContext` 进行交互。

5.  **实现 io_uring**:
    *   在 `scheduler/uring.rs` 中实现 Worker Loop。
    *   定义 `UringSlot`。

## 4. 关键收益

*   **API 零变动**：用户代码不需要任何修改。
*   **极致性能**：Worker 内部直接操作具体类型的 Slot，无虚函数调用，无 Enum 匹配。
*   **内存优化**：io_uring 模式下不需要分配 `iocb` 内存。