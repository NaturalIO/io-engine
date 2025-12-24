# 设计文档：统一 IO 引擎 (Shared Context Model)

## 1. 核心目标与约束

本项目的目标是重构 `io-engine` 以同时支持 **Linux AIO** 和 **io_uring**，并遵循以下严格约束：

1.  **对上层透明**：用户使用的 `IOContext` 核心逻辑保持一致，但构造方式略有变化以支持自定义队列。
2.  **IoContext 泛型化**：对外暴露的 `IOContext` 结构体引入队列泛型参数 `Q`（实现 `BlockingRxTrait`），以支持灵活的队列策略（单队列、多路复用等）。
3.  **零运行时开销**：
    *   **拒绝 Enum 分发**：工作线程（Worker）内部不使用 Enum 判断后端类型。
    *   **独立 Slot 定义**：`IoTaskSlot` 由 AIO 和 io_uring 分别定义。AIO 版本保留 `iocb` 以维持内存布局优化；io_uring 版本则轻量化实现。
4.  **双线程模型**：保持 Submitter 和 Poller 双线程模型。
5.  **调度逻辑解耦**：IO 引擎不再强制绑定特定的队列实现（如优先级/读/写三队列），而是通过泛型 `BlockingRxTrait` 接口消费任务。

## 2. 架构设计

核心思想是将 **数据提交（用户侧）** 与 **任务执行（Worker 侧）** 完全分离。用户侧通过自定义的队列发送任务；Worker 侧通过泛型接口从队列接收任务并执行。

### 2.1. 目录结构

`driver` 目录专注于驱动实现。

```text
src/
├── lib.rs
├── tasks.rs            # IOEvent 定义
├── callback_worker.rs  # 回调 Worker
├── embedded_list.rs    # 链表工具
├── merge.rs            # IO 合并逻辑
├── common.rs           # 公共调度逻辑 (SlotCollection 等)
├── context.rs          # IOContext (对外) 和 IoCtxShared (共享)
└── driver/             # 驱动实现
    ├── mod.rs
    ├── aio.rs          # AIO Driver & Slot 实现
    └── uring.rs        # io_uring Driver & Slot 实现 (待实现)
```

### 2.2. 共享上下文 (`IoCtxShared`)

这是连接用户和 Worker 的桥梁。它持有泛型队列 `Q`，但不包含任何后端特定的数据结构。

```rust
// src/context.rs

pub struct IoCtxShared<C: IoCallback, Q> {
    // 配置
    pub depth: usize,
    
    // 泛型队列，实现 crossfire::BlockingRxTrait
    pub queue: Q,
    
    // 状态
    pub running: AtomicBool,
    pub free_slots_count: AtomicUsize, // 空闲 Slot 计数
    
    // 辅助
    pub cb_workers: IOWorkers<C>,
    pub null_file: File, // File handle for /dev/null
}

// 对外接口
pub struct IOContext<C: IoCallback, Q> {
    pub(crate) inner: Arc<IoCtxShared<C, Q>>,
}
```

### 2.3. 驱动层 (Drivers)

Worker 线程的逻辑被封装在各自的 Driver 模块中。每个 Driver 拥有自己的 `start` 方法，启动线程并运行 loop。

#### A. AIO Driver (`src/driver/aio.rs`)

```rust
pub struct AioDriver;

impl AioDriver {
    // 启动 Submitter 和 Poller 线程
    // Q 必须实现 BlockingRxTrait，用于接收 IOEvent
    pub fn start<C: IoCallback, Q: Send + BlockingRxTrait<Box<IOEvent<C>>>>(
        ctx: Arc<IoCtxShared<C, Q>>,
    ) -> io::Result<()> {
        // ... (初始化 aio_context, slots, channels) ...

        // 启动 Submitter 线程
        thread::spawn(move || AioWorker::submit(ctx_submit, inner_submit, r_free));

        // 启动 Poller 线程
        thread::spawn(move || AioWorker::poll(ctx_poll, inner_poll, s_free));

        Ok(())
    }
}

// AIO Worker 线程的优化退出逻辑:
// 1.  **Submitter 线程退出检测**: 当用户侧的队列发送端 (`Sender`) 被 Drop 导致 `BlockingRxTrait::recv()` 返回错误时，Submitter 线程会检测到队列已关闭且无可提交任务。
// 2.  **Submitter 提交退出信号**: Submitter 线程此时会从空闲 Slot 队列中获取一个 Slot，并构造一个特殊的零长度读 (`aio_nbytes = 0`) 请求到 `/dev/null` 文件描述符，然后将其提交到 AIO 上下文。这个操作不执行实际的 IO，但会生成一个完成事件来唤醒 Poller 线程。
// 3.  **Poller 阻塞等待**: Poller 线程将 `io_getevents` 的 `timespec` 参数设置为一个较长的超时时间（或模拟无限阻塞），以避免 CPU 频繁唤醒。
// 4.  **Poller 识别并优雅退出**: `worker_poll` 内部逻辑将识别这个零长度读完成事件（通过 `iocb.aio_nbytes == 0`）。一旦识别到，Poller 线程会标记收到退出信号，并进入一个 "排空模式"，继续处理任何剩余的、非零长度的 IO 事件，直到所有 `depth` 的 Slot 都被释放（即 `ctx.free_slots_count == ctx.depth`），确保所有 IO 均已完成。最后，Poller 线程优雅地退出。
```

### 2.4. 公共调度逻辑 (`src/common.rs`)

`poll_request_from_queues` 函数现在基于 generic `Q` (BlockingRxTrait) 工作。它首先尝试阻塞接收一个任务，然后非阻塞接收后续任务以填充 batch。

```rust
pub fn poll_request_from_queues<C, I, Q>(
    ctx: &IoCtxShared<C, Q>, quota: usize, slots: &mut I, _last_write: &mut bool,
) where
    C: IoCallback,
    Q: BlockingRxTrait<Box<IOEvent<C>>>,
    I: SlotCollection<C>,
{
    while slots.len() < quota {
        if slots.len() == 0 {
            // 第一个任务阻塞接收
            match ctx.queue.recv() {
                Ok(event) => slots.push(event),
                Err(_) => break,
            }
        } else {
            // 后续任务非阻塞接收
            match ctx.queue.try_recv() {
                Ok(event) => slots.push(event),
                Err(_) => break,
            }
        }
    }
}
```

### 2.5. 初始化流程

`IOContext::new` 接收一个已经初始化好的接收端队列 `rx`。发送端 `tx` 由用户持有，用户直接向 `tx` 发送任务，不需要调用 `IOContext::submit`。

```rust
impl<C: IoCallback, Q> IOContext<C, Q>
where
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
{
    pub fn new(depth: usize, queue: Q, cbs: &IOWorkers<C>) -> Result<Arc<Self>, io::Error> {
        let inner = Arc::new(IoCtxShared {
            depth,
            running: AtomicBool::new(true),
            queue,
            cb_workers: cbs.clone(),
            free_slots_count: AtomicUsize::new(depth),
            null_file: File::open("/dev/null")?, // Add /dev/null File handle
        });

        // 启动驱动 (根据配置或默认选择 AIO)
        AioDriver::start(inner.clone())?;

        Ok(Arc::new(Self { inner }))
    }
}
```

### 2.6. IO 合并 (`src/merge.rs`)

`IOMergeSubmitter` 也进行了泛型化，不再依赖 `IOContext`，而是直接持有发送端 `Sender` (实现 `BlockingTxTrait`)。

```rust
pub struct IOMergeSubmitter<C: IoCallback, S: BlockingTxTrait<Box<IOEvent<C>>>> {
    // ...
    sender: S,
    // ...
}
```

## 3. 重构收益

*   **队列策略解耦**：用户可以自由选择单队列、MPMC、优先级队列或使用 `crossfire::select::Multiplex` 组合多种队列，只要实现了 `BlockingRxTrait`。
*   **API 灵活性**：移除了 `IOContext` 中硬编码的 `submit` 逻辑，减少了中间层。
*   **可测试性**：更容易mock队列进行单元测试。
*   **优雅退出机制**：通过零长度读信号和 `/dev/null` 实现了 AIO Worker 线程的优雅退出，减少了关闭时的等待时间。