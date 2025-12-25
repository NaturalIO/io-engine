# 设计文档：统一 IO 引擎 (Linux AIO + io_uring)

## 1. 概述
本项目的目标是重构 `io-engine` crate，使其能够同时支持 **Linux 原生 AIO** 和 **io_uring** 作为后端实现。该 crate 将向用户暴露一个统一的接口，能够自动选择最佳的可用后端（如果可用则优先选择 `io_uring`），或者允许用户显式选择。

## 2. 架构变更

### 2.1. 目录结构重构
目前位于 `src/scheduler/` 中的后端特定代码将被移动到一个新的顶层模块 `src/backend/` 中。

```
src/
├── lib.rs
├── backend/            # 新增：封装内核交互
│   ├── mod.rs          # 定义通用 Traits 和类型
│   ├── aio.rs          # Linux AIO 实现
│   └── uring.rs        # 新增：io_uring 实现
└── scheduler/
    ├── context.rs      # 通用调度逻辑
    ├── tasks.rs        # 任务定义（重构为后端无关）
    └── ...
```

### 2.2. 后端抽象 (`src/backend/mod.rs`)

我们将移除 `BackendSlotData` 枚举，采用后端内部管理状态的模式。`IOEventTaskSlot` 将保持纯净，只存储通用的 Event 数据。

```rust
use crate::scheduler::tasks::{IOEvent, IOCallbackCustom};
use std::io;

/// 可用的后端类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Aio,
    Uring,
}

/// Context 用于驱动 IO 的核心 Trait
pub trait IoBackend: Send + Sync {
    /// 返回此后端的类型
    fn backend_type(&self) -> BackendType;

    /// 准备一个任务以供提交。
    /// - `index`: 任务在全局 slot 数组中的索引（用于完成时识别）。
    /// - `event`: 包含 FD、Buffer、Offset 等信息的 IO 事件。
    /// 
    /// 对于 AIO，这会更新内部维护的 iocb 数组。
    /// 对于 io_uring，这会获取一个 SQE 并填充数据。
    fn prepare<C: IOCallbackCustom>(&mut self, index: usize, event: &IOEvent<C>);

    /// 执行批量提交。
    /// 将所有通过 `prepare` 添加的任务提交给内核。
    fn submit(&mut self) -> io::Result<usize>;

    /// 轮询完成情况。
    /// 返回已完成任务的 slot 索引列表。
    /// 对于 AIO，可能会包含部分写入的重试逻辑（在后端内部处理）。
    /// 
    /// 参数 `timeout` 可用于控制是否阻塞等待。
    fn poll(&mut self, min: usize) -> io::Result<Vec<CompletionInfo>>;
}

pub struct CompletionInfo {
    pub index: usize,
    pub result: i32, // 0 或 正数表示传输字节数，负数表示 errno
}
```

### 2.3. 任务槽重构 (`src/scheduler/tasks.rs`)

`IOEventTaskSlot` 变得非常精简，不再包含任何后端特定的数据。

```rust
pub struct IOEventTaskSlot<C: IOCallbackCustom> {
    pub(crate) event: Option<Box<IOEvent<C>>>,
    // 之前是 pub(crate) iocb: aio::iocb, 现在移除
}
```

### 2.4. IO Context 更新 (`src/scheduler/context.rs`)

`IOContext` 将持有 `Box<dyn IoBackend>`。

- **Submission Worker**:
  1. 从队列获取 Event。
  2. 获取一个空闲 Slot Index。
  3. 将 Event 放入 Slot。
  4. 调用 `backend.prepare(index, event)`。
  5. 循环结束后调用 `backend.submit()`。

- **Poll Worker**:
  1. 调用 `backend.poll(min_events)`。
  2. 遍历返回的 `CompletionInfo`。
  3. 根据 `info.index` 找到对应的 Slot。
  4. 取出 Event，设置结果，执行回调。
  5. 释放 Slot Index。

### 2.5. 后端实现细节

#### AIO Backend (`src/backend/aio.rs`)
- 内部维护 `Vec<iocb>`，大小与 depth 一致。
- 内部维护 `Vec<*mut iocb>` 作为提交队列。
- `prepare(index, event)`: 根据 event 填充 `iocbs[index]`，并将 `&mut iocbs[index]` 加入提交队列。
- `submit()`: 调用 `io_submit`。
- `poll()`: 调用 `io_getevents`，转换 `io_event` 为 `CompletionInfo`。

#### io_uring Backend (`src/backend/uring.rs`)
- 持有 `io_uring::IoUring` 实例。
- `prepare(index, event)`: `ring.submission().push(entry.user_data(index as u64))`。
- `submit()`: `ring.submit()`。
- `poll()`: `ring.submit_and_wait()`，遍历 CQE，提取 `user_data` 作为 index。

## 3. 实施步骤

1.  **准备工作**:
    - 添加 `io-uring` 依赖。
    - 创建 `src/backend/` 目录。
2.  **重构 Tasks**:
    - 修改 `tasks.rs`，移除 `iocb`，简化 `IOEventTaskSlot`。
3.  **提取 AIO**:
    - 将 `scheduler/aio.rs` 移动到 `backend/aio.rs`。
    - 改造 AIO 逻辑以适应新的 `IoBackend` 接口（内部管理 `iocb`）。
4.  **修改 Context**:
    - 更新 `context.rs` 以使用 `IoBackend` trait。
5.  **实现 uring**:
    - 新增 `backend/uring.rs`。
6.  **集成与测试**:
    - 实现自动检测与工厂方法。
    - 运行测试。

## 4. 审查要点
- `BackendSlotData` 枚举已移除，符合用户要求。
- 状态管理下沉至 Backend 实现内部。
- 接口更加清晰 (`prepare` -> `submit`)。