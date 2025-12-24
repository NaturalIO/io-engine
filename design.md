# Design Document: Unified IO Engine (Linux AIO + io_uring)

## 1. Overview
The goal is to refactor the `io-engine` crate to support both **Linux Native AIO** and **io_uring** as backend implementations. The crate will expose a unified interface to the user, automatically selecting the best available backend (preferring `io_uring` if available) or allowing explicit selection.

## 2. Architectural Changes

### 2.1. Directory Structure Refactoring
The backend-specific code currently residing in `src/scheduler/` will be moved to a new top-level module `src/backend/`.

**Current:**
```
src/
└── scheduler/
    ├── aio.rs          <-- Tight coupling
    ├── context.rs
    └── ...
```

**Proposed:**
```
src/
├── lib.rs
├── backend/            # NEW: Encapsulates kernel interactions
│   ├── mod.rs          # Defines generic Traits and Types
│   ├── aio.rs          # Linux AIO implementation (moved from scheduler/aio.rs)
│   └── uring.rs        # NEW: io_uring implementation
└── scheduler/
    ├── context.rs      # Generic scheduling logic
    ├── tasks.rs        # Task definitions (refactored to be backend-agnostic)
    └── ...
```

### 2.2. Backend Abstraction (`src/backend/mod.rs`)

We will introduce a generic interface to decouple `scheduler/context.rs` from the specific kernel syscalls.

```rust
use crate::scheduler::tasks::IOEventTaskSlot;
use std::io;

/// Available Backend Types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Aio,
    Uring,
}

/// Helper to carry backend-specific data within a generic Task Slot.
/// - AIO needs `iocb` storage.
/// - io_uring usually just needs the `user_data` mapping (index), but might need space for specialized flags.
pub enum BackendSlotData {
    Aio(crate::backend::aio::iocb),
    Uring, // Zero-sized, as uring copies SQE data immediately
}

impl Default for BackendSlotData {
    fn default() -> Self {
        // Default to something safe, though this is usually initialized by the specific backend
        Self::Uring
    }
}

/// The core trait that Context uses to drive IO
pub trait IoBackend: Send + Sync {
    /// Return the type of this backend
    fn backend_type(&self) -> BackendType;

    /// Submit a list of slots. 
    /// The backend is responsible for extracting the necessary info from `slots`
    /// and submitting it to the kernel.
    fn submit(&self, slots: &mut [IOEventTaskSlot]) -> io::Result<usize>;

    /// Poll for completions.
    /// The backend should find completed tasks and update the corresponding `slots`.
    fn poll(&self, slots: &mut [IOEventTaskSlot], min: usize) -> io::Result<usize>;
}
```

### 2.3. Task Slot Refactoring (`src/scheduler/tasks.rs`)

The `IOEventTaskSlot` struct currently holds `aio::iocb`. This must be genericized.

```rust
pub struct IOEventTaskSlot<C: IOCallbackCustom> {
    // Replaces the direct `iocb` field
    pub(crate) backend_data: BackendSlotData,
    
    pub(crate) event: Option<Box<IOEvent<C>>>,
}
```

### 2.4. IO Context Updates (`src/scheduler/context.rs`)

- The `IOContext` struct will hold `Box<dyn IoBackend>` (or an Enum wrapper if we want to avoid dynamic dispatch, though the overhead is minimal per batch).
- `IOContext::new()` will accept a `Option<BackendType>`.
  - If `None`, it performs **Availability Detection**.

### 2.5. Availability Detection & Factory

In `src/backend/mod.rs`:

```rust
pub fn probe_uring() -> bool {
    // Attempt to create a minimal io_uring instance
    io_uring::IoUring::new(1).is_ok()
}

pub fn create_backend(depth: usize, type_: Option<BackendType>) -> io::Result<Box<dyn IoBackend>> {
    let t = type_.unwrap_or_else(|| {
        if probe_uring() { BackendType::Uring } else { BackendType::Aio }
    });

    match t {
        BackendType::Aio => Ok(Box::new(aio::AioBackend::new(depth)?)),
        BackendType::Uring => Ok(Box::new(uring::UringBackend::new(depth)?)),
    }
}
```

## 3. Implementation Details: io_uring

We will use the `io-uring` crate.

- **Submission**:
  - Iterate generic slots.
  - Convert `IOEvent` (fd, offset, buffer) to `io_uring::squeue::Entry`.
  - **Important**: Set `entry.user_data(slot_index)`.
  - `ring.submit()`.

- **Polling**:
  - `ring.submit_and_wait(min)`.
  - Iterate generic Completion Queue (`cqueue`).
  - Extract `user_data` -> `slot_index`.
  - Mark `slots[slot_index]` as complete (Success/Error).

## 4. Implementation Steps

1.  **Preparation**:
    - Add `io-uring` dependency. (Done)
    - Create `src/backend/` folder structure.
2.  **Migration**:
    - Move `scheduler/aio.rs` to `backend/aio.rs`.
    - Create `backend/mod.rs` with the `IoBackend` trait and `BackendSlotData` enum.
3.  **Refactoring**:
    - Modify `scheduler/tasks.rs` to use `BackendSlotData`.
    - Refactor `backend/aio.rs` to implement `IoBackend`.
    - Modify `scheduler/context.rs` to use the `IoBackend` trait instead of direct AIO calls.
4.  **Feature Add**:
    - Implement `backend/uring.rs` implementing `IoBackend`.
    - Implement the probe/factory logic in `backend/mod.rs`.
5.  **Testing**:
    - Verify existing tests pass with AIO.
    - Run tests with `uring` forced.
    - Run tests with auto-detection.

## 5. Review Checklist
- [ ] Directory structure separation (`src/backend` vs `src/scheduler`).
- [ ] Unified interface definition.
- [ ] Runtime detection of `io_uring` support.
- [ ] Minimal performance impact on the existing AIO path.