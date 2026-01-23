//! # IO Engine
//!
//! A high-performance asynchronous IO library for Linux, masking `AIO` and `io_uring`
//! interfaces behind a unified API.
//!
//! ## Architecture
//!
//! Key components:
//! - [IOContext`]: The main driver entry point. Manages submission and completion of IO events.
//! - [IOEvent](crate::tasks::IOEvent): Represents a single IO operation (Read/Write). Carries buffer, offset, fd.
//! - [IOCallback`]: Trait for handling completion. `ClosureCb` is provided for closure-based callbacks.
//! - [IOWorkers]: Worker threads handling completions.
//! - **IO Merging**: The engine supports merging sequential IO requests to reduce system call overhead. See the [`merge`] module for details.
//!
//! ## Callbacks
//!
//! The engine supports flexible callback mechanisms. You can use either closures or custom structs implementing the [IOCallback](crate::tasks::IOCallback) trait.
//!
//! ### Closure Callback
//!
//! Use `ClosureCb` to wrap a closure. This is convenient for simple logic or one-off tasks.
//! The closure takes ownership of the completed `IOEvent`.
//!
//! ### Struct Callback
//!
//! For more complex state management or to avoid allocation overhead of `Box<dyn Fn...>`,
//! you can define your own struct and implement `IOCallback`.
//! For multiple types of callback, you can use enum.
//!
//! ```rust
//! use io_engine::tasks::{IOCallback, IOEvent};
//!
//! struct MyCallback {
//!     id: u64,
//! }
//!
//! impl IOCallback for MyCallback {
//!     fn call(self, event: IOEvent<Self>) {
//!         if event.is_done() {
//!             println!("Operation {} completed, result len: {}", self.id, event.get_size());
//!         }
//!     }
//! }
//! ```
//!
//! ## Short Read/Write Handling
//!
//! The engine supports transparent handling of short reads and writes (partial IO).
//! This is achieved through the `IOEvent` structure which tracks the progress of the operation.
//!
//! - The `res` field in `IOEvent` (initialized to `i32::MIN`) stores the accumulated bytes transferred
//!   when `res >= 0`.
//! - When a driver (`aio` or `uring`) processes an event, it checks if `res >= 0`.
//! - If true, it treats the event as a continuation (retry) and adjusts the buffer pointer,
//!   length, and file offset based on the bytes already transferred.
//! - This allows the upper layers or callback mechanisms to re-submit incomplete events
//!   without manually slicing buffers or updating offsets.
//!
//! ## Usage Example (io_uring)
//!
//! ```rust
//! use io_engine::callback_worker::IOWorkers;
//! use io_engine::{IOContext, Driver};
//! use io_engine::tasks::{ClosureCb, IOAction, IOEvent};
//! use io_buffer::Buffer;
//! use std::fs::OpenOptions;
//! use std::os::fd::AsRawFd;
//! use crossfire::oneshot;
//!
//! fn main() {
//!     // 1. Prepare file
//!     let file = OpenOptions::new()
//!         .read(true)
//!         .write(true)
//!         .create(true)
//!         .open("/tmp/test_io_engine.data")
//!         .unwrap();
//!     let fd = file.as_raw_fd();
//!
//!     // 2. Create channels for submission
//!     // This channel is used to send events into the engine's submission queue
//!     let (tx, rx) = crossfire::mpsc::bounded_blocking(128);
//!
//!     // 3. Create IOContext (io_uring)
//!     // worker_num=1, depth=16
//!     // This spawns the necessary driver threads.
//!     let _ctx = IOContext::<ClosureCb, _>::new(
//!         16,
//!         rx,
//!         &IOWorkers::new(1),
//!         Driver::Uring
//!     ).expect("Failed to create context");
//!
//!     // 4. Submit a Write
//!     let mut buffer = Buffer::aligned(4096).unwrap();
//!     buffer[0] = 65; // 'A'
//!     let mut event = IOEvent::new(fd, buffer, IOAction::Write, 0);
//!
//!     // Create oneshot for this event's completion
//!     let (done_tx, done_rx) = oneshot::oneshot();
//!     event.set_callback(ClosureCb(Box::new(move |event| {
//!         let _ = done_tx.send(event);
//!     })));
//!
//!     // Send to engine
//!     tx.send(event).expect("submit");
//!
//!     // 5. Wait for completion
//!     let event = done_rx.recv().unwrap();
//!     assert!(event.is_done());
//!     event.get_write_result().expect("Write failed");
//!
//!     // 6. Submit a Read
//!     let buffer = Buffer::aligned(4096).unwrap();
//!     let mut event = IOEvent::new(fd, buffer, IOAction::Read, 0);
//!
//!     let (done_tx, done_rx) = oneshot::oneshot();
//!     event.set_callback(ClosureCb(Box::new(move |event| {
//!         let _ = done_tx.send(event);
//!     })));
//!
//!     tx.send(event).expect("submit");
//!
//!     let mut event = done_rx.recv().unwrap();
//!     let read_buf = event.get_read_result().expect("Read failed");
//!     assert_eq!(read_buf.len(), 4096);
//!     assert_eq!(read_buf[0], 65);
//! }
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate captains_log;

pub mod callback_worker;
mod context;
pub use context::{Driver, IOContext};
mod driver;
pub mod merge;
pub mod tasks;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
mod test;
