//! # IO Engine
//!
//! A high-performance asynchronous IO library for Linux, masking `AIO` and `io_uring`
//! interfaces behind a unified API.
//!
//! ## Architecture
//!
//! Key components:
//! - [IOEvent](crate::tasks::IOEvent): Represents a single IO operation (Read/Write). Carries buffer, offset, fd.
//! - [IOCallback](crate::tasks::IOCallback): Trait for handling completion. `ClosureCb` is provided for closure-based callbacks.
//! - [Worker](crate::callback_worker::Worker): Trait for workers handling completions.
//! - [IOWorkers](crate::callback_worker::IOWorkers): Worker threads handling completions (implements `Worker`).
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
//! use nix::errno::Errno;
//!
//! struct MyCallback {
//!     id: u64,
//! }
//!
//! impl IOCallback for MyCallback {
//!     fn call(self, _offset: i64, res: Result<Option<io_buffer::Buffer>, Errno>) {
//!         match res {
//!             Ok(Some(buf)) => println!("Operation {} completed, buffer len: {}", self.id, buf.len()),
//!             Ok(None) => println!("Operation {} completed (no buffer)", self.id),
//!             Err(e) => println!("Operation {} failed, error: {}", self.id, e),
//!         }
//!     }
//! }
//! ```
//!
//! ## Short Read/Write Handling
//!
//! The engine supports transparent handling of short reads and writes (partial IO).
//! When a read or write operation completes, the callback worker automatically adjusts
//! the buffer length to reflect the actual bytes transferred.
//!
//! ### How It Works
//!
//! - When an IO operation completes, the `callback_unchecked` method adjusts `Buffer::len()`
//!   to match the actual bytes transferred (`res`).
//! - For read operations, this means the buffer contains exactly the data that was read.
//! - For write operations, the buffer length is also adjusted to reflect completion status.
//!
//! ### Callback Worker Implementation
//!
//! When implementing a custom callback worker, you should use `callback_unchecked` which detect
//! short I/O and change reading Buffer length to exact copied bytes.
//!
//! ```rust,ignore
//! // In your callback worker thread
//! loop {
//!     match rx.recv() {
//!         Ok(event) => event.callback_unchecked(true),
//!         Err(_) => break,
//!     }
//! }
//! ```
//!
//! ### Advanced: Detecting Short I/O with File Boundary Check
//!
//! The `callback` method accepts a closure that allows you to detect if short I/O
//! is due to reaching the file end (which is normal) or an actual error condition
//! that requires retry:
//!
//! ```rust,ignore
//! // check_short_read returns true if offset exceeds file end
//! event.callback(|offset| {
//!         // NOTE: you should probably use weak reference here
//!         offset < file_size
//!     })
//!     .unwrap_or_else(|event| {
//!         // Short I/O detected, resubmit the event
//!         queue_tx.send(event).unwrap();
//!     });
//! ```
//!
//! The closure receives the current offset and should return `true` if the offset
//! exceeds the file boundary (indicating EOF). If the closure returns `true`,
//! the short I/O is considered an error condition that may need retry.
//!
//! ## Usage Example (io_uring)
//!
//! ```rust
//! use io_engine::callback_worker::IOWorkers;
//! use io_engine::{setup, Driver};
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
//!     // 3. Setup the driver (io_uring)
//!     // worker_num=1, depth=16
//!     // This spawns the necessary driver threads.
//!     setup::<ClosureCb, _, _>(
//!         16,
//!         rx,
//!         IOWorkers::new(1),
//!         Driver::Uring
//!     ).expect("Failed to setup driver");
//!
//!     // 4. Submit a Write
//!     let mut buffer = Buffer::aligned(4096).unwrap();
//!     buffer[0] = 65; // 'A'
//!     let mut event = IOEvent::new(fd, buffer, IOAction::Write, 0);
//!
//!     // Create oneshot for this event's completion
//!     let (done_tx, done_rx) = oneshot::oneshot();
//!     event.set_callback(ClosureCb(Box::new(move |_offset, res| {
//!         let _ = done_tx.send(res);
//!     })));
//!
//!     // Send to engine
//!     tx.send(Box::new(event)).expect("submit");
//!
//!     // 5. Wait for completion
//!     let res = done_rx.recv().unwrap();
//!     res.map_err(|_| "Write failed").expect("Write failed");
//!
//!     // 6. Submit a Read
//!     let buffer = Buffer::aligned(4096).unwrap();
//!     let mut event = IOEvent::new(fd, buffer, IOAction::Read, 0);
//!
//!     let (done_tx, done_rx) = oneshot::oneshot();
//!     event.set_callback(ClosureCb(Box::new(move |_offset, res| {
//!         let _ = done_tx.send(res);
//!     })));
//!
//!     tx.send(Box::new(event)).expect("submit");
//!
//!     let res = done_rx.recv().unwrap();
//!     let read_buf = res.expect("Read failed");
//!     assert!(read_buf.is_some());
//!     let read_buf = read_buf.unwrap();
//!     assert_eq!(read_buf.len(), 4096);
//!     assert_eq!(read_buf[0], 65);
//! }
//! ```

#[macro_use]
extern crate captains_log;

pub mod callback_worker;
mod context;
pub use context::{Driver, setup};
mod driver;
pub mod merge;
pub mod tasks;

#[cfg(test)]
mod test;
