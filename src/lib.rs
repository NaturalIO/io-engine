//! # IO Engine
//!
//! A high-performance asynchronous IO library for Linux, masking `AIO` and `io_uring`
//! interfaces behind a unified API.
//!
//! ## Architecture
//!
//! Key components:
//! - [IOEvent]:
//!   - Represents a single IO operation (Read/Write). Carries buffer, offset, fd.
//!   - Because IOEvent is large (>=64B), you should submit `Box<IOEvent<_>>` through channel
//! - [CbArgs]: Optional completion arguments along with IOEvent.
//! - [Worker]: Trait for workers handling completions:
//!   - Inline closure [InlineClosure]
//!   - Inline function
//!   - Send the complete IOEvent through spsc, mpsc, mpmc channel sender
//! - **IO Merging**: The engine supports merging sequential IO requests to reduce system call overhead. See the [`merge`] module for details.
//!
//! ## Callbacks
//!
//! The engine supports flexible callback mechanisms. You may:
//! - Capture some global arguments inside closure of callback workers
//! - Pass arguments with IOEvent with [IOEvent::set_args()]
//!
//! ### Example (with WaitGroupGuard as CbArgs)
//!
//! ```rust,no_run
//! use io_engine::{InlineClosure, Driver, setup, IOAction, IOEvent};
//! use crossfire::{mpsc, waitgroup::{WaitGroup, WaitGroupGuard}};
//! use io_buffer::{Buffer, rand_buffer};
//! use rustix::io::Errno;
//!
//! let (tx, rx) = mpsc::bounded_blocking(128);
//!
//! // Use a shared closure that can be updated for different test phases
//! let worker = InlineClosure(Box::new(move |_guard: WaitGroupGuard<()>, offset, res| {}));
//! // WaitGroupGuard has impl CbArgs trait
//! setup::<WaitGroupGuard<()>, _, _>(128, rx, worker, Driver::Uring).unwrap();
//! let wg = WaitGroup::new((), 0);
//! let mut buf = Buffer::aligned(4096i32).unwrap();
//! rand_buffer(&mut buf);
//! let fd = todo!("init your fd");
//! let mut event = IOEvent::new(fd, buf, IOAction::Write, 0);
//! event.set_args(wg.add_guard());
//! let _ = tx.send(Box::new(event));
//! ```
//!
//! ## Short Read/Write Handling
//!
//! The engine supports transparent handling of short reads and writes (partial IO).
//! When a read or write operation completes, the callback worker can use `callback_unchecked`
//! to automatically adjust the buffer length to reflect the actual bytes transferred.
//!
//! ### How It Works
//!
//! - The [IOEvent::callback] method accepts a closure that allows you to detect if short I/O
//!   is due to reaching the file end (which is normal) or an actual error condition
//!   that requires retry.
//! - Optionally, you can ignore all short I/O by calling [IOEvent::callback_unchecked], which will
//!   adjust Buffer into the length actually read/write
//!
//! ## Example (TxOneshot as CbArgs and Short I/O handling)
//!
//! ```rust
//! use io_engine::{setup, Driver, IOAction, IOEvent, CbArgs};
//! use io_buffer::{Buffer, rand_buffer};
//! use std::fs::OpenOptions;
//! use std::os::fd::AsRawFd;
//! use std::thread;
//! use rustix::io::Errno;
//! use crossfire::{mpsc, oneshot, MTx};
//!
//! struct OneshotArg (oneshot::TxOneshot<Result<Option<Buffer>, Errno>>);
//!
//! impl CbArgs for OneshotArg {}
//!
//! // 1. Prepare file
//! let file = OpenOptions::new()
//!     .read(true)
//!     .write(true)
//!     .create(true)
//!     .open("/tmp/test_io_engine.data")
//!     .unwrap();
//! let fd = file.as_raw_fd();
//!
//!
//! // 2. Create channels for submission
//! let (submit_tx, submit_rx) = mpsc::bounded_blocking(128);
//!
//! // 3. Setup the driver (io_uring) with a channel worker
//! // This example uses a channel to wait for completion.
//! let (done_tx, done_rx) = crossfire::mpsc::bounded_blocking(128);
//! /// By default `MTx<_>` has impl Worker trait
//! setup::<OneshotArg, _, _>(
//!     16,
//!     submit_rx,
//!     done_tx, // Sender implements Worker
//!     Driver::Uring
//! ).expect("Failed to setup driver");
//!
//! // example for short-io handling
//! let weak_tx = submit_tx.downgrade();
//! let file_size = 4096;
//!
//! // spawn callback worker
//! let th = thread::spawn(move || {
//!     let cb = | done_tx: OneshotArg, _offset, res| {
//!         done_tx.0.send(res);
//!     };
//!     while let Ok(event) = done_rx.recv() {
//!         if let Err(short_event) = event.callback(
//!             // for simplicity, we just demonstrate checking a static file bound here
//!             | offset | offset < file_size,
//!             &cb,
//!         ) {
//!             if let Some(submit_tx) = weak_tx.upgrade::<MTx<mpsc::Array<Box<IOEvent<OneshotArg>>>>>() {
//!                 submit_tx.send(short_event).expect("resubmit");
//!             } else {
//!                 // submit tx already dropped, does not matter.
//!                 short_event.callback_unchecked(&cb);
//!             }
//!         }
//!     }
//! });

//!
//! // 4. Submit a Write
//! let mut buf = Buffer::aligned(4096).unwrap();
//! rand_buffer(&mut buf);
//! let written_buf = buf.clone();
//! let mut event = IOEvent::new(fd, buf, IOAction::Write, 0);
//! let (task_tx, task_rx) = oneshot::oneshot();
//! event.set_args(OneshotArg(task_tx));
//!
//! // Send to engine
//! submit_tx.send(Box::new(event)).expect("submit");
//!
//! // Wait for completion
//! let res: Result<Option<Buffer>, Errno> = task_rx.recv().unwrap_or(Err(Errno::SHUTDOWN));
//! assert!(res.is_ok());
//!
//! // 5. Submit a Read
//! let buffer = Buffer::aligned(4096).unwrap();
//! let mut event = IOEvent::new(fd, buffer, IOAction::Read, 0);
//! let (task_tx, task_rx) = oneshot::oneshot();
//! event.set_args(OneshotArg(task_tx));
//! submit_tx.send(Box::new(event)).expect("submit");
//!
//! let res: Result<Option<Buffer>, Errno> = task_rx.recv().unwrap_or(Err(Errno::SHUTDOWN));
//! let read_buf = res.expect("ok").unwrap();
//! assert_eq!(read_buf.len(), 4096);
//! assert_eq!(&read_buf[..], &written_buf[..]);
//! drop(submit_tx);
//! th.join();
//! // callback worker exited
//! ```

#[macro_use]
extern crate captains_log;

mod callback_worker;
pub use callback_worker::{InlineClosure, Worker};
mod context;
pub use context::{Driver, setup};
mod driver;
pub mod merge;
mod tasks;
pub use tasks::{CbArgs, IOAction, IOEvent};

#[cfg(test)]
mod test;
