//! # IO Merging
//!
//! This module provides functionality to merge multiple sequential IO requests into a single larger request.
//!
//! ## Overview
//!
//! Merging IO requests can significantly improve performance by:
//! - Reducing the number of system calls (`io_uring_enter` or `io_submit`).
//! - Allowing larger sequential transfers which are often more efficient for storage devices.
//! - Reducing per-request overhead in the driver and completion handling.
//!
//! ## Mechanism
//!
//! The core component is [`MergeSubmitter`], which buffers incoming [`IOEvent`]s.
//!
//! - **Buffering**: Events are added to [`MergeBuffer`]. They are merged if they are:
//!   - Sequential (contiguous offsets).
//!   - Same IO action (Read/Write).
//!   - Same file descriptor.
//!   - Total size does not exceed `merge_size_limit`.
//!
//! - **Flushing**: When the buffer is full, the limit is reached, or `flush()` is called, the merged request is submitted.
//!
//! - **Sub-tasks**:
//!   - If events are merged, a new "master" [`IOEvent`] is created covering the entire range.
//!   - The original events are attached as `sub_tasks` (a linked list) to this master event.
//!   - **Write**: The data from individual buffers is copied into a single large aligned buffer.
//!   - **Read**: A large buffer is allocated for the master event. Upon completion, data is copied back to the individual event buffers.
//!   - **Completion**: When the master event completes, `callback_merged` (in `tasks.rs`) is invoked. It iterates over sub-tasks, sets their results (copying data for reads), and triggers their individual callbacks.
//!
//! ## Components
//! - [`MergeBuffer`]: Internal buffer logic.
//! - [`MergeSubmitter`]: Wraps a sender channel and manages the merge logic before sending.

use crate::tasks::{BufOrLen, IOAction, IOCallback, IOEvent, IOEvent_};
use crossfire::BlockingTxTrait;
use embed_collections::slist::SLinkedList;
use io_buffer::Buffer;
use libc;
use std::io;
use std::os::fd::RawFd;

/// Buffers sequential IO events for merging.
///
/// This internal component collects [`IOEvent`]s,
/// presuming the same IO action and file descriptor (it does not check),
/// the merge upper bound is specified in `merge_size_limit`.
pub struct MergeBuffer<C: IOCallback> {
    pub merge_size_limit: usize,
    merged_events: SLinkedList<Box<IOEvent_<C>>, ()>,
    merged_offset: i64,
    merged_data_size: usize,
}

impl<C: IOCallback> MergeBuffer<C> {
    /// Creates a new `MergeBuffer` with the specified merge size limit.
    ///
    /// # Arguments
    /// * `merge_size_limit` - The maximum total data size to produce a merged event.
    #[inline(always)]
    pub fn new(merge_size_limit: usize) -> Self {
        Self {
            merge_size_limit,
            merged_events: SLinkedList::new(),
            merged_offset: -1,
            merged_data_size: 0,
        }
    }

    /// Checks if a new event can be added to the current buffer for merging.
    ///
    /// An event can be added if:
    /// - The buffer is empty.
    /// - The event is contiguous with the last event in the buffer.
    /// - Adding the event does not exceed the `merge_size_limit`.
    ///
    /// # Arguments
    /// * `event` - The [`IOEvent`] to check.
    ///
    /// # Returns
    /// `true` if the event can be added, `false` otherwise.
    #[inline(always)]
    pub fn may_add_event(&mut self, event: &IOEvent<C>) -> bool {
        if !self.merged_events.is_empty() {
            if self.merged_data_size + event.get_size() > self.merge_size_limit {
                return false;
            }
            return self.merged_offset + self.merged_data_size as i64 == event.offset;
        } else {
            return true;
        }
    }

    /// Pushes an event into the buffer.
    ///
    /// This method assumes that `may_add_event` has already been called and returned `true`.
    /// It updates the merged data size and tracks the merged offset.
    ///
    /// # Arguments
    /// * `event` - The [`IOEvent`] to push.
    ///
    /// # Safety
    ///
    /// You should always check whether event is contiguous with [may_add_event] before calling `push_event()`
    ///
    /// # Returns
    /// `true` if the buffer size has reached or exceeded `merge_size_limit` after adding the event, `false` otherwise.
    #[inline(always)]
    pub fn push_event(&mut self, event: IOEvent<C>) -> bool {
        if self.merged_events.is_empty() {
            self.merged_offset = event.offset;
        }
        self.merged_data_size += event.get_size();
        event.push_to_list(&mut self.merged_events);

        return self.merged_data_size >= self.merge_size_limit;
    }

    /// Returns the number of events currently in the buffer.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.merged_events.len()
    }

    /// Takes all buffered events and their merging metadata, resetting the buffer.
    ///
    /// This is an internal helper method.
    ///
    /// # Returns
    /// A tuple containing:
    /// - The `SLinkedList` of buffered events.
    /// - The starting offset of the merged events.
    /// - The total data size of the merged events.
    #[inline(always)]
    fn take(&mut self) -> (SLinkedList<Box<IOEvent_<C>>, ()>, i64, usize) {
        // Move the list content out by swapping with empty new list
        let tasks = std::mem::replace(&mut self.merged_events, SLinkedList::new());
        let merged_data_size = self.merged_data_size;
        let merged_offset = self.merged_offset;
        self.merged_offset = -1;
        self.merged_data_size = 0;
        (tasks, merged_offset, merged_data_size)
    }

    /// Flushes the buffered events, potentially merging them into a single [`IOEvent`].
    ///
    /// This method handles different scenarios based on the number of events in the buffer:
    /// - If the buffer is empty, it returns `None`.
    /// - If there is a single event, it returns `Some(event)` with the original event.
    /// - If there are multiple events, it attempts to merge them:
    ///   - If successful, a new master [`IOEvent`] covering the merged range is returned as `Some(merged_event)`.
    ///   - If buffer allocation for the merged event fails, all original events are marked with an `ENOMEM` error and their callbacks are triggered, then `None` is returned.
    /// - This function will always override fd in IOEvent with argument
    ///
    /// After flushing, the buffer is reset.
    ///
    /// # Arguments
    /// * `fd` - The raw file descriptor associated with the IO operations.
    /// * `action` - The IO action (Read/Write) for the events.
    ///
    /// # Returns
    /// An `Option<IOEvent<C>>` representing the merged event, a single original event, or `None` if the buffer was empty or merging failed.
    #[inline]
    pub fn flush(&mut self, fd: RawFd, action: IOAction) -> Option<IOEvent<C>> {
        let batch_len = self.len();
        if batch_len == 0 {
            return None;
        }
        if batch_len == 1 {
            self.merged_offset = -1;
            self.merged_data_size = 0;
            let mut event = IOEvent::pop_from_list(&mut self.merged_events).unwrap();
            // NOTE: always reset fd, allow false fd while adding
            event.set_fd(fd);
            return Some(event);
        }
        let (mut tasks, offset, size) = self.take();
        log_assert!(size > 0);

        match Buffer::aligned(size as i32) {
            Ok(mut buffer) => {
                if action == IOAction::Write {
                    let mut _offset = 0;
                    for event_box in tasks.iter() {
                        if let BufOrLen::Buffer(b) = &event_box.buf_or_len {
                            let _size = b.len();
                            buffer.copy_from(_offset, b.as_ref());
                            _offset += _size;
                        }
                    }
                }
                let mut event = IOEvent::<C>::new(fd, buffer, action, offset);
                event.set_subtasks(tasks);
                Some(event)
            }
            Err(e) => {
                warn!("mio: alloc buffer size {} failed: {}", size, e);
                while let Some(mut event) = IOEvent::<C>::pop_from_list(&mut tasks) {
                    event.set_error(libc::ENOMEM);
                    event.callback();
                }
                None
            }
        }
    }
}

/// Manages the submission of IO events, attempting to merge sequential events
/// before sending them to the IO driver.
///
/// This component buffers incoming [`IOEvent`]s into a [`MergeBuffer`].
/// It ensures that events for the same file descriptor and IO action are
/// considered for merging to optimize system calls.
pub struct MergeSubmitter<C: IOCallback, S: BlockingTxTrait<IOEvent<C>>> {
    fd: RawFd,
    buffer: MergeBuffer<C>,
    sender: S,
    action: IOAction,
}

impl<C: IOCallback, S: BlockingTxTrait<IOEvent<C>>> MergeSubmitter<C, S> {
    /// Creates a new `MergeSubmitter`.
    ///
    /// # Arguments
    /// * `fd` - The raw file descriptor for IO operations.
    /// * `sender` - A channel sender to send prepared [`IOEvent`]s to the IO driver.
    /// * `merge_size_limit` - The maximum data size for a merged event buffer.
    /// * `action` - The primary IO action (Read/Write) for this submitter.
    pub fn new(fd: RawFd, sender: S, merge_size_limit: usize, action: IOAction) -> Self {
        log_assert!(merge_size_limit > 0);
        Self { fd, buffer: MergeBuffer::<C>::new(merge_size_limit), sender, action }
    }

    /// Adds an [`IOEvent`] to the internal buffer, potentially triggering a flush.
    ///
    /// If the event cannot be merged with current buffered events (e.g., non-contiguous,
    /// exceeding merge limit), the existing buffered events are flushed first.
    /// If adding the new event fills the buffer to its `merge_size_limit`, a flush is also triggered.
    ///
    /// # Arguments
    /// * `event` - The [`IOEvent`] to add.
    ///
    /// # Returns
    /// An `Ok(())` on success, or an `io::Error` if flushing fails.
    /// On debug mode, will validate event.fd and event.action.
    pub fn add_event(&mut self, event: IOEvent<C>) -> Result<(), io::Error> {
        log_debug_assert_eq!(self.fd, event.fd);
        log_debug_assert_eq!(event.action, self.action);
        let event_size = event.get_size();

        if event_size >= self.buffer.merge_size_limit || !self.buffer.may_add_event(&event) {
            if let Err(e) = self._flush() {
                event.callback();
                return Err(e);
            }
        }
        if self.buffer.push_event(event) {
            self._flush()?;
        }
        return Ok(());
    }

    /// Explicitly flushes any pending buffered events to the IO driver.
    ///
    /// # Returns
    /// An `Ok(())` on success, or an `io::Error` if sending the flushed event fails.
    pub fn flush(&mut self) -> Result<(), io::Error> {
        self._flush()
    }

    #[inline(always)]
    fn _flush(&mut self) -> Result<(), io::Error> {
        if let Some(event) = self.buffer.flush(self.fd, self.action) {
            trace!("mio: submit event from flush {:?}", event);
            self.sender
                .send(event)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Queue closed"))?;
        }
        Ok(())
    }
}
