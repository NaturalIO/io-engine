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
//! The core component is [`IOMergeSubmitter`], which buffers incoming [`IOEvent`]s.
//!
//! - **Buffering**: Events are added to [`EventMergeBuffer`]. They are merged if they are:
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
//! - [`EventMergeBuffer`]: Internal buffer logic.
//! - [`IOMergeSubmitter`]: Wraps a sender channel and manages the merge logic before sending.

use crate::tasks::*;
use crossfire::BlockingTxTrait;
use embed_collections::dlist::DLinkedList;
use io_buffer::Buffer;
use std::io;
use std::os::fd::RawFd;

pub struct EventMergeBuffer<C: IOCallback> {
    pub merge_size_limit: usize,
    merged_events: DLinkedList<Box<IOEvent_<C>>, ()>,
    merged_offset: i64,
    merged_data_size: usize,
    merged_count: usize,
}

impl<C: IOCallback> EventMergeBuffer<C> {
    pub fn new(merge_size_limit: usize) -> Self {
        Self {
            merge_size_limit,
            merged_count: 0,
            merged_events: DLinkedList::new(),
            merged_offset: -1,
            merged_data_size: 0,
        }
    }

    #[inline(always)]
    pub fn may_add_event(&mut self, event: &IOEvent<C>) -> bool {
        if self.merged_count > 0 {
            if self.merged_data_size + event.get_size() > self.merge_size_limit {
                return false;
            }
            return self.merged_offset + self.merged_data_size as i64 == event.offset;
        } else {
            return true;
        }
    }

    // return full
    #[inline(always)]
    pub fn push_event(&mut self, event: IOEvent<C>) -> bool {
        if self.merged_count == 0 {
            self.merged_offset = event.offset;
        }
        self.merged_data_size += event.get_size();
        self.merged_count += 1;
        event.push_to_list(&mut self.merged_events);

        return self.merged_data_size >= self.merge_size_limit;
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.merged_count
    }

    #[inline(always)]
    pub fn take(&mut self) -> (DLinkedList<Box<IOEvent_<C>>, ()>, i64, usize) {
        log_debug_assert!(self.merged_count > 1, "merged_count {}", self.merged_count);
        // Move the list content out by swapping with empty new list
        let tasks = std::mem::replace(&mut self.merged_events, DLinkedList::new());
        let merged_data_size = self.merged_data_size;
        let merged_offset = self.merged_offset;

        self.merged_offset = -1;
        self.merged_data_size = 0;
        self.merged_count = 0;
        (tasks, merged_offset, merged_data_size)
    }

    #[inline(always)]
    pub fn take_one(&mut self) -> IOEvent<C> {
        log_debug_assert_eq!(self.merged_count, 1);
        self.merged_offset = -1;
        self.merged_data_size = 0;
        self.merged_count = 0;

        IOEvent::pop_from_list(&mut self.merged_events).expect("Should have one event")
    }
}

/// Try to merge sequential IOEvent.
///
/// NOTE: Assuming all the IOEvents are of the same file, only debug mode will do validation.
pub struct IOMergeSubmitter<C: IOCallback, S: BlockingTxTrait<IOEvent<C>>> {
    fd: RawFd,
    buffer: EventMergeBuffer<C>,
    sender: S,
    action: IOAction,
}

impl<C: IOCallback, S: BlockingTxTrait<IOEvent<C>>> IOMergeSubmitter<C, S> {
    pub fn new(fd: RawFd, sender: S, merge_size_limit: usize, action: IOAction) -> Self {
        log_assert!(merge_size_limit > 0);
        Self { fd, buffer: EventMergeBuffer::<C>::new(merge_size_limit), sender, action }
    }

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

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self._flush()
    }

    #[inline(always)]
    fn _flush(&mut self) -> Result<(), io::Error> {
        let batch_len = self.buffer.len();
        if batch_len == 0 {
            return Ok(());
        }
        if batch_len == 1 {
            let submit_event = self.buffer.take_one();
            trace!("mio: submit {:?} not merged", submit_event);
            self.sender
                .send(submit_event)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Queue closed"))?;
        } else {
            let (mut events, offset, size) = self.buffer.take();
            log_assert!(size > 0);

            match Buffer::aligned(size as i32) {
                Ok(mut buffer) => {
                    // trace!("mio: merged {} ev into {} @{}", events.len(), size, offset);
                    if self.action == IOAction::Write {
                        let mut _offset = 0;
                        // Iterate references safely
                        for event_box in events.iter() {
                            // event_box is &IOEvent<C>>
                            let b = event_box.buf.as_ref().unwrap().as_ref();
                            let _size = b.len();
                            buffer.copy_from(_offset, b);
                            _offset += _size;
                        }
                    }
                    let mut event = IOEvent::<C>::new(self.fd, buffer, self.action, offset);
                    event.set_subtasks(events);
                    self.sender
                        .send(event)
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Queue closed"))?;
                }
                Err(_) => {
                    // commit separately
                    warn!("mio: alloc buffer size {} failed", size);
                    let mut e: Option<io::Error> = None;
                    while let Some(event) = IOEvent::<C>::pop_from_list(&mut events) {
                        if let Err(_e) = self
                            .sender
                            .send(event)
                            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Queue closed"))
                        {
                            e.replace(_e);
                        }
                    }
                    if let Some(_e) = e {
                        return Err(_e);
                    }
                }
            }
        }
        Ok(())
    }
}
