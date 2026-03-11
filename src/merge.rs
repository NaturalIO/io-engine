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
//!   - **Completion**: When the master event completes, it iterates over sub-tasks, sets their results (copying data for reads), and triggers their individual callbacks.
//!
//! ## Components
//! - [`MergeBuffer`]: Internal buffer logic.
//! - [`MergeSubmitter`]: Wraps a sender channel and manages the merge logic before sending.

use crate::tasks::{IOAction, IOCallback, IOEvent, IOEventMerged};
use crossfire::BlockingTxTrait;
use embed_collections::SegList;
use io_buffer::Buffer;
use nix::errno::Errno;
use std::io;
use std::os::fd::RawFd;

/// Info about the first event and merged state.
struct MergedInfo<C: IOCallback> {
    /// First event stored as Box<IOEvent> to allow reuse when merging.
    first_event: Box<IOEvent<C>>,
    /// Tail offset: next contiguous address that can be merged.
    tail_offset: i64,
    /// Total size of all events including the first.
    total_size: usize,
}

/// Buffers sequential IO events for merging.
///
/// This internal component collects [`IOEvent`]s,
/// presuming the same IO action and file descriptor (it does not check),
/// the merge upper bound is specified in `merge_size_limit`.
pub struct MergeBuffer<C: IOCallback> {
    pub merge_size_limit: usize,
    merged_info: Option<MergedInfo<C>>,
    /// Subsequent events stored as IOEventMerged for cache-friendly storage.
    merged_events: SegList<IOEventMerged<C>>,
}

impl<C: IOCallback> MergeBuffer<C> {
    /// Creates a new `MergeBuffer` with the specified merge size limit.
    ///
    /// # Arguments
    /// * `merge_size_limit` - The maximum total data size to produce a merged event.
    #[inline(always)]
    pub fn new(merge_size_limit: usize) -> Self {
        Self { merge_size_limit, merged_info: None, merged_events: SegList::new() }
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
        if let Some(ref info) = self.merged_info {
            if event.get_size() as usize > self.merge_size_limit {
                return false;
            }
            return info.tail_offset == event.offset;
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
    /// You should always check whether event is contiguous with [Self::may_add_event] before calling `push_event()`
    ///
    /// # Returns
    /// `true` if the buffer size has reached or exceeded `merge_size_limit` after adding the event, `false` otherwise.
    #[inline(always)]
    pub fn push_event(&mut self, event: IOEvent<C>) -> bool {
        if let Some(ref mut info) = self.merged_info {
            // Safety check: ensure may_add_event was called
            debug_assert_eq!(info.tail_offset, event.offset, "push_event: event not contiguous");
            debug_assert!(
                info.total_size + event.get_size() as usize <= self.merge_size_limit,
                "push_event: exceeds merge_size_limit"
            );
            // If this is the second event, move first event's buffer to merged_events
            if self.merged_events.is_empty() {
                let first_merged = info.first_event.extract_merged();
                self.merged_events.push(first_merged);
            }
            // Subsequent events: convert to IOEventMerged and store in SegList
            info.total_size += event.get_size() as usize;
            info.tail_offset += event.get_size() as i64;
            self.merged_events.push(event.into_merged());
            return info.total_size >= self.merge_size_limit;
        } else {
            // First event: store as Box<IOEvent> for potential reuse
            let size = event.get_size() as usize;
            let offset = event.offset;
            self.merged_info = Some(MergedInfo {
                first_event: Box::new(event),
                tail_offset: offset + size as i64,
                total_size: size,
            });
            return size >= self.merge_size_limit;
        }
    }

    /// Returns the number of events currently in the buffer.
    #[inline(always)]
    pub fn len(&self) -> usize {
        if !self.merged_events.is_empty() {
            // First event buffer moved to merged_events, count is merged_events.len()
            self.merged_events.len()
        } else {
            // Single event or empty
            self.merged_info.as_ref().map(|_| 1).unwrap_or(0)
        }
    }

    /// Takes all buffered events, building merged buffer if needed.
    /// Returns the master event (Box<IOEvent>) or None if empty.
    #[inline(always)]
    fn take(&mut self, action: IOAction) -> Option<Box<IOEvent<C>>> {
        let info = self.merged_info.take()?;

        // Single event: return directly without mem::replace
        if self.merged_events.is_empty() {
            return Some(info.first_event);
        }

        // Multiple events: take merged_events and build merged buffer
        let sub_tasks = std::mem::replace(&mut self.merged_events, SegList::new());
        debug_assert!(sub_tasks.len() > 1);
        let size = info.total_size;
        let offset = info.first_event.offset;

        match Buffer::aligned(size as i32) {
            Ok(mut buffer) => {
                if action == IOAction::Write {
                    let mut write_offset = 0;
                    for merged in sub_tasks.iter() {
                        buffer.copy_from(write_offset, merged.buf.as_ref());
                        write_offset += merged.buf.len();
                    }
                }

                // Reuse first_event as master, set merged buffer and subtasks
                let mut master = info.first_event;
                master.set_merged_tasks(buffer, sub_tasks);
                Some(master)
            }
            Err(_) => {
                // Allocation failed: error out all events
                for merged in sub_tasks.drain() {
                    if let Some(cb) = merged.cb {
                        cb.call(offset, Err(Errno::ENOMEM));
                    }
                }
                None
            }
        }
    }

    /// Flushes the buffered events, potentially merging them into a single [`IOEvent`].
    ///
    /// This method handles different scenarios based on the number of events in the buffer:
    /// - If the buffer is empty, it returns `None`.
    /// - If there is a single event, it returns `Some(event)` with the original event.
    /// - If there are multiple events, it attempts to merge them:
    ///   - If successful, reuses the first `Box<IOEvent>` as the master event, replacing its buffer.
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
        let mut master = self.take(action)?;
        master.set_fd(fd);
        Some(*master)
    }
}

/// Manages the submission of IO events, attempting to merge sequential events
/// before sending them to the IO driver.
///
/// This component buffers incoming [`IOEvent`]s into a [`MergeBuffer`].
/// It ensures that events for the same file descriptor and IO action are
/// considered for merging to optimize system calls.
pub struct MergeSubmitter<C: IOCallback, S: BlockingTxTrait<Box<IOEvent<C>>>> {
    fd: RawFd,
    buffer: MergeBuffer<C>,
    sender: S,
    action: IOAction,
}

impl<C: IOCallback, S: BlockingTxTrait<Box<IOEvent<C>>>> MergeSubmitter<C, S> {
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
    pub fn add_event(&mut self, mut event: IOEvent<C>) -> Result<(), io::Error> {
        log_debug_assert_eq!(self.fd, event.fd);
        log_debug_assert_eq!(event.action, self.action);
        let event_size = event.get_size();

        if event_size >= self.buffer.merge_size_limit as u64 || !self.buffer.may_add_event(&event) {
            if let Err(e) = self._flush() {
                event.set_error(Errno::ESHUTDOWN as i32);
                event.callback_unchecked(false);
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
                .send(Box::new(event))
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Queue closed"))?;
        }
        Ok(())
    }
}
