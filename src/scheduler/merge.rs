/*
Copyright (c) NaturalIO Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use super::embedded_list::*;
use std::io;
use std::mem::transmute;
use std::os::fd::RawFd;
use std::sync::Arc;

use super::{
    context::{IOChannelType, IOContext},
    tasks::*,
};
use crate::buffer::Buffer;

pub struct EventMergeBuffer<C: IOCallbackCustom> {
    pub merge_size_limit: usize,
    last_event: Option<Box<IOEvent<C>>>,
    merged_events: Option<EmbeddedList>,
    merged_offset: i64,
    merged_data_size: usize,
    merged_count: usize,
    list_node_offset: usize,
}

impl<C: IOCallbackCustom> EventMergeBuffer<C> {
    pub fn new(merge_size_limit: usize) -> Self {
        Self {
            merge_size_limit,
            last_event: None,
            merged_count: 0,
            merged_events: None,
            merged_offset: -1,
            merged_data_size: 0,
            list_node_offset: std::mem::offset_of!(IOEvent<C>, node),
        }
    }

    #[inline(always)]
    pub fn may_add_event(&mut self, event: &Box<IOEvent<C>>) -> bool {
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
    pub fn push_event(&mut self, mut event: Box<IOEvent<C>>) -> bool {
        if self.merged_count == 0 {
            self.merged_offset = event.offset;
            self.merged_data_size = event.get_size();
            self.last_event = Some(event);
            self.merged_count = 1;
            return false;
        } else {
            self.merged_data_size += event.get_size();
            if self.merged_count == 1 {
                let mut last_event = self.last_event.take().unwrap();
                let mut list = EmbeddedList::new(self.list_node_offset);
                list.push_back(&mut last_event.node);
                let _ = Box::leak(last_event);
                list.push_back(&mut event.node);
                let _ = Box::leak(event);
                self.merged_events = Some(list);
            } else {
                let merged_events = self.merged_events.as_mut().unwrap();
                merged_events.push_back(&mut event.node);
                let _ = Box::leak(event);
            }
            self.merged_count += 1;
            return self.merged_data_size >= self.merge_size_limit;
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.merged_count
    }

    #[inline(always)]
    pub fn take(&mut self) -> (EmbeddedList, i64, usize) {
        log_debug_assert!(self.merged_count > 1, "merged_count {}", self.merged_count);
        let tasks = self.merged_events.take().unwrap();
        let merged_data_size = self.merged_data_size;
        let merged_offset = self.merged_offset;
        self.merged_offset = -1;
        self.merged_data_size = 0;
        self.merged_count = 0;
        (tasks, merged_offset, merged_data_size)
    }

    #[inline(always)]
    pub fn take_one(&mut self) -> Box<IOEvent<C>> {
        log_debug_assert_eq!(self.merged_count, 1);
        self.merged_offset = -1;
        self.merged_data_size = 0;
        self.merged_count = 0;
        self.last_event.take().unwrap()
    }
}

/// Try to merge sequential IOEvent.
///
/// NOTE: Assuming all the IOEvents are of the same file, only debug mode will do validation.
pub struct IOMergeSubmitter<C: IOCallbackCustom> {
    fd: RawFd,
    buffer: EventMergeBuffer<C>,
    ctx: Arc<IOContext<C>>,
    action: IOAction,
    channel_type: IOChannelType,
}

impl<C: IOCallbackCustom> IOMergeSubmitter<C> {
    pub fn new(
        fd: RawFd,
        ctx: Arc<IOContext<C>>,
        merge_size_limit: usize,
        action: IOAction,
        channel_type: IOChannelType,
    ) -> Self {
        log_assert!(merge_size_limit > 0);
        match action {
            IOAction::Read => {
                assert_eq!(channel_type, IOChannelType::Read);
            }
            IOAction::Write => {
                assert!(channel_type != IOChannelType::Read);
            }
        }
        Self {
            fd,
            buffer: EventMergeBuffer::new(merge_size_limit),
            ctx,
            action,
            channel_type,
        }
    }

    /// On debug mode, will validate event.fd and event.action.
    pub fn add_event(&mut self, event: Box<IOEvent<C>>) -> Result<(), io::Error> {
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
            self.ctx.submit(submit_event, self.channel_type)?;
        } else {
            let (mut events, offset, size) = self.buffer.take();
            log_assert!(size > 0);
            match Buffer::aligned(size) {
                Ok(mut buffer) => {
                    trace!(
                        "mio: merged {} ev into {} @{}",
                        events.get_length(),
                        size,
                        offset
                    );
                    if self.action == IOAction::Write {
                        let mut _offset = 0;
                        for _event in events.iter::<IOEvent<C>>() {
                            let event: &IOEvent<C> = unsafe { transmute(_event) };
                            let b = event.get_buf_ref();
                            let _size = b.len();
                            buffer.copy_from(_offset, b);
                            _offset += _size;
                        }
                    }
                    let mut event = IOEvent::<C>::new(self.fd, buffer, self.action, offset);
                    event.set_subtasks(events);
                    self.ctx.submit(event, self.channel_type)?;
                }
                Err(_) => {
                    // commit seperately
                    warn!("mio: alloc buffer size {} failed", size);
                    let mut e: Option<io::Error> = None;
                    loop {
                        let event = {
                            // to mask raw pointer from async scope
                            if let Some(_event) = events.pop_front::<IOEvent<C>>() {
                                Some(unsafe { Box::from_raw(_event) })
                            } else {
                                None
                            }
                        };
                        if let Some(_event) = event {
                            if let Err(_e) = self.ctx.submit(_event, self.channel_type) {
                                e.replace(_e);
                            }
                        } else {
                            break;
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
