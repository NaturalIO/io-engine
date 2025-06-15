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

use std::os::fd::RawFd;
use std::{
    fmt,
    sync::atomic::{AtomicI32, Ordering},
};

use nix::errno::Errno;

use super::embedded_list::*;
use super::{aio, callback_worker::*};
use crate::buffer::Buffer;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum IOAction {
    Read = 0,
    Write = 1,
}

/// Define your callback with this trait
pub trait IOCallbackCustom: Sized + 'static + Send + Unpin {
    fn call(self, _event: Box<IOEvent<Self>>);
}

/// Closure callback for IOEvent
pub struct ClosureCb(pub Box<dyn FnOnce(Box<IOEvent<Self>>) + Send + Sync + 'static>);

impl IOCallbackCustom for ClosureCb {
    fn call(self, event: Box<IOEvent<Self>>) {
        (self.0)(event)
    }
}

// Carries the infomation of read/write event
#[repr(C)]
pub struct IOEvent<C: IOCallbackCustom> {
    /// make sure EmbeddedListNode always in the front.
    /// This is for putting sub_tasks in the link list, without additional allocation.
    pub(crate) node: EmbeddedListNode,
    pub buf: Option<Buffer>,
    pub offset: i64,
    pub action: IOAction,
    pub fd: RawFd,
    res: AtomicI32,
    cb: Option<C>,
    sub_tasks: Option<EmbeddedList>,
}

impl<C: IOCallbackCustom> fmt::Debug for IOEvent<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(sub_tasks) = self.sub_tasks.as_ref() {
            write!(
                f,
                "offset={} {:?} sub_tasks {} ",
                self.offset,
                self.action,
                sub_tasks.get_length()
            )
        } else {
            write!(f, "offset={} {:?}", self.offset, self.action)
        }
    }
}

impl<C: IOCallbackCustom> IOEvent<C> {
    #[inline]
    pub fn new(fd: RawFd, buf: Buffer, action: IOAction, offset: i64) -> Box<Self> {
        log_assert!(
            buf.len() > 0,
            "{:?} offset={}, buffer size == 0",
            action,
            offset
        );
        Box::new(Self {
            buf: Some(buf),
            fd,
            action,
            offset,
            res: AtomicI32::new(0),
            cb: None,
            sub_tasks: None,
            node: Default::default(),
        })
    }

    /// Set callback for IOEvent, might be closure or a custom struct
    #[inline(always)]
    pub fn set_callback(&mut self, cb: C) {
        self.cb = Some(cb);
    }

    #[inline(always)]
    pub fn get_size(&self) -> usize {
        self.buf.as_ref().unwrap().len()
    }

    #[inline(always)]
    pub fn push_to_list(mut self: Box<Self>, events: &mut EmbeddedList) {
        events.push_back(&mut self.node);
        let _ = Box::leak(self);
    }

    #[inline(always)]
    pub fn pop_from_list(events: &mut EmbeddedList) -> Option<Box<Self>> {
        if let Some(event) = events.pop_front::<Self>() {
            Some(unsafe { Box::from_raw(event) })
        } else {
            None
        }
    }

    #[inline(always)]
    pub(crate) fn set_subtasks(&mut self, sub_tasks: EmbeddedList) {
        self.sub_tasks = Some(sub_tasks)
    }

    #[inline(always)]
    pub fn get_buf_ref<'a>(&'a self) -> &'a [u8] {
        self.buf.as_ref().unwrap().as_ref()
    }

    #[inline(always)]
    pub fn is_done(&self) -> bool {
        self.res.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn get_result(&mut self) -> Result<Buffer, Errno> {
        let res = self.res.load(Ordering::Acquire);
        if res > 0 {
            return Ok(self.buf.take().unwrap());
        } else if res == 0 {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(Errno::from_raw(-res));
        }
    }

    #[inline(always)]
    pub fn _get_result(&mut self) -> Result<Buffer, i32> {
        let res = self.res.load(Ordering::Acquire);
        if res > 0 {
            return Ok(self.buf.take().unwrap());
        } else if res == 0 {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(res);
        }
    }

    #[inline(always)]
    pub(crate) fn set_error(&self, mut errno: i32) {
        if errno == 0 {
            // XXX: EOF does not have code to represent,
            // also when offset is not align to 4096, may return result 0,
            errno = Errno::EINVAL as i32;
        }
        if errno > 0 {
            errno = -errno;
        }
        self.res.store(errno, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn set_ok(&self) {
        self.res.store(1, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn callback(mut self: Box<Self>) {
        match self.cb.take() {
            Some(cb) => {
                cb.call(self);
            }
            None => return,
        }
    }

    #[inline(always)]
    pub(crate) fn callback_merged(mut self: Box<Self>) {
        if let Some(mut tasks) = self.sub_tasks.take() {
            match self._get_result() {
                Ok(buffer) => {
                    if self.action == IOAction::Read {
                        let mut offset: usize = 0;
                        let b = buffer.as_ref();
                        while let Some(mut event) = Self::pop_from_list(&mut tasks) {
                            let sub_buf = event.buf.as_mut().unwrap();
                            let sub_size = sub_buf.len();
                            sub_buf.copy_from(0, &b[offset..offset + sub_size]);
                            offset += sub_size;
                            event.set_ok();
                            event.callback();
                        }
                    } else {
                        while let Some(event) = Self::pop_from_list(&mut tasks) {
                            event.set_ok();
                            event.callback();
                        }
                    }
                }
                Err(errno) => {
                    while let Some(event) = Self::pop_from_list(&mut tasks) {
                        event.set_error(errno);
                        event.callback();
                    }
                }
            }
        } else {
            self.callback();
        }
    }
}

pub(crate) struct IOEventTaskSlot<C: IOCallbackCustom> {
    pub(crate) iocb: aio::iocb,
    pub(crate) event: Option<Box<IOEvent<C>>>,
}

impl<C: IOCallbackCustom> IOEventTaskSlot<C> {
    pub(crate) fn new(slot_id: u64) -> Self {
        Self {
            iocb: aio::iocb {
                aio_data: slot_id,
                aio_reqprio: 1,
                ..Default::default()
            },
            event: None,
        }
    }

    #[inline(always)]
    pub(crate) fn fill_slot(&mut self, event: Box<IOEvent<C>>, slot_id: u16) {
        let iocb = &mut self.iocb;
        iocb.aio_data = slot_id as libc::__u64;
        iocb.aio_fildes = event.fd as libc::__u32;
        let buf = event.buf.as_ref().unwrap();
        iocb.aio_lio_opcode = event.action as u16;
        iocb.aio_buf = buf.get_raw() as u64;
        iocb.aio_nbytes = buf.len() as u64;
        iocb.aio_offset = event.offset;
        self.event.replace(event);
    }

    #[inline(always)]
    pub(crate) fn set_written(&mut self, written: usize, cb: &IOWorkers<C>) -> bool {
        if self.iocb.aio_nbytes <= written as u64 {
            if let Some(event) = self.event.take() {
                event.set_ok();
                cb.send(event);
            }
            return true;
        }
        self.iocb.aio_nbytes -= written as u64;
        self.iocb.aio_buf += written as u64;
        return false;
    }

    #[inline(always)]
    pub(crate) fn set_error(&mut self, errno: i32, cb: &IOWorkers<C>) {
        if let Some(event) = self.event.take() {
            event.set_error(errno);
            cb.send(event);
        }
    }
}
