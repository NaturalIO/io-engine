// Copyright (c) 2025 NaturalIO

use std::os::fd::RawFd;
use std::{
    fmt,
    sync::atomic::{AtomicI32, Ordering},
};

use nix::errno::Errno;

use crate::callback_worker::*;
use crate::embedded_list::*;
use io_buffer::Buffer;

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

// Carries the information of read/write event
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
    pub(crate) is_exit_signal: bool, // New field to identify exit signal
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
        log_assert!(buf.len() > 0, "{:?} offset={}, buffer size == 0", action, offset);
        Box::new(Self {
            buf: Some(buf),
            fd,
            action,
            offset,
            res: AtomicI32::new(0),
            cb: None,
            sub_tasks: None,
            node: Default::default(),
            is_exit_signal: false, // Default to false
        })
    }

    #[inline]
    pub fn new_exit_signal(fd: RawFd) -> Box<Self> {
        Box::new(Self {
            buf: Some(Buffer::aligned(0).unwrap()), // Zero-length buffer
            fd,                                     // Use the provided valid FD
            action: IOAction::Read,                 // Read operation
            offset: 0,
            res: AtomicI32::new(0),
            cb: None,
            sub_tasks: None,
            node: Default::default(),
            is_exit_signal: true, // Mark as exit signal
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
