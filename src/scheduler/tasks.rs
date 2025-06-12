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

use std::{
    fmt,
    mem::transmute,
    sync::atomic::{AtomicI32, Ordering},
    task::Waker,
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
    fn call(self, _event: &mut IOEvent<Self>);
}

pub struct DefaultCb();

impl IOCallbackCustom for DefaultCb {
    fn call(self, _event: &mut IOEvent<Self>) {}
}

pub enum IOCallback<C: IOCallbackCustom> {
    Closure(Box<dyn FnOnce(&mut IOEvent<C>) -> () + Send + Sync>),
    Custom(C),
}

// State information that is associated with an I/O request that is currently in flight.
pub struct IOEvent<C: IOCallbackCustom> {
    // Linux kernal I/O control block which can be submitted to io_submit
    pub(crate) node: EmbeddedListNode,
    pub buf: Option<Buffer>,
    pub offset: i64,
    pub action: IOAction,
    res: AtomicI32,
    cb: Option<IOCallback<C>>,
    pub sub_tasks: Option<EmbeddedList>,
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

pub(crate) struct IOEventTaskOne<C: IOCallbackCustom> {
    _event: *const IOEvent<C>, // use when caller blocked
    waker: Option<Waker>,
}

pub(crate) struct IOEventTaskAsync<C: IOCallbackCustom> {
    event: Option<Box<IOEvent<C>>>, // use when caller process in streaming mode
}

pub(crate) enum IOEventTask<C: IOCallbackCustom> {
    One(IOEventTaskOne<C>),
    Async(IOEventTaskAsync<C>),
}

unsafe impl<C: IOCallbackCustom> Send for IOEventTask<C> {}
unsafe impl<C: IOCallbackCustom> Sync for IOEventTask<C> {}

impl<C: IOCallbackCustom> IOEvent<C> {
    #[inline]
    pub fn new(buf: Buffer, action: IOAction, offset: i64) -> Self {
        log_assert!(
            buf.len() > 0,
            "{:?} offset={}, buffer size == 0",
            action,
            offset
        );
        Self {
            buf: Some(buf),
            action,
            offset,
            res: AtomicI32::new(0),
            cb: None,
            sub_tasks: None,
            node: Default::default(),
        }
    }

    #[inline(always)]
    pub fn set_callback(&mut self, cb: IOCallback<C>) {
        self.cb = Some(cb);
    }

    #[inline(always)]
    pub fn get_size(&self) -> usize {
        self.buf.as_ref().unwrap().len()
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
    pub(crate) fn callback(mut self) {
        match self.cb.take() {
            Some(cb) => match cb {
                IOCallback::Closure(b) => {
                    b(&mut self);
                }
                IOCallback::Custom(d) => {
                    d.call(&mut self);
                }
            },
            None => return,
        }
    }

    #[inline(always)]
    pub(crate) fn callback_merged(mut self) {
        if let Some(mut tasks) = self.sub_tasks.take() {
            match self._get_result() {
                Ok(buffer) => {
                    if self.action == IOAction::Read {
                        let mut offset: usize = 0;
                        let b = buffer.as_ref();
                        for _event in tasks.drain::<Self>() {
                            let mut event = unsafe { Box::from_raw(_event) };
                            let sub_buf = event.buf.as_mut().unwrap();
                            let sub_size = sub_buf.len();
                            sub_buf.copy_from(0, &b[offset..offset + sub_size]);
                            offset += sub_size;
                            event.set_ok();
                            event.callback();
                        }
                    } else {
                        for _event in tasks.drain::<Self>() {
                            let event = unsafe { Box::from_raw(_event) };
                            event.set_ok();
                            event.callback();
                        }
                    }
                }
                Err(errno) => {
                    for _event in tasks.drain::<Self>() {
                        let event = unsafe { Box::from_raw(_event) };
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

impl<C: IOCallbackCustom> IOEventTask<C> {
    #[inline(always)]
    pub fn new_one(event: &IOEvent<C>, waker: Waker) -> Self {
        Self::One(IOEventTaskOne {
            _event: event as *const IOEvent<C>,
            waker: Some(waker),
        })
    }

    #[inline(always)]
    pub fn new_async(event: Box<IOEvent<C>>) -> Self {
        Self::Async(IOEventTaskAsync { event: Some(event) })
    }

    #[inline(always)]
    pub(crate) fn set_error(self, errno: i32, cb_workers: Option<&IOWorkers<C>>) {
        match self {
            Self::One(o) => {
                unsafe {
                    (*o._event).set_error(errno);
                }
                if let Some(waker) = o.waker.as_ref() {
                    waker.wake_by_ref();
                }
            }
            Self::Async(mut b) => {
                let event = b.event.take().unwrap();
                event.set_error(errno);
                if let Some(cb) = cb_workers {
                    cb.send(event);
                } else {
                    event.callback_merged();
                }
            }
        }
    }
}

pub(crate) struct IOEventTaskSlot<C: IOCallbackCustom> {
    pub(crate) iocb: aio::iocb,
    pub(crate) task: Option<IOEventTask<C>>,
}

impl<C: IOCallbackCustom> IOEventTaskSlot<C> {
    pub(crate) fn new(fd: libc::__u32) -> Self {
        Self {
            iocb: aio::iocb {
                aio_fildes: fd,
                aio_reqprio: 1,
                ..Default::default()
            },
            task: None,
        }
    }

    #[inline(always)]
    pub(crate) fn fill_slot(&mut self, task: IOEventTask<C>, slot_id: u16) {
        let iocb = &mut self.iocb;
        iocb.aio_data = slot_id as libc::__u64;
        match task {
            IOEventTask::Async(ref _task) => {
                let event = _task.event.as_ref().unwrap();
                let buf = event.buf.as_ref().unwrap();
                iocb.aio_lio_opcode = event.action as u16;
                iocb.aio_buf = buf.get_raw() as u64;
                iocb.aio_nbytes = buf.len() as u64;
                iocb.aio_offset = event.offset;
            }
            IOEventTask::One(ref _task) => {
                log_debug_assert!(
                    _task._event != std::ptr::null_mut(),
                    "IOEventTask._event assume notnull"
                );
                let event: &IOEvent<C> = unsafe { transmute(_task._event) };
                let buf = event.buf.as_ref().unwrap();
                iocb.aio_lio_opcode = event.action as u16;
                iocb.aio_buf = buf.get_raw() as u64;
                iocb.aio_nbytes = buf.len() as u64;
                iocb.aio_offset = event.offset;
            }
        }
        self.task.replace(task);
    }

    #[inline(always)]
    pub(crate) fn set_written(&mut self, written: usize, cb: &IOWorkers<C>) -> bool {
        if self.iocb.aio_nbytes <= written as u64 {
            if let Some(task) = self.task.take() {
                match task {
                    IOEventTask::One(t) => {
                        unsafe {
                            (*t._event).set_ok();
                        }
                        if let Some(waker) = t.waker.as_ref() {
                            waker.wake_by_ref();
                        }
                    }
                    IOEventTask::Async(mut t) => {
                        let event = t.event.take().unwrap();
                        event.set_ok();
                        cb.send(event);
                    }
                }
            }
            return true;
        }
        self.iocb.aio_nbytes -= written as u64;
        self.iocb.aio_buf += written as u64;
        return false;
    }

    #[inline(always)]
    pub(crate) fn set_error(&mut self, errno: i32, cb: &IOWorkers<C>) {
        if let Some(task) = self.task.take() {
            task.set_error(errno, Some(cb));
        }
    }
}
