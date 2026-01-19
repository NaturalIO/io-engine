use std::fmt;
use std::os::fd::RawFd;

use nix::errno::Errno;

use crate::embedded_list::*;
use io_buffer::{Buffer, safe_copy};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum IOAction {
    Read = 0,
    Write = 1,
}

/// Define your callback with this trait
pub trait IoCallback: Sized + 'static + Send + Unpin {
    fn call(self, _event: Box<IOEvent<Self>>);
}

/// Closure callback for IOEvent
pub struct ClosureCb(pub Box<dyn FnOnce(Box<IOEvent<Self>>) + Send + Sync + 'static>);

impl IoCallback for ClosureCb {
    fn call(self, event: Box<IOEvent<Self>>) {
        (self.0)(event)
    }
}

// Carries the information of read/write event
#[repr(C)]
pub struct IOEvent<C: IoCallback> {
    /// make sure EmbeddedListNode always in the front.
    /// This is for putting sub_tasks in the link list, without additional allocation.
    pub(crate) node: EmbeddedListNode,
    pub buf: Option<Buffer>,
    pub offset: i64,
    pub action: IOAction,
    pub fd: RawFd,
    res: i32,
    cb: Option<C>,
    sub_tasks: Option<EmbeddedList>,
}

impl<C: IoCallback> fmt::Debug for IOEvent<C> {
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

impl<C: IoCallback> IOEvent<C> {
    #[inline]
    pub fn new(fd: RawFd, buf: Buffer, action: IOAction, offset: i64) -> Box<Self> {
        log_assert!(buf.len() > 0, "{:?} offset={}, buffer size == 0", action, offset);
        Box::new(Self {
            buf: Some(buf),
            fd,
            action,
            offset,
            res: i32::MIN,
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
    pub fn set_subtasks(&mut self, sub_tasks: EmbeddedList) {
        self.sub_tasks = Some(sub_tasks)
    }

    #[inline(always)]
    pub fn get_buf_ref<'a>(&'a self) -> &'a [u8] {
        self.buf.as_ref().unwrap().as_ref()
    }

    #[inline(always)]
    pub fn is_done(&self) -> bool {
        self.res != i32::MIN
    }

    #[inline(always)]
    pub fn get_write_result(self) -> Result<(), Errno> {
        let res = self.res;
        if res >= 0 {
            // Short write is handled by driver rety
            return Ok(());
        } else if res == i32::MIN {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(Errno::from_raw(-res));
        }
    }

    #[inline(always)]
    pub fn get_read_result(mut self) -> Result<Buffer, Errno> {
        let res = self.res;
        if res >= 0 {
            let mut buf = self.buf.take().unwrap();
            buf.set_len(res as usize);
            return Ok(buf);
        } else if res == i32::MIN {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(Errno::from_raw(-res));
        }
    }

    #[inline(always)]
    pub(crate) fn set_error(&mut self, mut errno: i32) {
        if errno == 0 {
            // XXX: EOF does not have code to represent,
            // also when offset is not align to 4096, may return result 0,
            errno = Errno::EINVAL as i32;
        }
        if errno > 0 {
            errno = -errno;
        }
        self.res = errno;
    }

    #[inline(always)]
    pub(crate) fn set_copied(&mut self, len: usize) {
        if self.res == i32::MIN {
            self.res = len as i32;
        } else {
            self.res += len as i32;
        }
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
            let res = self.res;
            if res >= 0 {
                if self.action == IOAction::Read {
                    let buffer = self.buf.take().unwrap();
                    let mut b = buffer.as_ref();
                    while let Some(mut event) = Self::pop_from_list(&mut tasks) {
                        let sub_buf = event.buf.as_mut().unwrap();
                        if b.len() == 0 {
                            // short read
                            event.set_copied(0);
                        } else {
                            let copied = safe_copy(sub_buf, b);
                            event.set_copied(copied);
                            b = &b[copied..];
                        }
                        event.callback();
                    }
                } else {
                    let l = self.buf.as_ref().unwrap().len();
                    while let Some(mut event) = Self::pop_from_list(&mut tasks) {
                        let mut sub_len = event.get_size();
                        if sub_len > l {
                            // short write
                            sub_len = l;
                        }
                        event.set_copied(sub_len);
                        event.callback();
                    }
                }
            } else {
                let errno = -res;
                while let Some(mut event) = Self::pop_from_list(&mut tasks) {
                    event.set_error(errno);
                    event.callback();
                }
            }
        } else {
            self.callback();
        }
    }

    // New constructor for exit signal events
    pub(crate) fn new_exit_signal(fd: RawFd) -> Box<Self> {
        Box::new(Self {
            node: Default::default(),
            buf: None,
            offset: 0,
            action: IOAction::Read, // Exit signal is a read
            fd,
            res: i32::MIN,
            cb: None, // No callback for exit signal
            sub_tasks: None,
        })
    }
}
