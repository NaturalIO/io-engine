use std::fmt;
use std::ops::{Deref, DerefMut};
use std::os::fd::RawFd;
use std::ptr::NonNull;

use nix::errno::Errno;

use embed_collections::slist_owned::{SLinkedListOwned, SListItemOwned};
use io_buffer::{Buffer, safe_copy};

pub enum BufOrLen {
    Buffer(Buffer),
    Len(u64),
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum IOAction {
    Read = 0,
    Write = 1,
    Alloc = 2,
    Fsync = 3,
}

/// Define your callback with this trait
pub trait IOCallback: Sized + 'static + Send + Unpin {
    fn call(self, _event: IOEvent<Self>);
}

/// Closure callback for IOEvent
pub struct ClosureCb(pub Box<dyn FnOnce(IOEvent<Self>) + Send + Sync + 'static>);

impl IOCallback for ClosureCb {
    fn call(self, event: IOEvent<Self>) {
        (self.0)(event)
    }
}

pub struct IOEvent<C: IOCallback>(pub Box<IOEvent_<C>>);

impl<C: IOCallback> Deref for IOEvent<C> {
    type Target = IOEvent_<C>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C: IOCallback> DerefMut for IOEvent<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<C: IOCallback> fmt::Debug for IOEvent<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Carries the information of read/write event
#[repr(C)]
pub struct IOEvent_<C: IOCallback> {
    pub(crate) next: Option<NonNull<Self>>,
    pub buf_or_len: BufOrLen,
    pub offset: i64,
    pub action: IOAction,
    pub fd: RawFd,
    /// Result of the IO operation.
    /// Initialized to i32::MIN.
    /// >= 0: Accumulated bytes transferred (used for partial IO retries).
    /// < 0: Error code (negative errno).
    pub(crate) res: i32,
    cb: Option<C>,
    sub_tasks: SLinkedListOwned<Self, ()>,
}

impl<C: IOCallback> SListItemOwned<()> for IOEvent_<C> {
    fn get_node(&mut self) -> &mut Option<NonNull<Self>> {
        &mut self.next
    }
}

// This is required because IOEvent_ contains NonNull, which is not Send.
// The user has asserted that this is safe in the context of this program.
unsafe impl<C: IOCallback> Send for IOEvent_<C> {}

impl<C: IOCallback> Drop for IOEvent_<C> {
    fn drop(&mut self) {
        // This is important to prevent memory leaks.
        SListItemOwned::consume_all(self);
    }
}

impl<C: IOCallback> fmt::Debug for IOEvent_<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "offset={} {:?} sub_tasks {}", self.offset, self.action, self.sub_tasks.len())
    }
}

impl<C: IOCallback> IOEvent<C> {
    #[inline]
    pub fn new(fd: RawFd, buf: Buffer, action: IOAction, offset: i64) -> IOEvent<C> {
        log_assert!(buf.len() > 0, "{:?} offset={}, buffer size == 0", action, offset);
        IOEvent(Box::new(IOEvent_ {
            next: None,
            buf_or_len: BufOrLen::Buffer(buf),
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: None,
            sub_tasks: SLinkedListOwned::new(),
        }))
    }

    #[inline]
    pub fn new_no_buf(fd: RawFd, action: IOAction, offset: i64, len: u64) -> IOEvent<C> {
        IOEvent(Box::new(IOEvent_ {
            next: None,
            buf_or_len: BufOrLen::Len(len), // No buffer for this action
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: None,
            sub_tasks: SLinkedListOwned::new(),
        }))
    }

    #[inline(always)]
    pub fn set_fd(&mut self, fd: RawFd) {
        self.fd = fd;
    }

    /// Set callback for IOEvent, might be closure or a custom struct
    #[inline(always)]
    pub fn set_callback(&mut self, cb: C) {
        self.cb = Some(cb);
    }

    #[inline(always)]
    pub fn get_size(&self) -> usize {
        if let BufOrLen::Buffer(buf) = &self.buf_or_len { buf.len() } else { 0 }
    }

    #[inline(always)]
    pub(crate) fn set_subtasks(&mut self, sub_tasks: SLinkedListOwned<IOEvent_<C>, ()>) {
        self.sub_tasks = sub_tasks;
    }

    #[inline(always)]
    pub fn get_buf_ref<'a>(&'a self) -> &'a [u8] {
        if let BufOrLen::Buffer(buf) = &self.buf_or_len {
            buf.as_ref()
        } else {
            panic!("get_buf_ref called on IOEvent with no buffer");
        }
    }

    #[inline(always)]
    pub fn is_done(&self) -> bool {
        self.res != i32::MIN
    }

    #[inline(always)]
    pub fn get_write_result(self) -> Result<(), Errno> {
        let res = self.res;
        if res >= 0 {
            return Ok(());
        } else if res == i32::MIN {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(Errno::from_raw(-res));
        }
    }

    /// Get the result of the IO operation (bytes read/written or error).
    /// Returns the number of bytes successfully transferred.
    #[inline(always)]
    pub fn get_result(&self) -> Result<usize, Errno> {
        let res = self.res;
        if res >= 0 {
            return Ok(res as usize);
        } else if res == i32::MIN {
            panic!("IOEvent get_result before it's done");
        } else {
            return Err(Errno::from_raw(-res));
        }
    }

    /// Get the buffer from a read operation.
    /// Note: The buffer length is NOT modified. Use `get_result()` to get actual bytes read.
    #[inline(always)]
    pub fn get_read_result(mut self) -> Result<Buffer, Errno> {
        let res = self.res;
        if res >= 0 {
            let buf_or_len = std::mem::replace(&mut self.buf_or_len, BufOrLen::Len(0));
            if let BufOrLen::Buffer(buf) = buf_or_len {
                // Do NOT modify buffer length - caller should use get_result() to know actual bytes read
                return Ok(buf);
            } else {
                panic!("get_read_result called on IOEvent with no buffer");
            }
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

    /// Trigger the callback for this IOEvent.
    /// This consumes the event and calls the associated callback.
    #[inline(always)]
    pub(crate) fn callback(mut self) {
        match self.cb.take() {
            Some(cb) => {
                cb.call(self);
            }
            None => return,
        }
    }

    /// For writing custom callback workers
    ///
    /// Callback worker should always call this function on receiving IOEvent from Driver
    #[inline(always)]
    pub fn callback_merged(mut self) {
        if !self.sub_tasks.is_empty() {
            let res = self.res;
            if res >= 0 {
                if self.action == IOAction::Read {
                    let buf_or_len = std::mem::replace(&mut self.buf_or_len, BufOrLen::Len(0));
                    if let BufOrLen::Buffer(buffer) = buf_or_len {
                        let mut b = buffer.as_ref();
                        while let Some(event_box) = self.sub_tasks.pop_front() {
                            let mut event = IOEvent(event_box);
                            if let BufOrLen::Buffer(sub_buf) = &mut event.buf_or_len {
                                if b.len() == 0 {
                                    // short read
                                    event.set_copied(0);
                                } else {
                                    let copied = safe_copy(sub_buf, b);
                                    event.set_copied(copied);
                                    b = &b[copied..];
                                }
                            }
                            event.callback();
                        }
                    }
                } else {
                    let l = self.get_size();
                    while let Some(event_box) = self.sub_tasks.pop_front() {
                        let mut event = IOEvent(event_box);
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
                while let Some(event_box) = self.sub_tasks.pop_front() {
                    let mut event = IOEvent(event_box);
                    event.set_error(errno);
                    event.callback();
                }
            }
        } else {
            self.callback();
        }
    }

    // New constructor for exit signal events
    pub(crate) fn new_exit_signal(fd: RawFd) -> Self {
        // Exit signal wraps a IOEvent
        Self(Box::new(IOEvent_ {
            next: None,
            buf_or_len: BufOrLen::Len(0),
            offset: 0,
            action: IOAction::Read, // Exit signal is a read
            fd,
            res: i32::MIN,
            cb: None, // No callback for exit signal
            sub_tasks: SLinkedListOwned::new(),
        }))
    }
}
