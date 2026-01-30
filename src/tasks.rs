use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::os::fd::RawFd;

use nix::errno::Errno;

use embed_collections::slist::{SLinkedList, SListItem, SListNode};
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
    /// make sure SListNode always in the front.
    /// This is for putting sub_tasks in the link list, without additional allocation.
    pub(crate) node: UnsafeCell<SListNode<Self, ()>>,
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
    sub_tasks: SLinkedList<Box<Self>, ()>,
}

// Implement SListItem for IOEvent_ to allow it to be linked
unsafe impl<C: IOCallback> SListItem<()> for IOEvent_<C> {
    fn get_node(&self) -> &mut SListNode<Self, ()> {
        unsafe { &mut *self.node.get() }
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
            buf_or_len: BufOrLen::Buffer(buf),
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: None,
            sub_tasks: SLinkedList::new(),
            node: UnsafeCell::new(SListNode::default()),
        }))
    }

    #[inline]
    pub fn new_no_buf(fd: RawFd, action: IOAction, offset: i64, len: u64) -> IOEvent<C> {
        IOEvent(Box::new(IOEvent_ {
            buf_or_len: BufOrLen::Len(len), // No buffer for this action
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: None,
            sub_tasks: SLinkedList::new(),
            node: UnsafeCell::new(SListNode::default()),
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
    pub(crate) fn push_to_list(self, events: &mut SLinkedList<Box<IOEvent_<C>>, ()>) {
        events.push_back(self.0);
    }

    #[inline(always)]
    pub(crate) fn pop_from_list(events: &mut SLinkedList<Box<IOEvent_<C>>, ()>) -> Option<Self> {
        events.pop_front().map(IOEvent)
    }

    #[inline(always)]
    pub(crate) fn set_subtasks(&mut self, sub_tasks: SLinkedList<Box<IOEvent_<C>>, ()>) {
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
                        for event_box in self.sub_tasks.drain() {
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
                    for event_box in self.sub_tasks.drain() {
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
                for event_box in self.sub_tasks.drain() {
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
            node: UnsafeCell::new(SListNode::default()),
            buf_or_len: BufOrLen::Len(0),
            offset: 0,
            action: IOAction::Read, // Exit signal is a read
            fd,
            res: i32::MIN,
            cb: None, // No callback for exit signal
            sub_tasks: SLinkedList::new(),
        }))
    }
}
