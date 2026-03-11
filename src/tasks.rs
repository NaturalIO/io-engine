use std::os::fd::RawFd;
use std::{fmt, u64};

use embed_collections::SegList;
use io_buffer::{Buffer, safe_copy};
use nix::errno::Errno;

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum IOAction {
    Read = 0,
    Write = 1,
    Alloc = 2,
    Fsync = 3,
}

/// Define your callback with this trait
pub trait IOCallback: Sized + 'static + Send + Unpin {
    fn call(self, offset: i64, buf: Option<Buffer>, res: Result<u32, i32>);
}

/// Closure callback for IOEvent
pub struct ClosureCb(pub Box<dyn FnOnce(i64, Option<Buffer>, Result<u32, i32>) + Send>);

impl IOCallback for ClosureCb {
    fn call(self, offset: i64, buf: Option<Buffer>, res: Result<u32, i32>) {
        (self.0)(offset, buf, res)
    }
}

// Carries the information of read/write event
pub struct IOEvent<C: IOCallback> {
    pub action: IOAction,
    /// Result of the IO operation.
    /// Initialized to i32::MIN.
    /// >= 0: Accumulated bytes transferred (used for partial IO retries).
    /// < 0: Error code (negative errno).
    pub(crate) res: i32,
    /// make sure SListNode always in the front.
    /// This is for putting sub_tasks in the link list, without additional allocation.
    buf_or_len: BufOrLen,
    pub offset: i64,
    pub fd: RawFd,
    cb: TaskCallback<C>,
}

enum TaskCallback<C: IOCallback> {
    None,
    Callback(C),
    Merged(SegList<IOEventMerged<C>>),
}

enum BufOrLen {
    Buffer(Buffer),
    /// for fallocate
    Len(u64),
}

pub(crate) struct IOEventMerged<C: IOCallback> {
    pub buf: Buffer,
    pub cb: Option<C>,
}

impl<C: IOCallback> fmt::Debug for IOEvent<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let TaskCallback::Merged(sub_tasks) = &self.cb {
            write!(f, "offset={} {:?} merged {}", self.offset, self.action, sub_tasks.len())
        } else {
            write!(f, "offset={} {:?}", self.offset, self.action)
        }
    }
}

impl<C: IOCallback> IOEvent<C> {
    #[inline]
    pub fn new(fd: RawFd, buf: Buffer, action: IOAction, offset: i64) -> Self {
        log_assert!(buf.len() > 0, "{:?} offset={}, buffer size == 0", action, offset);
        Self {
            buf_or_len: BufOrLen::Buffer(buf),
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: TaskCallback::None,
        }
    }

    #[inline]
    pub fn new_no_buf(fd: RawFd, action: IOAction, offset: i64, len: u64) -> Self {
        Self {
            buf_or_len: BufOrLen::Len(len), // No buffer for this action
            fd,
            action,
            offset,
            res: i32::MIN,
            cb: TaskCallback::None,
        }
    }

    #[inline(always)]
    pub fn set_fd(&mut self, fd: RawFd) {
        self.fd = fd;
    }

    /// Set callback for IOEvent, might be closure or a custom struct
    #[inline(always)]
    pub fn set_callback(&mut self, cb: C) {
        self.cb = TaskCallback::Callback(cb);
    }

    #[inline(always)]
    pub fn get_size(&self) -> u64 {
        match &self.buf_or_len {
            BufOrLen::Buffer(buf) => buf.len() as u64,
            BufOrLen::Len(l) => *l,
        }
    }

    /// Set merged buffer and subtasks for the master event after merging.
    #[inline(always)]
    pub(crate) fn set_merged_tasks(
        &mut self, merged_buf: Buffer, sub_tasks: SegList<IOEventMerged<C>>,
    ) {
        self.buf_or_len = BufOrLen::Buffer(merged_buf);
        self.cb = TaskCallback::Merged(sub_tasks);
    }

    /// Convert this IOEvent into an IOEventMerged for storing in merge buffer.
    /// Extracts the buffer and callback from the event.
    #[inline(always)]
    pub(crate) fn into_merged(mut self) -> IOEventMerged<C> {
        let buf = match std::mem::replace(&mut self.buf_or_len, BufOrLen::Len(0)) {
            BufOrLen::Buffer(buf) => buf,
            BufOrLen::Len(_) => panic!("into_merged called on IOEvent with no buffer"),
        };
        let cb = match std::mem::replace(&mut self.cb, TaskCallback::None) {
            TaskCallback::Callback(cb) => Some(cb),
            _ => None,
        };
        IOEventMerged { buf, cb }
    }

    /// Extract buffer and callback to create IOEventMerged, leaving this event with empty buffer.
    /// Used when moving first event to merged_events list.
    #[inline(always)]
    pub(crate) fn extract_merged(&mut self) -> IOEventMerged<C> {
        let buf = match std::mem::replace(&mut self.buf_or_len, BufOrLen::Len(0)) {
            BufOrLen::Buffer(buf) => buf,
            BufOrLen::Len(_) => panic!("extract_merged called on IOEvent with no buffer"),
        };
        let cb = match std::mem::replace(&mut self.cb, TaskCallback::None) {
            TaskCallback::Callback(cb) => Some(cb),
            _ => None,
        };
        IOEventMerged { buf, cb }
    }

    /// return (offset, ptr, len)
    #[inline(always)]
    pub(crate) fn get_param_for_io(&mut self) -> (u64, *mut u8, u32) {
        if let BufOrLen::Buffer(buf) = &mut self.buf_or_len {
            let mut offset = self.offset as u64;
            let mut p = buf.get_raw_mut();
            let mut l = buf.len() as u32;
            if self.res > 0 {
                // resubmited I/O
                offset += self.res as u64;
                p = unsafe { p.add(self.res as usize) };
                l += self.res as u32;
            }
            (offset, p, l)
        } else {
            panic!("get_buf_raw called on IOEvent with no buffer");
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
            // XXX?
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
            // the initial state
            self.res = len as i32;
        } else {
            // resubmit for short I/O
            self.res += len as i32;
        }
    }

    /// For writing custom callback workers
    ///
    /// Callback worker should always call this function on receiving IOEvent from Driver
    #[inline(always)]
    pub fn callback(mut self) {
        match std::mem::replace(&mut self.cb, TaskCallback::None) {
            TaskCallback::None => {}
            TaskCallback::Callback(cb) => {
                let res: Result<u32, i32> =
                    if self.res >= 0 { Ok(self.res as u32) } else { Err(-self.res) };
                let buf = match self.buf_or_len {
                    BufOrLen::Buffer(buf) => Some(buf),
                    BufOrLen::Len(_) => None,
                };
                cb.call(self.offset, buf, res);
            }
            TaskCallback::Merged(sub_tasks) => {
                if self.res >= 0 {
                    let mut offset = self.offset;
                    if self.action == IOAction::Read {
                        if let BufOrLen::Buffer(parent_buf) = &self.buf_or_len {
                            let mut b: &[u8] = &parent_buf[0..self.res as usize];
                            for IOEventMerged { mut buf, cb } in sub_tasks {
                                if let Some(_cb) = cb {
                                    let copied = safe_copy(&mut buf, b);
                                    _cb.call(offset, Some(buf), Ok(copied as u32));
                                    b = &b[copied..];
                                    offset += copied as i64
                                }
                            }
                        }
                    } else if self.action == IOAction::Write {
                        let mut l = self.res as usize;
                        for IOEventMerged { buf, cb } in sub_tasks {
                            let mut copied = buf.len();
                            if copied > l {
                                // short write
                                copied = l;
                            }
                            if let Some(_cb) = cb {
                                _cb.call(offset, None, Ok(copied as u32))
                            }
                            l -= copied;
                            offset += copied as i64;
                        }
                    }
                } else {
                    let mut offset = self.offset;
                    for IOEventMerged { buf, cb } in sub_tasks {
                        let _l = buf.len() as i64;
                        if let Some(_cb) = cb {
                            _cb.call(offset, Some(buf), Err(-self.res));
                        }
                        offset += _l;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use io_buffer::Buffer;
    use nix::errno::Errno;
    use std::mem::size_of;
    use std::sync::Arc;

    #[test]
    fn test_ioevent() {
        println!("IOEvent size {}", size_of::<IOEvent<ClosureCb>>());
        println!("BufOrLen size {}", size_of::<crate::tasks::BufOrLen>());
        println!("IOEventMerged size {}", size_of::<IOEventMerged<ClosureCb>>());
        /*
        let buffer = Buffer::aligned(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
        assert!(!event.is_done());
        event.set_copied(4096);
        assert!(event.is_done());
        assert!(event.get_write_result().is_ok());

        let buffer = Buffer::aligned(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
        event.set_error(Errno::EINTR as i32);
        assert!(event.get_write_result().is_err());

        let buffer = Buffer::aligned(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
        event.set_error(Errno::EINTR as i32);
        let err = event.get_write_result().err().unwrap();
        assert_eq!(err, Errno::EINTR);
        */
    }
}
