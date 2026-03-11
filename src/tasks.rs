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
    fn call(self, offset: i64, res: Result<Option<Buffer>, Errno>);
}

/// Closure callback for IOEvent
pub struct ClosureCb(pub Box<dyn FnOnce(i64, Result<Option<Buffer>, Errno>) + Send>);

impl IOCallback for ClosureCb {
    fn call(self, offset: i64, res: Result<Option<Buffer>, Errno>) {
        (self.0)(offset, res)
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
    ///
    /// parameter: `check_short_read(offset: u64)` should be checking the offset exceed file end.
    /// If `check_short_read()` return true, the callback function will return Err(IOEvent) for I/O resubmit.
    ///
    /// NOTE: you should always use a weak reference in `check_short_read` closure and
    /// re-submission.
    #[inline(always)]
    pub fn callback<F>(mut self: Box<Self>, check_short_read: F) -> Result<(), Box<Self>>
    where
        F: FnOnce(u64) -> bool,
    {
        if self.res >= 0 {
            if let BufOrLen::Buffer(buf) = &mut self.buf_or_len {
                if buf.len() > self.res as usize {
                    if self.action == IOAction::Read {
                        if check_short_read(self.offset as u64 + self.res as u64) {
                            return Err(self);
                        } else {
                            // reach file ending
                            buf.set_len(self.res as usize);
                        }
                    } else {
                        // short write always need to resubmit
                        return Err(self);
                    }
                }
            }
        }
        self.callback_unchecked(false);
        Ok(())
    }

    /// Perform callback on the IOEvent when cannot re-submit for short i/o
    ///
    /// # Arguments
    ///
    /// - to_fix_short_io: should always be true, fix the buffer len of short I/O
    ///
    /// # Safety
    ///
    /// Only for callback worker does not re-submit when short I/O.
    /// Buffer::len() will changed to actual I/O copied size during callback.
    #[inline(always)]
    pub fn callback_unchecked(mut self, to_fix_short_io: bool) {
        match std::mem::replace(&mut self.cb, TaskCallback::None) {
            TaskCallback::None => {}
            TaskCallback::Callback(cb) => {
                let res: Result<Option<Buffer>, Errno> = if self.res >= 0 {
                    match self.buf_or_len {
                        BufOrLen::Buffer(mut buf) => {
                            if to_fix_short_io && buf.len() > self.res as usize {
                                buf.set_len(self.res as usize);
                            }
                            Ok(Some(buf))
                        }
                        BufOrLen::Len(_) => Ok(None),
                    }
                } else {
                    Err(Errno::from_raw(-self.res))
                };
                cb.call(self.offset, res);
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
                                    if copied < buf.len() {
                                        buf.set_len(copied); // short I/O
                                    }
                                    _cb.call(offset, Ok(Some(buf)));
                                    b = &b[copied..];
                                    offset += copied as i64
                                }
                            }
                        }
                    } else if self.action == IOAction::Write {
                        let mut l = self.res as usize;
                        for IOEventMerged { mut buf, cb } in sub_tasks {
                            let mut copied = buf.len();
                            if copied > l {
                                // short write
                                copied = l;
                                buf.set_len(l);
                            }
                            if let Some(_cb) = cb {
                                _cb.call(offset, Ok(Some(buf)));
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
                            _cb.call(offset, Err(Errno::from_raw(-self.res)));
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
    fn test_ioevent_size() {
        println!("IOEvent size {}", size_of::<IOEvent<ClosureCb>>());
        println!("BufOrLen size {}", size_of::<crate::tasks::BufOrLen>());
        println!("IOEventMerged size {}", size_of::<IOEventMerged<ClosureCb>>());
    }

    /// Test normal callback (non-merged case)
    #[test]
    fn test_callback_normal() {
        let buffer = Buffer::alloc(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 1024);

        let result = Arc::new(std::sync::Mutex::new(None));
        let result_clone = result.clone();

        event.set_callback(ClosureCb(Box::new(move |offset, res| {
            *result_clone.lock().unwrap() = Some((offset, res));
        })));

        event.set_copied(4096);
        event.callback_unchecked(true);

        let (offset, res) = result.lock().unwrap().take().unwrap();
        assert_eq!(offset, 1024);
        assert!(res.is_ok());
        assert!(res.unwrap().is_some());
    }

    /// Test merged read callback - verifies offset correctness
    #[test]
    fn test_callback_merged_read() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let offsets = Arc::new([AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)]);
        let offsets_clone = offsets.clone();

        // Create sub-tasks with their own buffers first
        let mut sub_tasks = SegList::new();

        // First sub-task: 16 bytes at offset 1000
        let buf1 = Buffer::alloc(16).unwrap();
        sub_tasks.push(IOEventMerged {
            buf: buf1,
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                offsets_clone[0].store(offset, Ordering::SeqCst);
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Second sub-task: 16 bytes at offset 1016
        let buf2 = Buffer::alloc(16).unwrap();
        let offsets_clone2 = offsets.clone();
        sub_tasks.push(IOEventMerged {
            buf: buf2,
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                offsets_clone2[1].store(offset, Ordering::SeqCst);
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Third sub-task: 16 bytes at offset 1032
        let buf3 = Buffer::alloc(16).unwrap();
        let offsets_clone3 = offsets.clone();
        sub_tasks.push(IOEventMerged {
            buf: buf3,
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                offsets_clone3[2].store(offset, Ordering::SeqCst);
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Create parent buffer and event
        let parent_buf = Buffer::alloc(48).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, parent_buf, IOAction::Read, 1000);
        event.set_copied(48); // 48 bytes read

        // Get the parent buffer back and fill with data
        let parent_buf = match std::mem::replace(&mut event.buf_or_len, BufOrLen::Len(0)) {
            BufOrLen::Buffer(buf) => buf,
            BufOrLen::Len(_) => panic!("expected buffer"),
        };
        let mut parent_buf = parent_buf;
        parent_buf.copy_from(0, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()");

        event.set_merged_tasks(parent_buf, sub_tasks);
        event.callback_unchecked(true); // Should invoke all callbacks

        // Verify offsets
        assert_eq!(offsets[0].load(Ordering::SeqCst), 1000);
        assert_eq!(offsets[1].load(Ordering::SeqCst), 1016);
        assert_eq!(offsets[2].load(Ordering::SeqCst), 1032);
    }

    /// Test merged write callback - verifies offset correctness
    #[test]
    fn test_callback_merged_write() {
        // For write, parent buffer contains data that was written
        let parent_buf = Buffer::alloc(4096).unwrap();

        let mut event = IOEvent::<ClosureCb>::new(0, parent_buf, IOAction::Write, 2000);
        event.set_copied(48); // All 48 bytes written

        let mut sub_tasks = SegList::new();

        // First sub-task: 16 bytes at offset 2000
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                assert_eq!(offset, 2000, "first write callback offset");
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Second sub-task: 16 bytes at offset 2016
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                assert_eq!(offset, 2016, "second write callback offset");
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Third sub-task: 16 bytes at offset 2032
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                assert_eq!(offset, 2032, "third write callback offset");
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        event.set_merged_tasks(Buffer::alloc(4096).unwrap(), sub_tasks);
        event.callback_unchecked(true); // Should invoke all callbacks with correct offsets
    }

    /// Test merged callback with error result
    #[test]
    fn test_callback_merged_error() {
        let parent_buf = Buffer::alloc(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, parent_buf, IOAction::Read, 3000);
        event.set_error(Errno::EIO as i32); // IO error

        let mut sub_tasks = SegList::new();

        // First sub-task
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                assert_eq!(offset, 3000, "error callback offset");
                assert!(res.is_err());
                assert_eq!(res.err().unwrap(), Errno::EIO);
            }))),
        });

        // Second sub-task
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                assert_eq!(offset, 3016, "error callback offset 2");
                assert!(res.is_err());
            }))),
        });

        event.set_merged_tasks(Buffer::alloc(48).unwrap(), sub_tasks);
        event.callback_unchecked(true);
    }

    /// Test short read in merged callback
    #[test]
    fn test_callback_merged_short_read() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let offsets = Arc::new([AtomicI64::new(0), AtomicI64::new(0)]);
        let offsets_clone = offsets.clone();

        let mut sub_tasks = SegList::new();

        // First sub-task: 16 bytes (fully read)
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                offsets_clone[0].store(offset, Ordering::SeqCst);
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Second sub-task: 16 bytes but only 8 were read (short read)
        let offsets_clone2 = offsets.clone();
        sub_tasks.push(IOEventMerged {
            buf: Buffer::alloc(16).unwrap(),
            cb: Some(ClosureCb(Box::new(move |offset, res| {
                offsets_clone2[1].store(offset, Ordering::SeqCst);
                assert!(res.is_ok());
                assert!(res.unwrap().is_some());
            }))),
        });

        // Parent buffer with 32 bytes
        let parent_buf = Buffer::alloc(32).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, parent_buf, IOAction::Read, 4000);
        event.set_copied(24); // Short read: only 24 bytes (16 + 8)

        // Get parent buffer back
        let parent_buf = match std::mem::replace(&mut event.buf_or_len, BufOrLen::Len(0)) {
            BufOrLen::Buffer(buf) => buf,
            BufOrLen::Len(_) => panic!("expected buffer"),
        };

        event.set_merged_tasks(parent_buf, sub_tasks);
        event.callback_unchecked(true);

        // Verify
        assert_eq!(offsets[0].load(Ordering::SeqCst), 4000);
        assert_eq!(offsets[1].load(Ordering::SeqCst), 4016);
    }
}
