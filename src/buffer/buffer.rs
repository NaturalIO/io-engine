use super::utils::{safe_copy, set_zero};
use libc;
use nix::errno::Errno;
use std::slice;
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use fail::fail_point;

/// Buffer can obtain from alloc, or wrap a raw c buffer, or convert from Vec.
///
/// When Clone, will copy the contain into a new Buffer.
#[repr(C, align(1))]
pub struct Buffer {
    pub buf_ptr: *mut libc::c_void,
    /// the highest bit of `size` represents `owned`
    pub(crate) size: u32,
    /// the highest bit of `cap` represents `mutable`
    pub(crate) cap: u32,
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "buffer {:p} size {}", self.get_raw(), self.len())
    }
}

unsafe impl Send for Buffer {}

unsafe impl Sync for Buffer {}

pub const MIN_ALIGN: usize = 512;
pub const MAX_BUFFER_SIZE: usize = 1 << 31;

fn is_aligned(offset: usize, size: usize) -> bool {
    return (offset & (MIN_ALIGN - 1) == 0) && (size & (MIN_ALIGN - 1) == 0);
}

impl Buffer {
    /// Allocate aligned buffer for aio.
    ///
    /// NOTE: Be aware that buffer allocated will not necessarily be zeroed
    #[inline]
    pub fn aligned(size: usize) -> Result<Buffer, Errno> {
        let mut _buf = Self::_alloc(MIN_ALIGN, size)?;
        fail_point!("alloc_buf", |_| {
            rand_buffer(&mut _buf);
            return Ok(_buf);
        });
        return Ok(_buf);
    }

    /// Allocate non-aligned Buffer
    ///
    /// NOTE: Be aware that buffer allocated will not necessarily be zeroed
    #[inline]
    pub fn alloc(size: usize) -> Result<Buffer, Errno> {
        let mut _buf = Self::_alloc(0, size)?;
        fail_point!("alloc_buf", |_| {
            rand_buffer(&mut _buf);
            return Ok(_buf);
        });
        return Ok(_buf);
    }

    /// Allocate a buffer.
    #[inline]
    fn _alloc(align: usize, size: usize) -> Result<Self, Errno> {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        log_assert!(
            size < MAX_BUFFER_SIZE,
            "size {} >= {} is not supported",
            size,
            MAX_BUFFER_SIZE
        );
        if align > 0 {
            debug_assert!((align & (MIN_ALIGN - 1)) == 0);
            debug_assert!((size & (align - 1)) == 0);
            unsafe {
                let res =
                    libc::posix_memalign(&mut ptr, align as libc::size_t, size as libc::size_t);
                if res != 0 {
                    return Err(Errno::ENOMEM);
                }
            }
        } else {
            ptr = unsafe { libc::malloc(size as libc::size_t) };
            if ptr == std::ptr::null_mut() {
                return Err(Errno::ENOMEM);
            }
        }
        // owned == true
        let _size = size as u32 | MAX_BUFFER_SIZE as u32;
        // mutable == true
        let _cap = _size;
        Ok(Self { buf_ptr: ptr, size: _size, cap: _cap })
    }

    #[inline(always)]
    pub fn is_owned(&self) -> bool {
        self.size & (MAX_BUFFER_SIZE as u32) != 0
    }

    #[inline(always)]
    pub fn is_mutable(&self) -> bool {
        self.cap & (MAX_BUFFER_SIZE as u32) != 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let size = self.size & (MAX_BUFFER_SIZE as u32 - 1);
        size as usize
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        let cap = self.cap & (MAX_BUFFER_SIZE as u32 - 1);
        cap as usize
    }

    #[inline(always)]
    pub fn set_len(&mut self, len: usize) {
        log_assert!(len < MAX_BUFFER_SIZE, "size {} >= {} is not supported", len, MAX_BUFFER_SIZE);
        log_assert!(len <= self.cap as usize, "size {} must be <= {}", len, self.cap);
        let owned: u32 = self.size & MAX_BUFFER_SIZE as u32;
        self.size = owned | len as u32;
    }

    /// Wrap a mutable buffer passed from c code, without owner ship.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle.
    #[inline]
    pub fn from_c_ref_mut(ptr: *mut libc::c_void, size: usize) -> Self {
        log_assert!(
            size < MAX_BUFFER_SIZE,
            "size {} >= {} is not supported",
            size,
            MAX_BUFFER_SIZE
        );
        log_assert!(ptr != std::ptr::null_mut());
        // owned == false
        // mutable == true
        let _cap = size as u32 | MAX_BUFFER_SIZE as u32;
        Self { buf_ptr: ptr, size: size as u32, cap: _cap }
    }

    /// Wrap a const buffer passed from c code, without owner ship.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle
    #[inline]
    pub fn from_c_ref_const(ptr: *const libc::c_void, size: usize) -> Self {
        log_assert!(
            size < MAX_BUFFER_SIZE,
            "size {} >= {} is not supported",
            size,
            MAX_BUFFER_SIZE
        );
        log_assert!(ptr != std::ptr::null());
        // owned == false
        // mutable == false
        Self { buf_ptr: unsafe { std::mem::transmute(ptr) }, size: size as u32, cap: size as u32 }
    }

    #[inline(always)]
    pub fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.buf_ptr as *const u8, self.len()) }
    }

    /// Will only check mutability and debug mode, skip on release mode for speed
    #[inline(always)]
    pub fn as_mut(&mut self) -> &mut [u8] {
        #[cfg(debug_assertions)]
        {
            if !self.is_mutable() {
                panic!("Cannot change a mutable buffer")
            }
        }
        unsafe { slice::from_raw_parts_mut(self.buf_ptr as *mut u8, self.len()) }
    }

    /// Check this buffer usable by aio
    #[inline(always)]
    pub fn is_aligned(&self) -> bool {
        is_aligned(self.buf_ptr as usize, self.capacity())
    }

    /// Get buffer raw pointer
    #[inline]
    pub fn get_raw(&self) -> *const u8 {
        self.buf_ptr as *const u8
    }

    /// Get buffer raw mut pointer
    #[inline]
    pub fn get_raw_mut(&mut self) -> *mut u8 {
        self.buf_ptr as *mut u8
    }

    /// Copy from another u8 slice into self[offset..].
    ///
    /// NOTE: will not do memset.
    ///
    /// Argument:
    ///
    ///   offset: Address of this buffer to start filling.
    #[inline]
    pub fn copy_from(&mut self, offset: usize, other: &[u8]) {
        let size = self.len();
        let dst = self.as_mut();
        if offset > 0 {
            assert!(offset < size);
            safe_copy(&mut dst[offset..], other);
        } else {
            safe_copy(dst, other);
        }
    }

    /// Copy from another u8 slice into self[offset..], and memset the rest part.
    ///
    /// Argument:
    ///
    ///   offset: Address of this buffer to start filling.
    #[inline]
    pub fn copy_and_clean(&mut self, offset: usize, other: &[u8]) {
        let end: usize;
        let size = self.len();
        let dst = self.as_mut();
        assert!(offset < size);
        if offset > 0 {
            set_zero(&mut dst[0..offset]);
            end = offset + safe_copy(&mut dst[offset..], other);
        } else {
            end = safe_copy(dst, other);
        }
        if size > end {
            set_zero(&mut dst[end..]);
        }
    }

    /// Fill this buffer with zero
    #[inline]
    pub fn zero(&mut self) {
        set_zero(self);
    }

    /// Fill this buffer[offset..(offset+len)] with zero
    #[inline]
    pub fn set_zero(&mut self, offset: usize, len: usize) {
        let _len = self.len();
        let mut end = offset + len;
        if end > _len {
            end = _len;
        }
        let buf = self.as_mut();
        if offset > 0 || end < _len {
            set_zero(&mut buf[offset..end]);
        } else {
            set_zero(buf);
        }
    }
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        let mut new_buf = if self.is_aligned() {
            Self::aligned(self.capacity()).unwrap()
        } else {
            Self::alloc(self.capacity()).unwrap()
        };
        if self.len() != self.capacity() {
            new_buf.set_len(self.len());
        }
        safe_copy(new_buf.as_mut(), self.as_ref());
        new_buf
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if self.is_owned() {
            unsafe {
                libc::free(self.buf_ptr);
            }
        }
    }
}

impl Into<Vec<u8>> for Buffer {
    fn into(mut self) -> Vec<u8> {
        if !self.is_owned() {
            panic!("buffer is c ref, not owned");
        }
        // Change to not owned, to prevent drop()
        self.size &= MAX_BUFFER_SIZE as u32 - 1;
        return unsafe {
            Vec::<u8>::from_raw_parts(self.buf_ptr as *mut u8, self.len(), self.capacity())
        };
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(buf: Vec<u8>) -> Self {
        let size = buf.len();
        let cap = buf.capacity();
        log_assert!(
            size < MAX_BUFFER_SIZE,
            "size {} >= {} is not supported",
            size,
            MAX_BUFFER_SIZE
        );
        log_assert!(cap < MAX_BUFFER_SIZE, "cap {} >= {} is not supported", cap, MAX_BUFFER_SIZE);
        // owned == true
        let _size = size as u32 | MAX_BUFFER_SIZE as u32;
        // mutable == true
        let _cap = cap as u32 | MAX_BUFFER_SIZE as u32;
        Buffer { buf_ptr: buf.leak().as_mut_ptr() as *mut libc::c_void, size: _size, cap: _cap }
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Buffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsMut<[u8]> for Buffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}
