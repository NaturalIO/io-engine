mod large;
mod utils;
use large::{BufferLarge, MIN_ALIGN};
pub mod lz4;
pub use utils::*;

#[cfg(test)]
mod test;

use nix::errno::Errno;
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use fail::fail_point;

/// The Buffer enum, can obtain from alloc, wrap a raw c buffer, or convert from Vec.
///
/// When Clone, will copy the contain into a new Buffer.
#[derive(Clone)]
pub enum Buffer {
    Large(BufferLarge),
    Vec(Vec<u8>),
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "buffer {:p} size {}", self.get_raw(), self.len())
    }
}

impl Buffer {
    /// Allocate aligned buffer for aio.
    ///
    /// NOTE: Be aware that buffer allocated will not necessarily be zeroed
    #[inline]
    pub fn aligned(size: usize) -> Result<Buffer, Errno> {
        let mut _buf = BufferLarge::alloc(MIN_ALIGN, size)?;
        fail_point!("alloc_buf", |_| {
            rand_buffer(&mut _buf);
            return Ok(Buffer::Large(_buf));
        });
        return Ok(Buffer::Large(_buf));
    }

    /// Allocate non-aligned Buffer
    /// NOTE: Be aware that buffer allocated will not necessarily be zeroed
    #[inline]
    pub fn alloc(size: usize) -> Result<Buffer, Errno> {
        let mut _buf = BufferLarge::alloc(0, size)?;
        fail_point!("alloc_buf", |_| {
            rand_buffer(&mut _buf);
            return Ok(Buffer::Large(_buf));
        });
        return Ok(Buffer::Large(_buf));
    }

    /// Wrap a mutable buffer passed from c code without owner ship, as a fake 'static lifetime struct.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle
    #[inline]
    pub fn from_c_ref_mut(ptr: *mut libc::c_void, size: usize) -> Self {
        Self::Large(BufferLarge::from_c_ref_mut(ptr, size))
    }

    /// Wrap a const buffer passed from c code, without owner ship, as a fake 'static lifetime struct.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle
    #[inline]
    pub fn from_c_ref_const(ptr: *const libc::c_void, size: usize) -> Self {
        Self::Large(BufferLarge::from_c_ref_const(ptr, size))
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            Buffer::Large(buf) => buf.size as usize,
            Buffer::Vec(buf) => buf.len(),
        }
    }

    /// Get buffer raw pointer
    #[inline]
    pub fn get_raw(&self) -> *const u8 {
        match self {
            Buffer::Large(buf) => buf.buf_ptr as *const u8,
            Buffer::Vec(buf) => buf.as_ptr(),
        }
    }

    /// Get buffer raw mut pointer
    #[inline]
    pub fn get_raw_mut(&mut self) -> *mut u8 {
        match self {
            Buffer::Large(buf) => buf.buf_ptr as *mut u8,
            Buffer::Vec(v) => v.as_mut_ptr(),
        }
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

    #[inline]
    pub fn is_aligned(&self) -> bool {
        match self {
            Buffer::Large(buf) => buf.is_aligned(),
            Buffer::Vec(_) => false,
        }
    }
}

impl Into<Vec<u8>> for Buffer {
    fn into(self) -> Vec<u8> {
        if let Buffer::Vec(v) = self { v } else { unimplemented!() }
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(buf: Vec<u8>) -> Self {
        Buffer::Vec(buf)
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Self::Large(buf) => buf.as_ref(),
            Self::Vec(v) => &v,
        }
    }
}

impl AsRef<[u8]> for Buffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Large(buf) => buf.as_ref(),
            Self::Vec(v) => v.as_ref(),
        }
    }
}

impl AsMut<[u8]> for Buffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Large(buf) => buf.as_mut(),
            Self::Vec(v) => v.as_mut(),
        }
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Large(buf) => buf.as_mut(),
            Self::Vec(v) => v.as_mut(),
        }
    }
}
