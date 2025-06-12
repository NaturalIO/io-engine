use super::utils::safe_copy;
use libc;
use nix::errno::Errno;
use std::slice;

#[repr(C, align(1))]
pub struct BufferLarge {
    pub buf_ptr: *mut libc::c_void,
    pub size: u32,
    pub owned: bool,
    pub mutable: bool,
}

unsafe impl Send for BufferLarge {}

unsafe impl Sync for BufferLarge {}

pub const MIN_ALIGN: usize = 512;

fn is_aligned(offset: usize, size: usize) -> bool {
    return (offset & (MIN_ALIGN - 1) == 0) && (size & (MIN_ALIGN - 1) == 0);
}

impl BufferLarge {
    /// Allocate a buffer.
    #[inline]
    pub fn alloc(align: usize, size: usize) -> Result<BufferLarge, Errno> {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
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
        Ok(Self {
            buf_ptr: ptr,
            size: size as u32,
            owned: true,
            mutable: true,
        })
    }

    /// Wrap a mutable buffer passed from c code, without owner ship.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle
    #[inline]
    pub fn from_c_ref_mut(ptr: *mut libc::c_void, size: usize) -> Self {
        assert!(ptr != std::ptr::null_mut());
        Self {
            buf_ptr: ptr,
            size: size as u32,
            owned: false,
            mutable: true,
        }
    }

    /// Wrap a const buffer passed from c code, without owner ship.
    ///
    /// NOTE: will not free on drop. You have to ensure the buffer valid throughout the lifecycle
    #[inline]
    pub fn from_c_ref_const(ptr: *const libc::c_void, size: usize) -> Self {
        assert!(ptr != std::ptr::null());
        Self {
            buf_ptr: unsafe { std::mem::transmute(ptr) },
            size: size as u32,
            owned: false,
            mutable: false,
        }
    }

    #[inline(always)]
    pub fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.buf_ptr as *const u8, self.size as usize) }
    }

    /// Will only check mutability and debug mode, skip on release mode for speed
    #[inline(always)]
    pub fn as_mut(&mut self) -> &mut [u8] {
        #[cfg(debug_assertions)]
        {
            if !self.mutable {
                panic!("Cannot change a mutable buffer")
            }
        }
        unsafe { slice::from_raw_parts_mut(self.buf_ptr as *mut u8, self.size as usize) }
    }

    /// Check this buffer usable by aio
    #[inline(always)]
    pub fn is_aligned(&self) -> bool {
        is_aligned(self.buf_ptr as usize, self.size as usize)
    }
}

impl Clone for BufferLarge {
    fn clone(&self) -> Self {
        let mut new_buf = BufferLarge::alloc(512, self.size as usize).unwrap();
        safe_copy(new_buf.as_mut(), self.as_ref());
        new_buf
    }
}

impl Drop for BufferLarge {
    fn drop(&mut self) {
        if self.owned {
            unsafe {
                libc::free(self.buf_ptr);
            }
        }
    }
}
