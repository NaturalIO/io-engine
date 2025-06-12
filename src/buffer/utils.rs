use rand::{Rng, distr::Alphanumeric, rng};

extern crate libc;

#[inline]
pub fn safe_copy(dst: &mut [u8], src: &[u8]) -> usize {
    let len = dst.len();
    let other_len = src.len();
    if other_len > len {
        dst.copy_from_slice(&src[0..len]);
        return len;
    } else if other_len < len {
        dst[0..other_len].copy_from_slice(src);
        return other_len;
    } else {
        dst.copy_from_slice(src);
        return len;
    }
}

#[inline(always)]
pub fn set_zero(dst: &mut [u8]) {
    unsafe {
        libc::memset(dst.as_mut_ptr() as *mut libc::c_void, 0, dst.len());
    }
}

// produce ascii random string
#[inline]
pub fn rand_buffer<T: AsMut<[u8]>>(dst: &mut T) {
    let s: &mut [u8] = dst.as_mut();
    let len = s.len();
    let mut rng = rng();
    for i in 0..len {
        s[i] = rng.sample(Alphanumeric) as u8;
    }
}

#[inline(always)]
pub fn is_all_zero(s: &[u8]) -> bool {
    for c in s {
        if *c != 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {

    extern crate md5;
    use super::*;

    #[test]
    fn test_safe_copy() {
        let buf1: [u8; 10] = [0; 10];
        let mut buf2: [u8; 10] = [1; 10];
        let mut buf3: [u8; 10] = [2; 10];
        let zero: usize = 0;
        // dst zero size copy should be protected
        assert_eq!(0, safe_copy(&mut buf2[0..zero], &buf3));
        assert_eq!(&buf2, &[1; 10]);
        assert_eq!(0, safe_copy(&mut buf2[10..], &buf3));
        assert_eq!(&buf2, &[1; 10]);
        // src zero size copy should be protected

        // src zero size copy ?
        assert_eq!(10, safe_copy(&mut buf2, &buf1));
        assert_eq!(buf1, buf2);
        assert_eq!(5, safe_copy(&mut buf2[5..], &buf3));
        assert_eq!(buf1[0..5], buf2[0..5]);
        assert_eq!(buf2[5..], buf3[5..]);
        assert_eq!(5, safe_copy(&mut buf3[0..5], &buf1));
        assert_eq!(buf2, buf3);
    }

    #[test]
    fn test_rand_buffer() {
        let mut buf1: [u8; 10] = [0; 10];
        let mut buf2: [u8; 10] = [0; 10];
        rand_buffer(&mut buf1);
        rand_buffer(&mut buf2);
        assert!(md5::compute(&buf1) != md5::compute(&buf2));
    }

    #[test]
    fn test_set_zero() {
        let mut buf1: [u8; 10] = [1; 10];
        set_zero(&mut buf1);
        for i in &buf1 {
            assert_eq!(*i, 0)
        }
    }
}
