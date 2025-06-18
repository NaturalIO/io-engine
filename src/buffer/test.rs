use super::*;
use std::time::Instant;

#[test]
fn test_buffer_alloc() {
    assert_eq!(std::mem::size_of::<Buffer>(), 16);
    {
        let mut buffer = Buffer::aligned(1024 * 1024).unwrap();
        buffer[1024 * 1024 - 1] = 5;
        assert_eq!(buffer.len(), 1024 * 1024);
        assert_eq!(buffer.capacity(), 1024 * 1024);
        assert_eq!(buffer.is_aligned(), true);
        assert!(buffer.is_mutable());
        assert!(buffer.is_owned());
    }
    {
        let mut buffer = Buffer::alloc(1024 * 1024 * 10).unwrap();
        buffer[1024 * 1024 - 1] = 5;
        assert_eq!(buffer.len(), 1024 * 1024 * 10);
        assert_eq!(buffer.capacity(), 1024 * 1024 * 10);
        assert_eq!(buffer.is_aligned(), false);
        assert!(buffer.is_mutable());
        assert!(buffer.is_owned());
    }
    {
        let start_ts = Instant::now();
        let round = 100000;
        let mut v = Vec::<Buffer>::with_capacity(round);
        for _i in 0..round {
            let _buffer = Buffer::aligned(4096).unwrap();
            v.push(_buffer);
        }
        let end_ts = Instant::now();
        println!(
            "alloc + free speed {} /sec",
            ((round) as f64) / (end_ts.duration_since(start_ts).as_secs_f64())
        )
    }
}

#[test]
fn test_copy_and_clean() {
    {
        let mut buffer = Buffer::aligned(4096).unwrap();
        rand_buffer(&mut buffer);
        let src: [u8; 100] = [1; 100];
        buffer.copy_and_clean(0, &src);
        assert_eq!(buffer.len(), 4096);
        assert_eq!(buffer.capacity(), 4096);
        assert_eq!(&buffer[0..100], &src[0..]);
        for b in &buffer[100..4096] {
            assert_eq!(*b, 0);
        }
    }
    {
        let mut buffer = Buffer::alloc(4096).unwrap();
        rand_buffer(&mut buffer);
        let src: [u8; 100] = [1; 100];
        buffer.copy_and_clean(1024, &src);
        assert_eq!(&buffer[1024..1124], &src[0..]);
        for b in &buffer[0..1024] {
            assert_eq!(*b, 0);
        }
        for b in &buffer[1124..4096] {
            assert_eq!(*b, 0);
        }
    }
    {
        let mut buffer = Buffer::alloc(4096).unwrap();
        rand_buffer(&mut buffer);
        let src: [u8; 1024] = [1; 1024];
        buffer.copy_and_clean(4096 - 1024, &src);
        assert_eq!(&buffer[(4096 - 1024)..], &src[0..]);
        for b in &buffer[0..4096 - 1024] {
            assert_eq!(*b, 0);
        }
    }
    {
        let mut buffer = Buffer::alloc(1024).unwrap();
        rand_buffer(&mut buffer);
        let src: [u8; 1024] = [1; 1024];
        buffer.copy_and_clean(0, &src);
        assert_eq!(&buffer[0..], &src[0..]);
    }
}

#[test]
fn test_set_zero() {
    let mut buffer = Buffer::alloc(100).unwrap();
    rand_buffer(&mut buffer);
    println!("buffer: {}", unsafe { str::from_utf8_unchecked(&buffer) });
    buffer.zero();
    println!("buffer: {}", unsafe { str::from_utf8_unchecked(&buffer) });
    for b in &buffer[0..100] {
        assert_eq!(*b, 0);
    }
    rand_buffer(&mut buffer);
    println!("buffer: {}", unsafe { str::from_utf8_unchecked(&buffer) });
    buffer.set_zero(20, 20);
    assert_eq!(buffer.len(), 100);
    assert_eq!(buffer.capacity(), 100);
    for b in &buffer[20..40] {
        assert_eq!(*b, 0);
    }
    assert_eq!(buffer.len(), 100);
    assert_eq!(buffer.capacity(), 100);
    println!("buffer: {}", unsafe { str::from_utf8_unchecked(&buffer) });
}

#[test]
fn test_set_len() {
    let mut buffer = Buffer::alloc(100).unwrap();
    rand_buffer(&mut buffer);
    let buffer1 = buffer.clone();
    buffer.set_len(50);
    assert_eq!(&buffer[..], &buffer1[0..50]);
    assert_eq!(buffer.len(), 50);
    assert_eq!(buffer.capacity(), 100);
    assert_eq!(buffer1.len(), 100);
    assert_eq!(buffer1.capacity(), 100);
    let buffer2 = buffer.clone();
    assert_eq!(buffer2.len(), 50);
    assert_eq!(buffer2.capacity(), 100);
    assert!(buffer2.is_mutable());
    assert!(buffer1.is_mutable());
    assert!(buffer.is_mutable());
    assert!(buffer.is_owned());
    assert!(buffer1.is_owned());
    assert!(buffer2.is_owned());
}

#[test]
fn test_buf_convertion() {
    let buf = Vec::with_capacity(1000);
    let mut buf2: Buffer = buf.into();
    assert!(buf2.is_owned());
    assert!(buf2.is_mutable());
    assert_eq!(buf2.len(), 0);
    assert_eq!(buf2.capacity(), 1000);
    buf2.set_len(500);
    rand_buffer(&mut buf2);
    let buf_v: Vec<u8> = buf2.into();
    assert_eq!(buf_v.len(), 500);
    assert_eq!(buf_v.capacity(), 1000);
}

#[test]
fn test_c_ref_clone_and_drop() {
    let mut buffer = Buffer::alloc(1024).unwrap();
    rand_buffer(&mut buffer);
    let mut buffer_ref =
        Buffer::from_c_ref_mut(buffer.get_raw_mut() as *mut libc::c_void, buffer.len());
    assert!(!buffer_ref.is_owned());
    assert!(buffer_ref.is_mutable());
    assert!(buffer.is_owned());
    assert!(buffer.is_mutable());
    assert_eq!(&buffer[..], &buffer_ref[..]);
    let buffer2 = buffer_ref.clone();
    assert!(buffer2.is_owned());
    assert!(buffer2.is_mutable());
    assert_eq!(&buffer[..], &buffer2[..]);
    rand_buffer(&mut buffer_ref);
    assert_eq!(&buffer[..], &buffer_ref[..]);
    buffer_ref.set_len(10);
    assert_eq!(buffer_ref.len(), 10);
    assert_eq!(buffer_ref.capacity(), 1024);
    assert_eq!(&buffer[..10], &buffer_ref[..]);
    assert!(!buffer_ref.is_owned());
    assert!(buffer_ref.is_mutable());
    drop(buffer_ref);
    let mut equals = true;
    for i in 0..buffer2.len() {
        if buffer[i] != buffer2[i] {
            equals = false;
        }
    }
    assert!(!equals);
}

#[test]
fn test_c_ref_const() {
    let mut buffer = Buffer::alloc(1024).unwrap();
    rand_buffer(&mut buffer);
    let mut buffer_ref =
        Buffer::from_c_ref_const(buffer.get_raw() as *const libc::c_void, buffer.len());
    assert_eq!(buffer.len(), 1024);
    assert_eq!(buffer.capacity(), 1024);
    assert!(buffer.is_mutable());
    assert!(buffer.is_owned());
    assert!(!buffer_ref.is_mutable());
    assert!(!buffer_ref.is_owned());
    let buffer2 = buffer_ref.clone();
    buffer_ref.set_len(50);
    assert!(!buffer_ref.is_mutable());
    assert!(!buffer_ref.is_owned());
    assert_eq!(buffer_ref.len(), 50);
    assert_eq!(buffer_ref.capacity(), 1024);
    assert_eq!(buffer2.len(), 1024);
    assert_eq!(buffer2.capacity(), 1024);
    assert_eq!(&buffer[..], &buffer2[..]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn test_c_ref_mutability() {
    let mut buffer = Buffer::alloc(1024).unwrap();
    rand_buffer(&mut buffer);
    let mut buffer_ref_const =
        Buffer::from_c_ref_const(buffer.get_raw() as *const libc::c_void, buffer.len());
    assert!(!buffer_ref_const.is_mutable());
    assert!(!buffer_ref_const.is_owned());
    rand_buffer(&mut buffer_ref_const);
}
