use super::*;
use std::time::Instant;

#[test]
fn test_buffer_alloc() {
    {
        let mut buffer = Buffer::aligned(1024 * 1024).unwrap();
        buffer[1024 * 1024 - 1] = 5;
        assert_eq!(buffer.len(), 1024 * 1024);
        assert_eq!(buffer.is_aligned(), true)
    }
    {
        let mut buffer = Buffer::alloc(1024 * 1024 * 10).unwrap();
        buffer[1024 * 1024 - 1] = 5;
        assert_eq!(buffer.len(), 1024 * 1024 * 10);
        assert_eq!(buffer.is_aligned(), false)
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
    for b in &buffer[20..40] {
        assert_eq!(*b, 0);
    }
    println!("buffer: {}", unsafe { str::from_utf8_unchecked(&buffer) });
}

#[test]
fn test_buf_convertion() {
    let buf_enum = {
        let buf = Vec::with_capacity(1000);
        let buf2: Buffer = buf.into();
        buf2
    };
    {
        let _buf_v: Vec<u8> = buf_enum.into();
    }
}

#[test]
fn test_c_ref_clone_and_drop() {
    let mut buffer = Buffer::alloc(1024).unwrap();
    rand_buffer(&mut buffer);
    let mut buffer_ref =
        Buffer::from_c_ref_mut(buffer.get_raw_mut() as *mut libc::c_void, buffer.len());
    assert_eq!(&buffer[..], &buffer_ref[..]);
    let buffer2 = buffer_ref.clone();
    assert_eq!(&buffer[..], &buffer2[..]);
    rand_buffer(&mut buffer_ref);
    assert_eq!(&buffer[..], &buffer_ref[..]);
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
#[cfg(debug_assertions)]
#[should_panic]
fn test_c_ref_mutability() {
    let mut buffer = Buffer::alloc(1024).unwrap();
    rand_buffer(&mut buffer);
    let mut buffer_ref_const =
        Buffer::from_c_ref_const(buffer.get_raw() as *const libc::c_void, buffer.len());
    rand_buffer(&mut buffer_ref_const);
}
