use crate::callback_worker::IOWorkers;
use crate::context::{Driver, IOContext};
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use crate::test::*;
use crossfire::BlockingTxTrait;
use io_buffer::{Buffer, rand_buffer};
use std::os::fd::AsRawFd;
use std::sync::mpsc::channel as unbounded;
extern crate md5;

#[test]
fn test_read_write_aio() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx = IOContext::<ClosureCb, _, _>::new(2, rx, IOWorkers::new(1), Driver::Aio).unwrap();

    let (done_tx, done_rx) = unbounded::<IOEvent<ClosureCb>>();
    let callback = Box::new(move |event: IOEvent<ClosureCb>| {
        let _ = done_tx.send(event);
    });
    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3.clone(), IOAction::Read, 100);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    // Get actual bytes read
    let n = event.get_result().unwrap();
    match event.get_read_result() {
        Ok(_buffer2) => {
            // NOTE: although the offset is not aligned,
            // Read the file out of boundary gets res=0
            assert_eq!(n, 0);
        }
        Err(e) => {
            panic!("unexpected error {}", e);
        }
    }
    for _j in 0..100 {
        for i in 0..10 {
            // write
            // TODO randomize
            let mut buffer = Buffer::aligned(4096).unwrap();
            rand_buffer(&mut buffer);
            let digest = md5::compute(&buffer);
            let mut event = IOEvent::new(fd, buffer, IOAction::Write, 4096 * i as i64);
            event.set_callback(ClosureCb(callback.clone()));
            tx.send(event).expect("submit");
            let event = done_rx.recv().unwrap();
            assert!(event.is_done());
            // read
            let buffer2 = Buffer::aligned(4096).unwrap();
            let mut event = IOEvent::new(fd, buffer2, IOAction::Read, 4096 * i as i64);
            event.set_callback(ClosureCb(callback.clone()));
            tx.send(event).expect("submit");
            let event = done_rx.recv().unwrap();
            assert!(event.is_done());
            // Get bytes read first before consuming event
            let n = event.get_result().unwrap();
            match event.get_read_result() {
                Ok(mut _buffer2) => {
                    // Set buffer length based on actual bytes read
                    _buffer2.set_len(n);
                    let _digest = md5::compute(&_buffer2);
                    assert_eq!(_digest, digest);
                }
                Err(e) => {
                    panic!("error: {}", e);
                }
            }
        }
    }
}

#[test]
fn test_fallocate_fsync_aio() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx = IOContext::<ClosureCb, _, _>::new(2, rx, IOWorkers::new(1), Driver::Aio).unwrap();

    let (done_tx, done_rx) = unbounded::<IOEvent<ClosureCb>>();
    let callback = Box::new(move |event: IOEvent<ClosureCb>| {
        let _ = done_tx.send(event);
    });

    // Test fallocate
    let fallocate_len = 8192;
    let mut event = IOEvent::new_no_buf(fd, IOAction::Alloc, 0, fallocate_len);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit fallocate");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_result().is_ok());

    // Verify file size after fallocate
    let metadata = std::fs::metadata(temp_file.as_ref()).unwrap();
    assert_eq!(metadata.len(), fallocate_len);

    // Verify fallocate by writing to the allocated space
    let mut buffer = Buffer::aligned(4096).unwrap();
    rand_buffer(&mut buffer);
    let mut event = IOEvent::new(fd, buffer.clone(), IOAction::Write, 4096);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit write");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_write_result().is_ok());

    // Test fsync
    let mut event = IOEvent::new_no_buf(fd, IOAction::Fsync, 0, 0);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit fsync");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_result().is_ok());
}

#[test]
fn test_fallocate_fsync_uring() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx = IOContext::<ClosureCb, _, _>::new(2, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded::<IOEvent<ClosureCb>>();
    let callback = Box::new(move |event: IOEvent<ClosureCb>| {
        let _ = done_tx.send(event);
    });

    // Test fallocate
    let fallocate_len = 8192;
    let mut event = IOEvent::new_no_buf(fd, IOAction::Alloc, 0, fallocate_len);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit fallocate");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_result().is_ok());

    // Verify file size after fallocate
    let metadata = std::fs::metadata(temp_file.as_ref()).unwrap();
    assert_eq!(metadata.len(), fallocate_len);

    // Verify fallocate by writing to the allocated space
    let mut buffer = Buffer::aligned(4096).unwrap();
    rand_buffer(&mut buffer);
    let mut event = IOEvent::new(fd, buffer.clone(), IOAction::Write, 4096);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit write");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_write_result().is_ok());

    // Test fsync
    let mut event = IOEvent::new_no_buf(fd, IOAction::Fsync, 0, 0);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit fsync");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    assert!(event.get_result().is_ok());
}

#[test]
fn test_read_write_uring() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx = IOContext::<ClosureCb, _, _>::new(2, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded::<IOEvent<ClosureCb>>();
    let callback = Box::new(move |event: IOEvent<ClosureCb>| {
        let _ = done_tx.send(event);
    });
    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3, IOAction::Read, 100);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    // Get actual bytes read
    let n = event.get_result().unwrap();
    match event.get_read_result() {
        Ok(_buffer2) => {
            // short read reached EOF
            assert_eq!(n, 0);
        }
        Err(e) => {
            unreachable!("error: {}", e);
        }
    }
    for _j in 0..100 {
        for i in 0..10 {
            // write
            // TODO randomize
            let mut buffer = Buffer::aligned(4096).unwrap();
            rand_buffer(&mut buffer);
            let digest = md5::compute(&buffer);
            let mut event = IOEvent::new(fd, buffer, IOAction::Write, 4096 * i as i64);
            event.set_callback(ClosureCb(callback.clone()));
            tx.send(event).expect("submit");
            let event = done_rx.recv().unwrap();
            assert!(event.is_done());
            match event.get_write_result() {
                Ok(()) => {}
                Err(e) => {
                    panic!("write error: {}", e);
                }
            }
            // read
            let buffer2 = Buffer::aligned(4096).unwrap();
            let mut event = IOEvent::new(fd, buffer2, IOAction::Read, 4096 * i as i64);
            event.set_callback(ClosureCb(callback.clone()));
            tx.send(event).expect("submit");
            let event = done_rx.recv().unwrap();
            assert!(event.is_done());
            // Get bytes read first before consuming event
            let n = event.get_result().unwrap();
            match event.get_read_result() {
                Ok(mut _buffer2) => {
                    // Set buffer length based on actual bytes read
                    _buffer2.set_len(n);
                    let _digest = md5::compute(&_buffer2);
                    assert_eq!(_digest, digest);
                }
                Err(e) => {
                    panic!("error: {}", e);
                }
            }
        }
    }
}
