use crate::callback_worker::IOWorkers;
use crate::context::{IOContext, IoEngineType};
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use crate::test::*;
use crossfire::BlockingTxTrait;
use io_buffer::{Buffer, rand_buffer};
use nix::errno::Errno;
use std::sync::mpsc::channel as unbounded;
extern crate md5;

#[test]
fn test_read_write_aio() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.fd;
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx =
        IOContext::<ClosureCb, _>::new(2, rx, &IOWorkers::new(1), IoEngineType::Aio).unwrap();

    let (done_tx, done_rx) = unbounded::<Box<IOEvent<ClosureCb>>>();
    let callback = Box::new(move |event: Box<IOEvent<ClosureCb>>| {
        let _ = done_tx.send(event);
    });
    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3.clone(), IOAction::Read, 100);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    match event.get_read_result() {
        Ok(_buffer2) => {
            panic!("expect error, but return ok");
        }
        Err(e) => {
            // Read the file out of boundary gets EINVAL
            assert_eq!(e, Errno::EINVAL);
            println!("expected error {}", e);
        }
    }
    //    let mut event = IOEvent::new(fd, buffer3, IOAction::Read, 4096);
    //    event.set_callback(ClosureCb(callback.clone()));
    //    tx.send(event).expect("submit");
    //    let event = done_rx.recv().unwrap();
    //    assert!(event.is_done());
    //    match event.get_read_result() {
    //        Ok(buffer2) => {
    //            assert_eq!(buffer2.len(), 0);
    //        }
    //        Err(e) => {
    //            panic!("unexpected error: {}", e);
    //        }
    //    }
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
            match event.get_read_result() {
                Ok(_buffer2) => {
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
fn test_read_write_uring() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.fd;
    let (tx, rx) = crossfire::mpsc::bounded_blocking(2);
    let _ctx =
        IOContext::<ClosureCb, _>::new(2, rx, &IOWorkers::new(1), IoEngineType::Uring).unwrap();

    let (done_tx, done_rx) = unbounded::<Box<IOEvent<ClosureCb>>>();
    let callback = Box::new(move |event: Box<IOEvent<ClosureCb>>| {
        let _ = done_tx.send(event);
    });
    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3, IOAction::Read, 100);
    event.set_callback(ClosureCb(callback.clone()));
    tx.send(event).expect("submit");
    let event = done_rx.recv().unwrap();
    assert!(event.is_done());
    match event.get_read_result() {
        Ok(_buffer2) => {
            // short read reached EOF
            assert_eq!(_buffer2.len(), 0);
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
            match event.get_read_result() {
                Ok(_buffer2) => {
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
