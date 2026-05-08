use crate::callback_worker::InlineClosure;
use crate::context::{Driver, setup};
use crate::tasks::{IOAction, IOEvent};
use crate::test::*;
use crossfire::mpsc;
use io_buffer::{Buffer, rand_buffer};
use rstest::rstest;
use rustix::io::Errno;
use std::os::fd::AsRawFd;
extern crate md5;

#[rstest]
#[case(Driver::Aio)]
#[case(Driver::Uring)]
fn test_read_write(#[case] driver: Driver) {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = mpsc::bounded_blocking(2);

    let (done_tx, done_rx) = mpsc::unbounded_blocking::<Result<Option<Buffer>, Errno>>();
    let worker = InlineClosure(Box::new(move |(), _offset, res| {
        let _ = done_tx.send(res);
    }));
    setup::<(), _, _>(2, rx, worker, driver).unwrap();

    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3, IOAction::Read, 100);
    event.set_args(());
    tx.send(Box::new(event)).expect("submit");
    let res = done_rx.recv().unwrap();
    match res {
        Ok(Some(_buffer2)) => {
            // NOTE: although the offset is not aligned,
            // Read the file out of boundary gets res=0
            assert_eq!(_buffer2.len(), 0);
        }
        Ok(None) => {
            panic!("unexpected empty buffer");
        }
        Err(e) => {
            panic!("unexpected error: {:?}", e);
        }
    }
    for _j in 0..100 {
        for i in 0..10 {
            // write
            let mut buffer = Buffer::aligned(4096).unwrap();
            rand_buffer(&mut buffer);
            let digest = md5::compute(&buffer);
            let mut event = IOEvent::new(fd, buffer, IOAction::Write, 4096 * i as i64);
            event.set_args(());
            tx.send(Box::new(event)).expect("submit");
            let res = done_rx.recv().unwrap();
            assert!(res.is_ok());
            // read
            let buffer2 = Buffer::aligned(4096).unwrap();
            let mut event = IOEvent::new(fd, buffer2, IOAction::Read, 4096 * i as i64);
            event.set_args(());
            tx.send(Box::new(event)).expect("submit");
            let res = done_rx.recv().unwrap();
            match res {
                Ok(Some(_buffer2)) => {
                    let _digest = md5::compute(&_buffer2);
                    assert_eq!(_digest, digest);
                }
                Ok(None) => {
                    panic!("empty buffer");
                }
                Err(_) => {
                    panic!("error");
                }
            }
        }
    }
}
