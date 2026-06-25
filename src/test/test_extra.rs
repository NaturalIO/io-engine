use crate::callback_worker::InlineClosure;
use crate::context::{Driver, setup};
use crate::tasks::{IOAction, IOEvent};
use crate::test::*;
use crossfire::mpsc;
use io_buffer::{Buffer, rand_buffer};
use rstest::rstest;
use rustix::io::Errno;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;

#[rstest]
#[case(Driver::Aio)]
#[case(Driver::Uring)]
fn test_fallocate(#[case] driver: Driver) {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();

    let (tx, rx) = mpsc::bounded_blocking(1);
    let (done_tx, done_rx) = mpsc::unbounded_blocking::<Result<Option<Buffer>, Errno>>();
    let worker = InlineClosure(Box::new(move |(), _offset, res| {
        done_tx.send(res).unwrap();
    }));
    setup::<(), _, _>(1, rx, worker, driver).unwrap();

    let mut event = IOEvent::new_fallocate(fd, 0, 4096);
    event.set_args(());
    tx.send(Box::new(event)).unwrap();
    assert!(done_rx.recv().unwrap().is_ok());

    let metadata = std::fs::metadata(temp_file.as_ref()).unwrap();
    assert_eq!(metadata.size(), 4096);

    // Verify fallocate by writing to the allocated space
    let mut buffer = Buffer::aligned(4096).unwrap();
    rand_buffer(&mut buffer);
    let mut event = IOEvent::new(fd, buffer, IOAction::Write, 0);
    event.set_args(());
    tx.send(Box::new(event)).expect("submit write");
    let res = done_rx.recv().unwrap();
    assert!(res.is_ok());
}

#[rstest]
#[case(Driver::Aio)]
#[case(Driver::Uring)]
fn test_fsync(#[case] driver: Driver) {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();

    let (tx, rx) = mpsc::bounded_blocking(1);
    let (done_tx, done_rx) = mpsc::unbounded_blocking::<Result<Option<Buffer>, Errno>>();
    let worker = InlineClosure(Box::new(move |(), _offset, res| {
        done_tx.send(res).unwrap();
    }));
    setup::<(), _, _>(1, rx, worker, driver).unwrap();

    let mut event = IOEvent::new_fsync(fd);
    event.set_args(());
    tx.send(Box::new(event)).unwrap();

    // Wait for completion
    assert!(done_rx.recv().expect("done").is_ok());
}
