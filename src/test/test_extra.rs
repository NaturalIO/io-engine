use crate::callback_worker::IOWorkers;
use crate::context::{Driver, setup};
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use crate::test::*;
use nix::errno::Errno;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::sync::mpsc::channel as unbounded;
use std::time::Duration;

#[test]
fn test_fallocate() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();

    let (tx, rx) = crossfire::mpsc::bounded_blocking(1);
    setup::<ClosureCb, _, _>(1, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded::<Result<(), Errno>>();

    let cb = ClosureCb(Box::new(move |_offset, res| {
        assert!(res.is_ok());
        done_tx.send(res.map(|_| ())).unwrap();
    }));

    let mut event = IOEvent::new_no_buf(fd, IOAction::Alloc, 0, 4096);
    event.set_callback(cb);
    tx.send(Box::new(event)).unwrap();
    assert!(done_rx.recv().unwrap().is_ok());

    let metadata = std::fs::metadata(temp_file.as_ref()).unwrap();
    assert_eq!(metadata.size(), 4096);
}

#[test]
fn test_fsync() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.as_raw_fd();

    let (tx, rx) = crossfire::mpsc::bounded_blocking(1);
    setup::<ClosureCb, _, _>(1, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded::<Result<(), Errno>>();

    let cb = ClosureCb(Box::new(move |_offset, res| {
        assert!(res.is_ok());
        done_tx.send(res.map(|_| ())).unwrap();
    }));

    let mut event = IOEvent::new_no_buf(fd, IOAction::Fsync, 0, 0);
    event.set_callback(cb);
    tx.send(Box::new(event)).unwrap();

    // Wait for completion
    assert!(done_rx.recv().expect("done").is_ok());
}
