use crate::callback_worker::IOWorkers;
use crate::context::{Driver, IOContext};
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use crate::test::*;
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
    let _ctx = IOContext::<ClosureCb, _, _>::new(1, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded();

    let cb = ClosureCb(Box::new(move |event: IOEvent<ClosureCb>| {
        assert!(event.get_result().is_ok());
        done_tx.send(()).unwrap();
    }));

    let mut event = IOEvent::new_no_buf(fd, IOAction::Alloc, 0, 4096);
    event.set_callback(cb);
    tx.send(event).unwrap();

    // Wait for completion
    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();

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
    let _ctx = IOContext::<ClosureCb, _, _>::new(1, rx, IOWorkers::new(1), Driver::Uring).unwrap();

    let (done_tx, done_rx) = unbounded();

    let cb = ClosureCb(Box::new(move |event: IOEvent<ClosureCb>| {
        assert!(event.get_result().is_ok());
        done_tx.send(()).unwrap();
    }));

    let mut event = IOEvent::new_no_buf(fd, IOAction::Fsync, 0, 0);
    event.set_callback(cb);
    tx.send(event).unwrap();

    // Wait for completion
    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}
