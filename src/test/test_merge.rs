use crate::callback_worker::IOWorkers;
use crate::context::{Driver, IOContext};
use crate::merge::IOMergeSubmitter;
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use std::os::fd::{AsRawFd, RawFd};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::test::*;
use atomic_waitgroup::WaitGroup;
use crossfire::BlockingTxTrait;
use io_buffer::Buffer;

#[test]
fn test_merged_submit_aio() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());

    println!("created temp file fd={}", owned_fd.as_raw_fd());
    let fd = owned_fd.as_raw_fd();
    let (tx, rx) = crossfire::mpsc::bounded_blocking(128);
    let _ctx = IOContext::<ClosureCb, _, _>::new(128, rx, IOWorkers::new(2), Driver::Aio).unwrap();

    _test_merge_submit(fd, tx.clone(), 1024, 1024, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 512, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 256, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 64 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 32 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 1 * 1024);

    std::thread::sleep(Duration::from_secs(1));
}

#[test]
fn test_merged_submit_uring() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());

    let fd = owned_fd.as_raw_fd();
    println!("created temp file fd={}", fd);
    let (tx, rx) = crossfire::mpsc::bounded_blocking(128);
    let _ctx =
        IOContext::<ClosureCb, _, _>::new(128, rx, IOWorkers::new(2), Driver::Uring).unwrap();

    _test_merge_submit(fd, tx.clone(), 1024, 1024, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 512, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 256, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 64 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 32 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 16 * 1024);
    _test_merge_submit(fd, tx.clone(), 1024, 64, 1 * 1024);

    std::thread::sleep(Duration::from_secs(1));
}

fn _test_merge_submit<S: BlockingTxTrait<IOEvent<ClosureCb>> + Clone + Send + Sync + 'static>(
    fd: RawFd, sender: S, io_size: usize, batch_num: usize, merge_size_limit: usize,
) {
    println!("test_merged_submit {} {} {}", io_size, batch_num, merge_size_limit);
    let mut m_write = IOMergeSubmitter::<ClosureCb, _>::new(
        fd,
        sender.clone(),
        merge_size_limit,
        IOAction::Write,
    );

    let rt =
        tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(4).build().unwrap();

    let mut buf_all = Vec::<u8>::with_capacity(batch_num * io_size);
    buf_all.resize_with(batch_num * io_size, || rand::random::<u8>());
    let md51 = md5::compute(&buf_all);
    println!("buf all md5 {:?}", md51);

    rt.block_on(async move {
        let wg = WaitGroup::new();

        let _wg = wg.clone();
        let write_cb = Box::new(move |_event: IOEvent<ClosureCb>| {
            _wg.done();
        });

        for i in (0..batch_num / 2).step_by(2) {
            let mut buf = Buffer::aligned(io_size as i32).unwrap();
            buf.copy_from(0, &buf_all[i * io_size..(i + 1) * io_size]);
            let mut event = IOEvent::new(fd, buf, IOAction::Write, (i * io_size) as i64);
            event.set_callback(ClosureCb(write_cb.clone()));
            wg.add(1);
            m_write.add_event(event).expect("add_event");
        }
        println!("-- write 1");

        for i in batch_num / 2..batch_num {
            let mut buf = Buffer::aligned(io_size as i32).unwrap();
            buf.copy_from(0, &buf_all[i * io_size..(i + 1) * io_size]);
            let mut event = IOEvent::new(fd, buf, IOAction::Write, (i * io_size) as i64);
            event.set_callback(ClosureCb(write_cb.clone()));
            wg.add(1);
            m_write.add_event(event).expect("add_event");
        }
        println!("---write 2");

        for i in (1..(batch_num / 2 + 1)).step_by(2) {
            let mut buf = Buffer::aligned(io_size as i32).unwrap();
            buf.copy_from(0, &buf_all[i * io_size..(i + 1) * io_size]);
            let mut event = IOEvent::new(fd, buf, IOAction::Write, (i * io_size) as i64);
            event.set_callback(ClosureCb(write_cb.clone()));
            wg.add(1);
            m_write.add_event(event).expect("add_event");
        }
        m_write.flush().expect("flush");
        wg.wait().await;
        println!("wriiten");
        // TODO assert f.size

        let read_buf = Arc::new(Mutex::new(Buffer::aligned((batch_num * io_size) as i32).unwrap()));
        let mut m_read = IOMergeSubmitter::<ClosureCb, _>::new(
            fd,
            sender.clone(),
            merge_size_limit,
            IOAction::Read,
        );
        println!("--- reading");

        for i in (0..batch_num / 2).step_by(2) {
            let buf = Buffer::aligned(io_size as i32).unwrap();
            let mut event = IOEvent::new(fd, buf, IOAction::Read, (i * io_size) as i64);
            let _read_buf = read_buf.clone();
            let offset = i * io_size;
            let _wg = wg.clone();
            event.set_callback(ClosureCb(Box::new(move |mut _event: IOEvent<ClosureCb>| {
                let mut _buf_all = _read_buf.lock().unwrap();
                match _event.get_read_result() {
                    Ok(buffer) => {
                        _buf_all.copy_from(offset, buffer.as_ref());
                    }
                    Err(e) => {
                        panic!("read error: {}", e);
                    }
                }
                _wg.done();
            })));
            wg.add(1);
            m_read.add_event(event).expect("add_event");
        }

        println!("--- read 1");

        for i in batch_num / 2..batch_num {
            let buf = Buffer::aligned(io_size as i32).unwrap();
            let mut event = IOEvent::new(fd, buf, IOAction::Read, (i * io_size) as i64);
            let _read_buf = read_buf.clone();
            let offset = i * io_size;
            let _wg = wg.clone();
            event.set_callback(ClosureCb(Box::new(move |mut _event: IOEvent<ClosureCb>| {
                let mut _buf_all = _read_buf.lock().unwrap();
                match _event.get_read_result() {
                    Ok(buffer) => {
                        _buf_all.copy_from(offset, buffer.as_ref());
                    }
                    Err(e) => {
                        panic!("read error: {}", e);
                    }
                }
                _wg.done();
            })));
            wg.add(1);
            m_read.add_event(event).expect("add_event");
        }

        println!("--- read 2");
        for i in (1..(batch_num / 2 + 1)).step_by(2) {
            let buf = Buffer::aligned(io_size as i32).unwrap();
            let mut event = IOEvent::new(fd, buf, IOAction::Read, (i * io_size) as i64);
            let _read_buf = read_buf.clone();
            let offset = i * io_size;
            let _wg = wg.clone();
            event.set_callback(ClosureCb(Box::new(move |mut _event: IOEvent<ClosureCb>| {
                let mut _buf_all = _read_buf.lock().unwrap();
                match _event.get_read_result() {
                    Ok(buffer) => {
                        _buf_all.copy_from(offset, buffer.as_ref());
                    }
                    Err(e) => {
                        panic!("read error: {}", e);
                    }
                }
                _wg.done();
            })));
            wg.add(1);
            m_read.add_event(event).expect("add_event");
        }
        println!("--- read 3");
        m_read.flush().expect("flush");
        wg.wait().await;

        assert_eq!(md51, md5::compute(read_buf.lock().unwrap().as_ref()));
    });
}
