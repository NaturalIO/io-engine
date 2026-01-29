use crate::callback_worker::IOWorkers;
use crate::context::{Driver, IOContext};
use crate::merge::MergeSubmitter;
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
    let mut m_write =
        MergeSubmitter::<ClosureCb, _>::new(fd, sender.clone(), merge_size_limit, IOAction::Write);

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
        let mut m_read = MergeSubmitter::<ClosureCb, _>::new(
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

#[test]
fn test_event_merge_buffer_logic() {
    setup_log();
    let merge_size_limit = 4 * 1024;
    let mut buffer = crate::merge::MergeBuffer::<ClosureCb>::new(merge_size_limit);

    let fd = 100; // Dummy fd

    // --- Test empty flush ---
    assert!(buffer.flush(fd, IOAction::Write).is_none());
    assert_eq!(buffer.len(), 0);

    // --- Scenario 1: Add a single event ---
    let event1 = IOEvent::new(fd, Buffer::aligned(1024).unwrap(), IOAction::Write, 0);
    // clone for later comparison
    let event1_clone: IOEvent<ClosureCb> =
        IOEvent::new(fd, Buffer::aligned(1024).unwrap(), IOAction::Write, 0);
    assert!(buffer.may_add_event(&event1));
    buffer.push_event(event1);
    assert_eq!(buffer.len(), 1);

    // Flush single event
    let single_event_opt = buffer.flush(fd, IOAction::Write);
    assert!(single_event_opt.is_some());
    let single_event = single_event_opt.unwrap();
    assert_eq!(single_event.offset, event1_clone.offset);
    assert_eq!(single_event.get_size(), event1_clone.get_size());
    assert_eq!(buffer.len(), 0);

    // --- Scenario 2: Add contiguous events ---
    // Event 2: offset 0, size 1KB
    let event2 = IOEvent::new(fd, Buffer::aligned(1024).unwrap(), IOAction::Write, 0);
    buffer.push_event(event2);

    // Event 3: offset 1KB, size 1KB (contiguous)
    let event3 = IOEvent::new(fd, Buffer::aligned(1024).unwrap(), IOAction::Write, 1024);
    buffer.push_event(event3);
    assert_eq!(buffer.len(), 2);

    // --- Scenario 3: Add non-contiguous event ---
    // Event 4: offset 3KB, size 1KB (non-contiguous)
    let event4 = IOEvent::new(fd, Buffer::aligned(1024).unwrap(), IOAction::Write, 3072);
    assert!(!buffer.may_add_event(&event4));

    // Now, flush the buffered events and check them
    assert_eq!(buffer.len(), 2);
    let merged_event_opt = buffer.flush(fd, IOAction::Write);
    assert!(merged_event_opt.is_some());
    let merged_event = merged_event_opt.unwrap();
    assert_eq!(merged_event.offset, 0);
    assert_eq!(merged_event.get_size(), 2048);
    assert_eq!(buffer.len(), 0);

    // The buffer is now empty. We can add event4.
    assert!(buffer.may_add_event(&event4));
    buffer.push_event(event4);
    assert_eq!(buffer.len(), 1);

    // --- Scenario 4: Add event that makes buffer full ---
    // Event 5: offset 4096, size 3072. Contiguous with event4 (offset 3072, size 1024).
    // Total size would be 1024 + 3072 = 4096, which is exactly merge_size_limit
    let event5 = IOEvent::new(fd, Buffer::aligned(3072).unwrap(), IOAction::Write, 4096);

    assert!(buffer.may_add_event(&event5));
    let is_full = buffer.push_event(event5);
    assert!(is_full);
    assert_eq!(buffer.len(), 2);

    let merged_event_opt_2 = buffer.flush(fd, IOAction::Write);
    assert!(merged_event_opt_2.is_some());
    let merged_event_2 = merged_event_opt_2.unwrap();
    assert_eq!(merged_event_2.offset, 3072);
    assert_eq!(merged_event_2.get_size(), 4096);
    assert_eq!(buffer.len(), 0);
}
