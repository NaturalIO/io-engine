use crate::callback_worker::IOWorkers;
use crate::context::{IOChannelType, IOContext};
use crate::tasks::{ClosureCb, IOAction, IOEvent};
use crate::test::*;
use crossbeam::channel::unbounded;
use io_buffer::{Buffer, rand_buffer};
use nix::errno::Errno;
extern crate md5;

#[test]
fn test_read_write() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.fd;
    let ctx = IOContext::<ClosureCb>::new(2, &IOWorkers::new(1)).unwrap();

    let (done_tx, done_rx) = unbounded::<Box<IOEvent<ClosureCb>>>();
    let callback = Box::new(move |event: Box<IOEvent<ClosureCb>>| {
        let _ = done_tx.send(event);
    });
    let buffer3 = Buffer::aligned(4096).unwrap();
    // wrong offset
    let mut event = IOEvent::new(fd, buffer3, IOAction::Read, 100);
    event.set_callback(ClosureCb(callback.clone()));
    ctx.submit(event, IOChannelType::Prio).expect("submit");
    let mut event = done_rx.recv().unwrap();
    assert!(event.is_done());
    match event.get_result() {
        Ok(_buffer2) => {
            panic!("expect error, but return ok");
        }
        Err(e) => {
            // Read the file out of boundary gets EINVAL
            assert_eq!(e, Errno::EINVAL);
            println!("expected error {}", e);
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
            ctx.submit(event, IOChannelType::Write).expect("submit");
            let event = done_rx.recv().unwrap();
            assert!(event.is_done());
            // read
            let buffer2 = Buffer::aligned(4096).unwrap();
            let mut event = IOEvent::new(fd, buffer2, IOAction::Read, 4096 * i as i64);
            event.set_callback(ClosureCb(callback.clone()));
            ctx.submit(event, IOChannelType::Read).expect("submit");
            let mut event = done_rx.recv().unwrap();
            assert!(event.is_done());
            match event.get_result() {
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
