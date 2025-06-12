use crate::scheduler::*;
use crate::{buffer::*, test::*};
use nix::errno::Errno;
use tokio::sync::mpsc::channel;

extern crate md5;

#[test]
fn test_read_write() {
    setup_log();
    let temp_file = make_temp_file();
    let owned_fd = create_temp_file(temp_file.as_ref());
    let fd = owned_fd.fd;
    let ctx = IOContext::<DefaultCb>::new(fd, 2, &IOWorkers::new(1)).unwrap();

    let ctx1 = ctx.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(10)
        .build()
        .unwrap();
    rt.block_on(async move {
        let buffer3 = Buffer::aligned(4096).unwrap();
        // wrong offset
        let mut event = IOEvent::new(buffer3, IOAction::Read, 100);
        ctx1.submit(&event).await.expect("submit");
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
    });

    let ctx2 = ctx.clone();

    rt.block_on(async move {
        let (tx, mut rx) = channel::<i32>(10);
        for i in 0..10 {
            let tx_new = tx.clone();
            let t_id: i32 = i;
            let ctx_new = ctx2.clone();
            tokio::spawn(async move {
                for _j in 0..100 {
                    // write
                    // TODO randomize
                    let mut buffer = Buffer::aligned(4096).unwrap();
                    rand_buffer(&mut buffer);
                    let digest = md5::compute(&buffer);
                    let event = IOEvent::new(buffer, IOAction::Write, 4096 * i as i64);
                    ctx_new.submit(&event).await.expect("submit");
                    assert!(event.is_done());
                    // read
                    let buffer2 = Buffer::aligned(4096).unwrap();
                    let mut event = IOEvent::new(buffer2, IOAction::Read, 4096 * i as i64);
                    ctx_new.submit(&event).await.expect("submit");
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
                let _ = tx_new.send(t_id).await;
            });
        }
        for _i in 0..10 {
            rx.recv().await;
        }
    });
}
