use crate::callback_worker::Worker;
use crate::tasks::{IOAction, IOCallback, IOEvent};
use crossfire::BlockingRxTrait;
use io_uring::{IoUring, opcode, types::*};
use log::{error, info};
use std::{cell::UnsafeCell, io, marker::PhantomData, sync::Arc, thread, time::Duration};

const URING_EXIT_SIGNAL_USER_DATA: u64 = u64::MAX;

pub struct UringDriver<C: IOCallback, Q: BlockingRxTrait<Box<IOEvent<C>>>, W: Worker<C>> {
    _marker: PhantomData<(C, Q, W)>,
}

struct UringInner(UnsafeCell<IoUring>);

unsafe impl Send for UringInner {}
unsafe impl Sync for UringInner {}

impl<
    C: IOCallback,
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
    W: Worker<C> + Send + 'static,
> UringDriver<C, Q, W>
{
    pub fn start(depth: u32, rx: Q, cb_workers: W) -> io::Result<()> {
        let ring = IoUring::new(depth.max(8))?;
        let ctx = Arc::new(UringInner(UnsafeCell::new(ring)));
        let ctx_submit = ctx.clone();
        let ctx_complete = ctx;
        thread::spawn(move || {
            Self::submit(ctx_submit, depth as usize, rx);
        });
        thread::spawn(move || {
            Self::complete(ctx_complete, cb_workers);
        });

        Ok(())
    }

    fn submit(ctx: Arc<UringInner>, depth: usize, rx: Q) {
        info!("io_uring submitter thread start");
        let exit_sent = false;

        let ring = unsafe { &mut *ctx.0.get() };

        loop {
            // 1. Receive events
            let mut events = Vec::with_capacity(depth);

            match rx.recv() {
                Ok(event) => events.push(event),
                Err(_) => {
                    if !exit_sent {
                        let nop_sqe =
                            opcode::Nop::new().build().user_data(URING_EXIT_SIGNAL_USER_DATA);
                        {
                            let mut sq = ring.submission();
                            unsafe {
                                if sq.push(&nop_sqe).is_err() {
                                    drop(sq);
                                    let _ = ring.submit();
                                    let mut sq = ring.submission();
                                    let _ = sq.push(&nop_sqe);
                                }
                            }
                        }
                        info!("io_uring submitter sent exit signal");
                    }
                    break;
                }
            }
            {
                let sq = ring.submission();
                if sq.is_full() {
                    drop(sq);
                    let _ = ring.submit();
                }
            }
            while events.len() < depth {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(_) => break,
                }
            }

            // 2. Push to SQ
            if !events.is_empty() {
                {
                    let mut sq = ring.submission();
                    for mut event in events.drain(0..) {
                        let fd = event.fd;

                        let sqe = match event.action {
                            IOAction::Read => {
                                let (offset, buf_ptr, buf_len) = event.get_param_for_io();
                                opcode::Read::new(Fd(fd), buf_ptr, buf_len).offset(offset).build()
                            }
                            IOAction::Write => {
                                let (offset, buf_ptr, buf_len) = event.get_param_for_io();
                                opcode::Write::new(Fd(fd), buf_ptr, buf_len).offset(offset).build()
                            }
                            IOAction::Alloc => {
                                let len = event.get_size();
                                opcode::Fallocate::new(Fd(fd), len)
                                    .offset(event.offset as u64)
                                    .mode(0)
                                    .build()
                            }
                            IOAction::Fsync => opcode::Fsync::new(Fd(fd)).build(),
                        };
                        let user_data = Box::into_raw(event) as u64;
                        let sqe = sqe.user_data(user_data);
                        unsafe {
                            if let Err(_) = sq.push(&sqe) {
                                error!("SQ full (should not happen)");
                                let _ = Box::from_raw(user_data as *mut IOEvent<C>);
                            }
                        }
                    }
                }

                if let Err(e) = ring.submit() {
                    error!("io_uring submit error: {:?}", e);
                }
            }
        }
        info!("io_uring submitter thread exit");
    }

    fn complete(ctx: Arc<UringInner>, cb_workers: W) {
        info!("io_uring completer thread start");

        let ring = unsafe { &mut *ctx.0.get() };

        loop {
            match ring.submit_and_wait(1) {
                Ok(_) => {
                    let mut exit_received = false;
                    {
                        let mut cq = ring.completion();
                        cq.sync();
                        for cqe in cq {
                            let user_data = cqe.user_data();

                            if user_data == URING_EXIT_SIGNAL_USER_DATA {
                                info!("io_uring completer received exit signal");
                                exit_received = true;
                                continue;
                            }

                            let event_ptr = user_data as *mut IOEvent<C>;
                            let mut event: Box<IOEvent<C>> = unsafe { Box::from_raw(event_ptr) };
                            let res = cqe.result();
                            if res >= 0 {
                                event.set_copied(res as usize);
                            } else {
                                event.set_error(-res);
                            }
                            cb_workers.done(event);
                        }
                    }
                    if exit_received {
                        break;
                    }
                }
                Err(e) => {
                    error!("io_uring submit_and_wait error: {:?}", e);
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
        info!("io_uring completer thread exit");
    }
}
