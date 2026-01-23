use crate::context::IoCtxShared;
use crate::tasks::{IOAction, IOEvent, IoCallback};
use crossfire::BlockingRxTrait;
use io_uring::{IoUring, opcode, types};
use log::{error, info};
use std::{cell::UnsafeCell, io, marker::PhantomData, sync::Arc, thread, time::Duration};

const URING_EXIT_SIGNAL_USER_DATA: u64 = u64::MAX;

pub struct UringDriver<C: IoCallback, Q: BlockingRxTrait<Box<IOEvent<C>>>> {
    _marker: PhantomData<(C, Q)>,
}

struct UringInner(UnsafeCell<IoUring>);

unsafe impl Send for UringInner {}
unsafe impl Sync for UringInner {}

impl<C: IoCallback, Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static> UringDriver<C, Q> {
    pub fn start(ctx: Arc<IoCtxShared<C, Q>>) -> io::Result<()> {
        let depth = ctx.depth as u32;
        let ring = IoUring::new(depth.max(2))?;
        let ring_arc = Arc::new(UringInner(UnsafeCell::new(ring)));
        let ring_submit = ring_arc.clone();
        let ring_complete = ring_arc.clone();
        let ctx_submit = ctx.clone();
        let ctx_complete = ctx.clone();
        thread::spawn(move || {
            Self::submit(ctx_submit, ring_submit);
        });
        thread::spawn(move || {
            Self::complete(ctx_complete, ring_complete);
        });

        Ok(())
    }

    fn submit(ctx: Arc<IoCtxShared<C, Q>>, ring_arc: Arc<UringInner>) {
        info!("io_uring submitter thread start");
        let depth = ctx.depth;
        let exit_sent = false;

        let ring = unsafe { &mut *ring_arc.0.get() };

        loop {
            // 1. Receive events
            let mut events = Vec::with_capacity(depth);

            match ctx.queue.recv() {
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
                match ctx.queue.try_recv() {
                    Ok(event) => events.push(event),
                    Err(_) => break,
                }
            }

            // 2. Push to SQ
            if !events.is_empty() {
                {
                    let mut sq = ring.submission();
                    for event in events {
                        let event = event;
                        let fd = event.fd;
                        let buf_slice = event.get_buf_ref();

                        let (offset, buf_ptr, buf_len) = if event.res > 0 {
                            let progress = event.res as u64;
                            (
                                event.offset as u64 + progress,
                                unsafe { (buf_slice.as_ptr() as *mut u8).add(progress as usize) },
                                (buf_slice.len() as u64 - progress) as u32,
                            )
                        } else {
                            (
                                event.offset as u64,
                                buf_slice.as_ptr() as *mut u8,
                                buf_slice.len() as u32,
                            )
                        };

                        let sqe = match event.action {
                            IOAction::Read => opcode::Read::new(types::Fd(fd), buf_ptr, buf_len)
                                .offset(offset)
                                .build(),
                            IOAction::Write => opcode::Write::new(types::Fd(fd), buf_ptr, buf_len)
                                .offset(offset)
                                .build(),
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

    fn complete(ctx: Arc<IoCtxShared<C, Q>>, ring_arc: Arc<UringInner>) {
        info!("io_uring completer thread start");

        let ring = unsafe { &mut *ring_arc.0.get() };

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
                            let mut event = unsafe { Box::from_raw(event_ptr) };
                            let res = cqe.result();
                            if res >= 0 {
                                event.set_copied(res as usize);
                            } else {
                                event.set_error(-res);
                            }
                            ctx.cb_workers.send(event);
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
