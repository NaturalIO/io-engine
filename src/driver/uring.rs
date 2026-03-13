use crate::callback_worker::Worker;
use crate::tasks::{IOAction, IOCallback, IOEvent};
use crossfire::BlockingRxTrait;
use io_uring::{IoUring, opcode, squeue::Flags, types::*};
use log::{error, info};
use std::{collections::VecDeque, io, marker::PhantomData, sync::Arc, thread, time::Duration};

const URING_EXIT_SIGNAL_USER_DATA: u64 = u64::MAX;

pub struct UringDriver<C: IOCallback, Q: BlockingRxTrait<Box<IOEvent<C>>>, W: Worker<C>> {
    _marker: PhantomData<(C, Q, W)>,
}

impl<
    C: IOCallback,
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
    W: Worker<C> + Send + 'static,
> UringDriver<C, Q, W>
{
    pub fn start(depth: u32, rx: Q, cb_workers: W) -> io::Result<()> {
        let ctx = Arc::new(IoUring::new(depth.max(8))?);
        let _ctx = ctx.clone();
        thread::spawn(move || {
            Self::submit(_ctx, depth as usize, rx);
        });
        thread::spawn(move || {
            Self::complete(ctx, cb_workers);
        });

        Ok(())
    }

    fn submit(ring: Arc<IoUring>, depth: usize, rx: Q) {
        info!("io_uring submitter thread start");
        macro_rules! get_sq {
            () => {{ unsafe { ring.submission_shared() } }};
        }
        let mut events = VecDeque::with_capacity(depth);
        loop {
            match rx.recv() {
                Ok(event) => events.push_back(event),
                Err(_) => {
                    break;
                }
            }
            while events.len() < depth {
                match rx.try_recv() {
                    Ok(event) => events.push_back(event),
                    Err(_) => break,
                }
            }
            if !events.is_empty() {
                {
                    let mut sq = get_sq!();
                    while let Some(mut event) = events.pop_front() {
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
                                debug!("sq is full");
                                let _event = Box::from_raw(user_data as *mut IOEvent<C>);
                                events.push_front(_event);
                                // TODO squeue_wait
                                // TODO we should write one function to combine submit and squeue_wait
                            }
                        }
                    }
                }
                if let Err(e) = ring.submit() {
                    error!("io_uring submit error: {:?}", e);
                }
            }
        }

        // use IO_DRAIN to make sure this event is the last to finish
        let nop_sqe = opcode::Nop::new()
            .build()
            .user_data(URING_EXIT_SIGNAL_USER_DATA)
            .flags(Flags::IO_DRAIN);
        {
            {
                let mut sq = get_sq!();
                if unsafe { sq.push(&nop_sqe) }.is_ok() {
                    drop(sq);
                    let _ = ring.submit();
                    info!("io_uring submitter sent exit signal");
                    return;
                }
            }
            // TODO we should write one function to combine submit and squeue_wait
            ring.submitter().squeue_wait().expect("wait");

            {
                let mut sq = get_sq!();
                unsafe { sq.push(&nop_sqe).expect("push") };
                drop(sq);
                let _ = ring.submit();
            }
        }
        info!("io_uring submitter sent exit signal");
    }

    fn complete(ring: Arc<IoUring>, cb_workers: W) {
        info!("io_uring completer thread start");

        loop {
            match ring.submit_and_wait(1) {
                Ok(_) => {
                    let mut exit_received = false;
                    {
                        let mut cq = unsafe { ring.completion_shared() };
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
