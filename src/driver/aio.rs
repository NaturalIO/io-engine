use crate::callback_worker::Worker;
use nix::fcntl::{FallocateFlags, fallocate};
use nix::unistd::fsync;

use crate::tasks::{IOAction, IOCallback, IOEvent};
use crossfire::{BlockingRxTrait, Rx, SendError, Tx, spsc};
use nix::errno::Errno;
use std::collections::VecDeque;
use std::fs::File;
use std::os::fd::RawFd;
use std::os::unix::io::BorrowedFd;
use std::{
    cell::UnsafeCell, io, mem::transmute, os::fd::AsRawFd, sync::Arc, thread, time::Duration,
};

pub struct AioSlot<C: IOCallback> {
    pub(crate) iocb: iocb,
    pub(crate) event: Option<Box<IOEvent<C>>>,
}

impl<C: IOCallback> AioSlot<C> {
    pub fn new(slot_id: u64) -> Self {
        Self { iocb: iocb { aio_data: slot_id, aio_reqprio: 1, ..Default::default() }, event: None }
    }

    pub fn fill_exit_slot(&mut self, slot_id: u16, fd: RawFd) {
        let iocb = &mut self.iocb;
        iocb.aio_data = slot_id as libc::__u64;
        iocb.aio_fildes = fd as libc::__u32;
        iocb.aio_lio_opcode = IOAction::Read as u16;
        iocb.aio_buf = 0;
        iocb.aio_nbytes = 0;
        iocb.aio_offset = 0;
    }

    #[inline(always)]
    pub fn fill_buffer_slot(&mut self, slot_id: u16, mut event: Box<IOEvent<C>>) {
        let iocb = &mut self.iocb;
        iocb.aio_data = slot_id as libc::__u64;
        iocb.aio_fildes = event.fd as libc::__u32;
        let (_offset, p, l) = event.get_param_for_io();
        iocb.aio_lio_opcode = event.action as u16;
        iocb.aio_buf = p as u64;
        iocb.aio_nbytes = l as u64;
        iocb.aio_offset = _offset as i64;
        self.event.replace(event);
    }

    #[inline(always)]
    pub fn set_result<W: Worker<C>>(&mut self, written: usize, cb: &W) {
        if let Some(mut event) = self.event.take() {
            // If it was a zero-length read (exit signal), callback is usually None, so this is safe.
            event.set_copied(written);
            cb.done(event);
        }
    }

    #[inline(always)]
    pub fn set_error<W: Worker<C>>(&mut self, errno: i32, cb: &W) {
        if let Some(mut event) = self.event.take() {
            event.set_error(errno);
            cb.done(event);
        }
    }
}

struct AioInner<C: IOCallback> {
    depth: usize,
    context: aio_context_t,
    slots: UnsafeCell<Vec<AioSlot<C>>>,
    null_file: File,
}

unsafe impl<C: IOCallback> Send for AioInner<C> {}
unsafe impl<C: IOCallback> Sync for AioInner<C> {}

pub struct AioDriver<C: IOCallback, Q: BlockingRxTrait<Box<IOEvent<C>>>, W: Worker<C>> {
    _marker: std::marker::PhantomData<(C, Q, W)>,
}

impl<
    C: IOCallback,
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
    W: Worker<C> + Send + 'static,
> AioDriver<C, Q, W>
{
    pub fn start(depth: usize, rx: Q, cb_workers: W) -> io::Result<()> {
        let mut aio_context: aio_context_t = 0;
        if io_setup(depth as c_long, &mut aio_context) != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut slots = Vec::with_capacity(depth);
        for slot_id in 0..depth {
            slots.push(AioSlot::new(slot_id as u64));
        }
        let null_file;
        match File::open("/dev/null") {
            Err(e) => {
                error!("cannot open /dev/null: {}", e);
                return Err(e);
            }
            Ok(f) => {
                null_file = f;
            }
        }
        let inner = Arc::new(AioInner {
            depth,
            context: aio_context,
            slots: UnsafeCell::new(slots),
            null_file,
        });

        let (s_free, r_free) = spsc::bounded_blocking::<u16>(depth);
        for i in 0..depth {
            let _ = s_free.send(i as u16);
        }
        let inner_submit = inner.clone();
        thread::spawn(move || Self::submit_loop(inner_submit, rx, r_free));
        thread::spawn(move || Self::poll_loop(inner, cb_workers, s_free));
        Ok(())
    }

    /// This worker process IOEvent fallocate & fsync
    fn background_worker(rx: Rx<spsc::Array<Box<IOEvent<C>>>>) {
        loop {
            match rx.recv() {
                Ok(mut event) => {
                    let size = event.get_size() as i64;
                    let res = match event.action {
                        IOAction::Alloc => {
                            let fd = unsafe { BorrowedFd::borrow_raw(event.fd) };
                            fallocate(fd, FallocateFlags::empty(), event.offset, size)
                                .map_or_else(|e| -(e as i32), |_| 0)
                        }
                        IOAction::Fsync => fsync(unsafe { BorrowedFd::borrow_raw(event.fd) })
                            .map_or_else(|e| -(e as i32), |_| 0),
                        _ => -libc::EINVAL, // Should not happen
                    };

                    if res == 0 {
                        event.set_copied(size as usize);
                    } else {
                        event.set_error(-res);
                    }
                    event.callback_unchecked(true);
                }
                Err(_) => {
                    // Channel closed, exit worker
                    break;
                }
            }
        }
    }

    fn submit_loop(inner: Arc<AioInner<C>>, rx: Q, free_recv: Rx<spsc::Array<u16>>) {
        let depth = inner.depth;
        let mut iocbs = Vec::<*mut iocb>::with_capacity(depth);
        let slots_ref: &mut Vec<AioSlot<C>> = unsafe { transmute(inner.slots.get()) };
        let aio_context = inner.context;
        let mut events_to_process = VecDeque::with_capacity(depth);
        let mut background_channel_tx: Option<Tx<spsc::Array<Box<IOEvent<C>>>>> = None;

        loop {
            // 1. Fetch events
            // Only block if we have no events pending.
            if events_to_process.is_empty() {
                match rx.recv() {
                    Ok(event) => match event.action {
                        IOAction::Alloc | IOAction::Fsync => {
                            if background_channel_tx.is_none() {
                                let (tx, rx) = spsc::bounded_blocking::<Box<IOEvent<C>>>(depth);
                                thread::spawn(move || Self::background_worker(rx));
                                background_channel_tx = Some(tx);
                            }
                            if let Some(tx) = &background_channel_tx {
                                if let Err(SendError(mut event)) = tx.send(event) {
                                    event.set_error(Errno::ESHUTDOWN as i32);
                                    event.callback_unchecked(true);
                                }
                            }
                        }
                        _ => events_to_process.push_back(event),
                    },
                    Err(_) => {
                        // Queue closed. Time to exit.
                        // We need a free slot to submit the exit signal.
                        // We block to get one because we must signal exit to the poller.
                        let slot_id = free_recv.recv().unwrap();

                        let slot = &mut slots_ref[slot_id as usize];
                        slot.fill_exit_slot(slot_id, inner.null_file.as_raw_fd());
                        let mut iocb_ptr: *mut iocb = &mut slot.iocb as *mut _;

                        let res = io_submit(aio_context, 1, &mut iocb_ptr);
                        if res != 1 {
                            error!("Failed to submit exit signal: {}", res);
                        }
                        info!("io_submit worker exit due to queue closing");
                        break;
                    }
                }
            }

            // Try to fetch more events up to depth
            while events_to_process.len() < depth {
                if let Ok(event) = rx.try_recv() {
                    match event.action {
                        IOAction::Alloc | IOAction::Fsync => {
                            if let Some(tx) = &background_channel_tx {
                                if let Err(SendError(mut event)) = tx.send(event) {
                                    event.set_error(Errno::ESHUTDOWN as i32);
                                    event.callback_unchecked(true);
                                }
                            }
                        }
                        _ => events_to_process.push_back(event),
                    }
                } else {
                    break;
                }
            }

            // 2. Fill slots and prepare batch
            // We need to move events from queue to slots.
            let mut first = true;
            while !events_to_process.is_empty() {
                let slot_id_opt =
                    if first { Some(free_recv.recv().unwrap()) } else { free_recv.try_recv().ok() };

                if let Some(slot_id) = slot_id_opt {
                    first = false;
                    let event = events_to_process.pop_front().unwrap();
                    let slot = &mut slots_ref[slot_id as usize];
                    slot.fill_buffer_slot(slot_id, event);
                    iocbs.push(&mut slot.iocb as *mut iocb);
                } else {
                    // No more slots available right now
                    break;
                }
            }

            // 3. Submit batch
            if !iocbs.is_empty() {
                let mut done: libc::c_long = 0;
                let mut left = iocbs.len();

                // Reserve quota
                'submit: loop {
                    let result = unsafe {
                        let arr = iocbs.as_mut_ptr().add(done as usize);
                        io_submit(aio_context, left as libc::c_long, arr)
                    };

                    if result < 0 {
                        // All remaining failed
                        if -result == Errno::EINTR as i64 {
                            continue 'submit;
                        }
                        error!("io_submit error: {}", result);
                        break 'submit;
                    } else {
                        // Success (partial or full)
                        if result == left as libc::c_long {
                            trace!("io submit {} events", result);
                            break 'submit;
                        } else {
                            done += result;
                            left -= result as usize;
                            trace!("io submit {}/{} events", result, left);
                        }
                    }
                }
                iocbs.clear();
            }
        }
    }

    fn poll_loop(inner: Arc<AioInner<C>>, cb_workers: W, free_sender: Tx<spsc::Array<u16>>) {
        let depth = inner.depth;
        let mut infos = Vec::<io_event>::with_capacity(depth);
        let slots_ref: &mut Vec<AioSlot<C>> = unsafe { transmute(inner.slots.get()) };
        let aio_context = inner.context;
        let mut exit_received = false;

        loop {
            infos.clear();
            let result = io_getevents(
                aio_context,
                1,
                depth as i64,
                infos.as_mut_ptr(),
                std::ptr::null_mut(),
            );

            if result < 0 {
                if -result == Errno::EINTR as i64 {
                    continue;
                }
                error!("io_getevents errno: {}", -result);
                thread::sleep(Duration::from_millis(10));
                continue;
            }

            assert!(result > 0);
            unsafe {
                infos.set_len(result as usize);
            }
            for ref info in &infos {
                let slot_id = (*info).data as usize;
                let slot = &mut slots_ref[slot_id];

                // Check for exit signal: zero-length read on null_file
                if slot.iocb.aio_nbytes == 0 && (*info).res == 0 {
                    exit_received = true;
                    // We also free this slot
                    let _ = free_sender.send(slot_id as u16);
                    continue;
                }

                Self::verify_result(&cb_workers, slot, info);
                let _ = free_sender.send(slot_id as u16);
            }

            if exit_received {
                info!("io_poll worker exit gracefully");
                break;
            }
        }
        info!("io_poll worker exit cleaning up");
        // poll_loop() always exit last, after poll_submit()
        let _ = io_destroy(aio_context);
    }

    #[inline(always)]
    fn verify_result(cb_workers: &W, slot: &mut AioSlot<C>, info: &io_event) {
        if info.res < 0 {
            println!("set error {:?}", info.res);
            slot.set_error((-info.res) as i32, cb_workers);
            return;
        }
        slot.set_result(info.res as usize, cb_workers);
    }
}

// Relevant symbols from the native bindings exposed via aio-bindings
use io_engine_aio_bindings::{
    __NR_io_destroy, __NR_io_getevents, __NR_io_setup, __NR_io_submit, aio_context_t, io_event,
    iocb, syscall, timespec,
};
use libc::c_long;

// -----------------------------------------------------------------------------------------------
// Inline functions that wrap the kernel calls for the entry points corresponding to Linux
// AIO functions
// -----------------------------------------------------------------------------------------------

// Initialize an AIO context for a given submission queue size within the kernel.
//
// See [io_setup(7)](http://man7.org/linux/man-pages/man2/io_setup.2.html) for details.
#[inline(always)]
fn io_setup(nr: c_long, ctxp: *mut aio_context_t) -> c_long {
    unsafe { syscall(__NR_io_setup as c_long, nr, ctxp) }
}

// Destroy an AIO context.
//
// See [io_destroy(7)](http://man7.org/linux/man-pages/man2/io_destroy.2.html) for details.
#[inline(always)]
fn io_destroy(ctx: aio_context_t) -> c_long {
    unsafe { syscall(__NR_io_destroy as c_long, ctx) }
}

// Submit a batch of IO operations.
//
// See [io_sumit(7)](http://man7.org/linux/man-pages/man2/io_submit.2.html) for details.
#[inline(always)]
fn io_submit(ctx: aio_context_t, nr: c_long, iocbpp: *mut *mut iocb) -> c_long {
    unsafe { syscall(__NR_io_submit as c_long, ctx, nr, iocbpp) }
}

// Retrieve completion events for previously submitted IO requests.
//
// See [io_getevents(7)](http://man7.org/linux/man-pages/man2/io_getevents.2.html) for details.
#[inline(always)]
fn io_getevents(
    ctx: aio_context_t, min_nr: c_long, max_nr: c_long, events: *mut io_event,
    timeout: *mut timespec,
) -> c_long {
    unsafe { syscall(__NR_io_getevents as c_long, ctx, min_nr, max_nr, events, timeout) }
}
