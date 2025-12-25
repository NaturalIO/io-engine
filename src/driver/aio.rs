// Copyright (c) 2025 NaturalIO

use crate::callback_worker::IOWorkers;
use crate::common::{SlotCollection, poll_request_from_queues};
use crate::context::IoSharedContext;
use crate::tasks::{IOAction, IOCallbackCustom, IOEvent};
use crossbeam::channel::{Receiver, Sender, bounded};
use nix::errno::Errno;
use std::{
    cell::UnsafeCell,
    io,
    mem::transmute,
    sync::{Arc, atomic::Ordering},
    thread,
};

pub struct AioSlot<C: IOCallbackCustom> {
    pub(crate) iocb: iocb,
    pub(crate) event: Option<Box<IOEvent<C>>>,
}

impl<C: IOCallbackCustom> AioSlot<C> {
    pub fn new(slot_id: u64) -> Self {
        Self { iocb: iocb { aio_data: slot_id, aio_reqprio: 1, ..Default::default() }, event: None }
    }

    #[inline(always)]
    pub fn fill_slot(&mut self, event: Box<IOEvent<C>>, slot_id: u16) {
        let iocb = &mut self.iocb;
        iocb.aio_data = slot_id as libc::__u64;
        iocb.aio_fildes = event.fd as libc::__u32;
        let buf = event.buf.as_ref().unwrap();
        iocb.aio_lio_opcode = event.action as u16;
        iocb.aio_buf = buf.get_raw() as u64;
        iocb.aio_nbytes = buf.len() as u64;
        iocb.aio_offset = event.offset;
        self.event.replace(event);
    }

    #[inline(always)]
    pub fn set_written(&mut self, written: usize, cb: &IOWorkers<C>) -> bool {
        if self.iocb.aio_nbytes <= written as u64 {
            if let Some(event) = self.event.take() {
                event.set_ok();
                cb.send(event);
            }
            return true;
        }
        self.iocb.aio_nbytes -= written as u64;
        self.iocb.aio_buf += written as u64;
        return false;
    }

    #[inline(always)]
    pub fn set_error(&mut self, errno: i32, cb: &IOWorkers<C>) {
        if let Some(event) = self.event.take() {
            event.set_error(errno);
            cb.send(event);
        }
    }
}

struct ThreadSafeSlots<C: IOCallbackCustom>(UnsafeCell<Vec<AioSlot<C>>>);

unsafe impl<C: IOCallbackCustom> Send for ThreadSafeSlots<C> {}
unsafe impl<C: IOCallbackCustom> Sync for ThreadSafeSlots<C> {}

pub struct AioDriver;

impl AioDriver {
    pub fn start<C: IOCallbackCustom>(
        ctx: Arc<IoSharedContext<C>>, _s_noti: Sender<()>, r_noti: Receiver<()>,
    ) -> io::Result<()> {
        let depth = ctx.depth;
        let mut context: aio_context_t = 0;
        if io_setup(depth as c_long, &mut context) != 0 {
            return Err(io::Error::last_os_error());
        }

        let mut slots = Vec::with_capacity(depth);
        for slot_id in 0..depth {
            slots.push(AioSlot::new(slot_id as u64));
        }
        // shared slots
        let slots = Arc::new(ThreadSafeSlots(UnsafeCell::new(slots)));

        let (s_free, r_free) = bounded::<u16>(depth);
        for i in 0..depth {
            let _ = s_free.send(i as u16);
        }

        let ctx_submit = ctx.clone();
        let slots_submit = slots.clone();
        thread::spawn(move || worker_submit(ctx_submit, context, slots_submit, r_noti, r_free));

        let ctx_poll = ctx.clone();
        let slots_poll = slots.clone();
        let s_free_poll = s_free.clone();
        thread::spawn(move || worker_poll(ctx_poll, context, slots_poll, s_free_poll));

        Ok(())
    }
}

struct AioSlotCollection<'a, C: IOCallbackCustom> {
    slots: &'a mut Vec<AioSlot<C>>,
    iocbs: &'a mut Vec<*mut iocb>,
    free_recv: &'a Receiver<u16>,
    // quota is handled by the loop condition in poll_request_from_queues
}

impl<'a, C: IOCallbackCustom> SlotCollection<C> for AioSlotCollection<'a, C> {
    fn push(&mut self, event: Box<IOEvent<C>>) {
        let slot_id = self.free_recv.recv().unwrap();
        let slot = &mut self.slots[slot_id as usize];
        slot.fill_slot(event, slot_id);
        self.iocbs.push(&mut slot.iocb as *mut iocb);
    }

    fn len(&self) -> usize {
        self.iocbs.len()
    }

    fn is_full(&self) -> bool {
        false // Check loop condition instead
    }
}

fn worker_submit<C: IOCallbackCustom>(
    ctx: Arc<IoSharedContext<C>>, context: aio_context_t, slots: Arc<ThreadSafeSlots<C>>,
    noti_recv: Receiver<()>, free_recv: Receiver<u16>,
) {
    let depth = ctx.depth;
    let mut iocbs = Vec::<*mut iocb>::with_capacity(depth);
    let slots_ref: &mut Vec<AioSlot<C>> = unsafe { transmute(slots.0.get()) };
    let mut last_write: bool = false;

    'outer: loop {
        if iocbs.len() == 0 && ctx.total_count.load(Ordering::Acquire) == 0 {
            if noti_recv.recv().is_err() {
                info!("io_submit worker exit due to closing");
                return;
            }
        }

        // Fill batch
        {
            let mut collection =
                AioSlotCollection { slots: slots_ref, iocbs: &mut iocbs, free_recv: &free_recv };
            poll_request_from_queues(&ctx, depth, &mut collection, &mut last_write);
        }

        let mut done: libc::c_long = 0;
        let mut left = iocbs.len();
        if left > 0 {
            'submit: loop {
                let _ = ctx.free_slots_count.fetch_sub(left, Ordering::SeqCst);
                let result = unsafe {
                    let arr = iocbs.as_mut_ptr().add(done as usize);
                    io_submit(context, left as libc::c_long, arr)
                };
                if result < 0 {
                    let _ = ctx.free_slots_count.fetch_add(left, Ordering::SeqCst); // submit failed add back
                    if -result == Errno::EINTR as i64 {
                        continue 'submit;
                    }
                    error!("io_submit error: {}", result);
                    break 'submit;
                } else {
                    if result == left as libc::c_long {
                        trace!("io submit {} events", result);
                        break 'submit;
                    } else {
                        let _ = ctx
                            .free_slots_count
                            .fetch_add(left - result as usize, Ordering::SeqCst); // submit partial add back
                        done += result;
                        left -= result as usize;
                        trace!("io submit {}/{} events", result, left);
                    }
                }
            }
        }
        iocbs.clear();
    }
}

fn worker_poll<C: IOCallbackCustom>(
    ctx: Arc<IoSharedContext<C>>, context: aio_context_t, slots: Arc<ThreadSafeSlots<C>>,
    free_sender: Sender<u16>,
) {
    let depth = ctx.depth;
    let mut infos = Vec::<io_event>::with_capacity(depth);
    let slots_ref: &mut Vec<AioSlot<C>> = unsafe { transmute(slots.0.get()) };
    let ts = timespec { tv_sec: 2, tv_nsec: 0 };
    loop {
        infos.clear();
        let result = io_getevents(context, 1, depth as i64, infos.as_mut_ptr(), unsafe {
            std::mem::transmute::<&timespec, *mut timespec>(&ts)
        });
        if result < 0 {
            if -result == Errno::EINTR as i64 {
                continue;
            }
            if !ctx.running.load(Ordering::Acquire) {
                // device error and we are stopping
                break;
            }
            error!("io_getevents errno: {}", -result);
            continue;
        } else if result == 0 {
            if !ctx.running.load(Ordering::Acquire) {
                // wait for all submmited io return
                if ctx.free_slots_count.load(Ordering::SeqCst) == ctx.depth {
                    break;
                }
            }
            continue;
        }
        let _ = ctx.free_slots_count.fetch_add(result as usize, Ordering::SeqCst);
        unsafe {
            infos.set_len(result as usize);
        }
        for ref info in &infos {
            let slot_id = (*info).data as usize;
            if verify_result(&ctx, context, &mut slots_ref[slot_id], info) {
                let _ = free_sender.send(slot_id as u16);
            }
        }
    }
    info!("io_poll worker exit due to closing");
    let _ = io_destroy(context);
}

#[inline(always)]
fn verify_result<C: IOCallbackCustom>(
    ctx: &IoSharedContext<C>, context: aio_context_t, slot: &mut AioSlot<C>, info: &io_event,
) -> bool {
    if info.res <= 0 {
        slot.set_error((-info.res) as i32, &ctx.cb_workers);
        return true;
    }
    if slot.set_written(info.res as usize, &ctx.cb_workers) {
        return true;
    }
    trace!("io not enough, resubmit");
    // Write data not enough, resubmit.
    let mut arr: [*mut iocb; 1] = [&mut slot.iocb as *mut iocb];
    'submit: loop {
        let result = io_submit(context, 1, arr.as_mut_ptr() as *mut *mut iocb);
        if result < 0 {
            if -result == Errno::EINTR as i64 {
                continue 'submit;
            }
            error!("io_re_submit error: {}", result);
            slot.set_error(-result as i32, &ctx.cb_workers);
            return true;
        } else if result > 0 {
            return false;
        }
    }
}

// Relevant symbols from the native bindings exposed via aio-bindings
use io_engine_aio_bindings::{
    __NR_io_destroy, __NR_io_getevents, __NR_io_setup, __NR_io_submit, IOCB_CMD_FDSYNC,
    IOCB_CMD_FSYNC, IOCB_CMD_PREAD, IOCB_CMD_PWRITE, IOCB_FLAG_RESFD, RWF_DSYNC, RWF_SYNC,
    aio_context_t, io_event, iocb, syscall, timespec,
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
