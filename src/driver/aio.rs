use crate::callback_worker::Worker;
use rustix::fs::{FallocateFlags, fallocate, fsync};

use crate::tasks::{CbArgs, IOAction, IOEvent};
use crossfire::{BlockingRxTrait, Rx, Tx, spsc};
use rustix::io::Errno;
use std::fs::File;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::os::unix::io::BorrowedFd;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{cell::UnsafeCell, io, os::fd::AsRawFd, thread, time::Duration};

// Relevant symbols from the native bindings exposed via aio-bindings
use io_engine_aio_bindings::{
    __NR_io_destroy, __NR_io_getevents, __NR_io_setup, __NR_io_submit, IOCB_CMD_PREAD,
    aio_context_t, io_event, iocb, syscall, timespec,
};

const EXIT_MAGIC: u64 = 0xFFFF_FFFF_FFFF_0000;

pub struct AioSlot<C: CbArgs> {
    iocb: iocb,
    _event: MaybeUninit<Box<IOEvent<C>>>,
}

impl<C: CbArgs> AioSlot<C> {
    pub fn new(slot_id: u64) -> Self {
        Self {
            iocb: iocb { aio_data: slot_id, aio_reqprio: 1, ..Default::default() },
            _event: MaybeUninit::uninit(),
        }
    }

    #[inline(always)]
    pub fn fill_exit_slot(&mut self, null_fd: RawFd) {
        let iocb = &mut self.iocb;
        // NOTE: the aio_data is init with slot_id, normally we don't touch it;
        // on exit the chosen slot will fill with EXIT_MAGIC on higher bits.
        // This slot will never used again, because no more io from upstream channel.
        iocb.aio_data |= EXIT_MAGIC;
        iocb.aio_fildes = null_fd as libc::__u32;
        iocb.aio_lio_opcode = IOCB_CMD_PREAD as libc::__u16;
        iocb.aio_buf = 0;
        iocb.aio_nbytes = 0;
        iocb.aio_offset = 0;
    }

    #[inline(always)]
    fn fill_noop_slot(&mut self, event: Box<IOEvent<C>>, null_fd: RawFd) {
        let iocb = &mut self.iocb;
        iocb.aio_lio_opcode = IOCB_CMD_PREAD as libc::__u16;
        iocb.aio_fildes = null_fd as libc::__u32;
        iocb.aio_buf = 0;
        iocb.aio_nbytes = 0;
        iocb.aio_offset = 0;
        self._event.write(event);
    }

    #[inline(always)]
    pub fn fill_buffer_slot(&mut self, mut event: Box<IOEvent<C>>) {
        let iocb = &mut self.iocb;
        iocb.aio_fildes = event.fd as libc::__u32;
        let (_offset, p, l) = event.get_param_for_io();
        iocb.aio_lio_opcode = event.action as libc::__u16;
        iocb.aio_buf = p as u64;
        iocb.aio_nbytes = l as u64;
        iocb.aio_offset = _offset as i64;
        self._event.write(event);
    }

    #[inline(always)]
    pub fn set_result<W: Worker<C>>(&mut self, written: usize, cb: &W) {
        let mut event = unsafe { self._event.assume_init_read() };
        if event.action.is_read_write() {
            // If it was a zero-length read (exit signal), callback is usually None, so this is safe.
            event.set_copied(written);
        }
        // for ALLOC or Fsync, the result is already set
        cb.done(event);
    }

    #[inline(always)]
    pub fn set_error<W: Worker<C>>(&mut self, errno: i32, cb: &W) {
        let mut event = unsafe { self._event.assume_init_read() };
        if event.action.is_read_write() {
            event.set_error(errno);
        }
        // for ALLOC or Fsync, the result is already set
        cb.done(event);
    }

    #[inline]
    fn submit_one(&mut self, aio_context: aio_context_t) -> Result<(), c_long> {
        let mut iocb_ptr: *mut iocb = &mut self.iocb as *mut _;
        let res = io_submit(aio_context, 1, &mut iocb_ptr);
        if res == 1 { Ok(()) } else { Err(res) }
    }

    #[inline]
    fn event(&mut self) -> &mut Box<IOEvent<C>> {
        unsafe { self._event.assume_init_mut() }
    }
}

struct AioInner<C: CbArgs> {
    depth: usize,
    context: aio_context_t,
    // NOTE: linux fs/aio.c submit_one has never dealt with IOCB_CMD_NOOP, it returns EINVAL.
    // so we have to open /dev/null to mock noop with 0 size read.
    null_file: File,
    slots: Vec<UnsafeCell<AioSlot<C>>>,
    // For poll_loop() count the inflict task number,
    // the submit_loop() might cache one slot_id but no way to put back into free_slot channel,
    // so submit_loop() will set the flag if `free_slot_temp` is not empty
    temp_slot_drop: AtomicBool,
}

impl<C: CbArgs> AioInner<C> {
    #[inline(always)]
    fn get_slot(&self, slot_id: u16) -> &mut AioSlot<C> {
        unsafe { &mut *self.slots[slot_id as usize].get() }
    }
}

unsafe impl<C: CbArgs> Send for AioInner<C> {}
unsafe impl<C: CbArgs> Sync for AioInner<C> {}

pub struct AioDriver<C: CbArgs, Q: BlockingRxTrait<Box<IOEvent<C>>>, W: Worker<C>> {
    _marker: std::marker::PhantomData<(C, Q, W)>,
}

impl<C: CbArgs, Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static, W: Worker<C> + Send + 'static>
    AioDriver<C, Q, W>
{
    pub fn start(depth: usize, rx: Q, cb_workers: W) -> io::Result<()> {
        let mut aio_context: aio_context_t = 0;
        if io_setup(depth as c_long, &mut aio_context) != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut slots = Vec::with_capacity(depth);
        for slot_id in 0..depth {
            slots.push(UnsafeCell::new(AioSlot::<C>::new(slot_id as u64)));
        }
        let null_file = match File::open("/dev/null") {
            Err(e) => {
                error!("cannot open /dev/null: {}", e);
                return Err(e);
            }
            Ok(f) => f,
        };
        let inner = Arc::new(AioInner {
            depth,
            context: aio_context,
            slots,
            null_file,
            temp_slot_drop: AtomicBool::new(false),
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
    fn background_worker(inner: Arc<AioInner<C>>, rx: Rx<spsc::Array<u16>>) {
        loop {
            match rx.recv() {
                Ok(slot_id) => {
                    let slot = inner.get_slot(slot_id);
                    let event = slot.event();
                    let mut size: usize = 0;
                    let fd = unsafe { BorrowedFd::borrow_raw(event.fd) };
                    let res: Result<(), Errno> = match event.action {
                        IOAction::Alloc => {
                            size = event.get_size() as usize;
                            fallocate(fd, FallocateFlags::empty(), event.offset as u64, size as u64)
                        }
                        IOAction::Fsync => fsync(fd),
                        _ => Err(Errno::INVAL), // Should not happen
                    };
                    if let Err(e) = res {
                        event.set_error(e.raw_os_error());
                    } else {
                        event.set_copied(size);
                    }
                    let _ = slot.submit_one(inner.context);
                }
                Err(_) => return, // exit
            }
        }
    }

    fn submit_loop(inner: Arc<AioInner<C>>, rx: Q, free_recv: Rx<spsc::Array<u16>>) {
        let depth = inner.depth;
        let mut iocbs = Vec::<*mut iocb>::with_capacity(depth);
        let aio_context = inner.context;
        let mut background_tx: Option<Tx<spsc::Array<u16>>> = None;
        let mut free_slot_temp: Option<u16> = None;

        macro_rules! event_fill_slot {
            ($event: expr, $slot_id: expr) => {{
                let slot = inner.get_slot($slot_id);
                if $event.action.is_read_write() {
                    slot.fill_buffer_slot($event);
                    iocbs.push(&mut slot.iocb as *mut iocb);
                } else {
                    slot.fill_noop_slot($event, inner.null_file.as_raw_fd());
                    if background_tx.is_none() {
                        let _inner = inner.clone();
                        let (_tx, _rx) = spsc::bounded_blocking::<u16>(depth);
                        thread::spawn(move || Self::background_worker(_inner, _rx));
                        background_tx = Some(_tx);
                    }
                    background_tx.as_ref().unwrap().send($slot_id).expect("ok");
                }
            }};
        }
        'MAIN: loop {
            match rx.recv() {
                Ok(event) => {
                    let slot_id =
                        free_slot_temp.take().unwrap_or_else(|| free_recv.recv().unwrap());
                    event_fill_slot!(event, slot_id);
                    while iocbs.len() < depth {
                        if let Ok(slot_id) = free_recv.try_recv() {
                            if let Ok(event) = rx.try_recv() {
                                event_fill_slot!(event, slot_id);
                            } else {
                                free_slot_temp.replace(slot_id);
                                break;
                            }
                        }
                    }
                }
                Err(_) => {
                    // Queue closed. Time to exit.
                    // We need a free slot to submit the exit signal.
                    // We block to get one because we must signal exit to the poller.
                    let slot_id = free_recv.recv().unwrap();
                    let slot = inner.get_slot(slot_id);
                    slot.fill_exit_slot(inner.null_file.as_raw_fd());
                    if let Err(e) = slot.submit_one(aio_context) {
                        error!("Failed to submit exit signal: {}", e);
                    }
                    break 'MAIN;
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
                        if -result == Errno::INTR.raw_os_error() as i64 {
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
        if free_slot_temp.is_some() {
            inner.temp_slot_drop.store(true, Ordering::SeqCst);
        }
        info!("io_submit worker closed");
    }

    fn poll_loop(inner: Arc<AioInner<C>>, cb_workers: W, free_sender: Tx<spsc::Array<u16>>) {
        let depth = inner.depth;
        let mut infos = Vec::<io_event>::with_capacity(depth);
        let aio_context = inner.context;
        let mut is_running = true;

        macro_rules! has_inflight {
            () => {{
                if is_running {
                    true
                } else if free_sender.is_full() {
                    false
                } else if inner.temp_slot_drop.load(Ordering::Acquire) {
                    free_sender.len() + 1 < depth
                } else {
                    true
                }
            }};
        }
        while has_inflight!() {
            infos.clear();
            let result = io_getevents(
                aio_context,
                1,
                depth as i64,
                infos.as_mut_ptr(),
                std::ptr::null_mut(),
            );

            if result < 0 {
                if -result == Errno::INTR.raw_os_error() as i64 {
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
            for info in &infos {
                let data = info.data;
                let slot_id = data as u16;
                // refer to fill_exit_slot()
                if data & EXIT_MAGIC == 0 {
                    let slot = inner.get_slot(slot_id);
                    if info.res >= 0 {
                        slot.set_result(info.res as usize, &cb_workers);
                    } else {
                        slot.set_error((-info.res) as i32, &cb_workers);
                    }
                } else {
                    // exit signal
                    is_running = false;
                }
                let _ = free_sender.send(slot_id);
            }
        }
        info!("io_poll worker exit cleaning up");
        // poll_loop() always exit last, after poll_submit()
        let _ = io_destroy(aio_context);
    }
}

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
