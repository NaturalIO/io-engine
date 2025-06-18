/*
Copyright (c) NaturalIO Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    io,
    mem::transmute,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use crossbeam::{
    channel::{Receiver, Sender, bounded},
    queue::SegQueue,
};
use libc::c_long;
use nix::errno::Errno;
use parking_lot::Mutex;

use super::{aio, callback_worker::*, tasks::*};

struct IOContextInner<C: IOCallbackCustom> {
    // the context handle for submitting AIO requests to the kernel
    context: aio::aio_context_t,

    depth: usize,
    //queue_size: usize,
    slots: UnsafeCell<Vec<IOEventTaskSlot<C>>>,
    prio_count: AtomicUsize,
    read_count: AtomicUsize,
    write_count: AtomicUsize,
    total_count: AtomicUsize,
    // shared by submitting worker and polling worker
    prio_queue: SegQueue<Box<IOEvent<C>>>,
    read_queue: SegQueue<Box<IOEvent<C>>>,
    write_queue: SegQueue<Box<IOEvent<C>>>,
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
    running: AtomicBool,
    cb_workers: IOWorkers<C>,
    free_slots_count: AtomicUsize,
}

unsafe impl<C: IOCallbackCustom> Send for IOContextInner<C> {}

unsafe impl<C: IOCallbackCustom> Sync for IOContextInner<C> {}

pub struct IOContext<C: IOCallbackCustom> {
    inner: Arc<IOContextInner<C>>,
    noti_sender: Sender<()>,
}

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum IOChannelType {
    Prio = 0,
    Read = 1,
    Write = 2,
}

impl<C: IOCallbackCustom> Drop for IOContext<C> {
    fn drop(&mut self) {
        error!("drop");
        self.inner.running.store(false, Ordering::SeqCst);
    }
}

impl<C: IOCallbackCustom> IOContext<C> {
    pub fn new(depth: usize, cbs: &IOWorkers<C>) -> Result<Arc<Self>, io::Error> {
        let mut context: aio::aio_context_t = 0;
        if aio::io_setup(depth as c_long, &mut context) != 0 {
            return Err(io::Error::last_os_error());
        }
        let (s_noti, r_noti) = bounded::<()>(1);
        let (s_free, r_free) = bounded::<u16>(depth);
        for i in 0..depth {
            let _ = s_free.send(i as u16);
        }
        let mut slots = Vec::with_capacity(depth);
        for slot_id in 0..depth {
            slots.push(IOEventTaskSlot::new(slot_id as u64));
        }
        let inner = Arc::new(IOContextInner {
            context,
            depth,
            slots: UnsafeCell::new(slots),
            running: AtomicBool::new(true),
            threads: Mutex::new(Vec::new()),
            prio_count: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            total_count: AtomicUsize::new(0),
            prio_queue: SegQueue::new(),
            read_queue: SegQueue::new(),
            write_queue: SegQueue::new(),
            cb_workers: cbs.clone(),
            free_slots_count: AtomicUsize::new(depth),
        });

        {
            let mut threads = inner.threads.lock();
            let inner1 = inner.clone();
            let th = thread::spawn(move || inner1.worker_submit(r_noti, r_free));
            threads.push(th);
            let inner2 = inner.clone();
            let sender_free = s_free.clone();
            let th = thread::spawn(move || inner2.worker_poll(sender_free));
            threads.push(th);
        }
        Ok(Arc::new(Self { inner, noti_sender: s_noti }))
    }

    #[inline]
    pub fn get_depth(&self) -> usize {
        self.get_inner().depth
    }

    #[inline(always)]
    pub fn submit(
        &self, event: Box<IOEvent<C>>, channel_type: IOChannelType,
    ) -> Result<(), io::Error> {
        let inner = &self.get_inner();
        match channel_type {
            IOChannelType::Prio => {
                let _ = inner.prio_count.fetch_add(1, Ordering::SeqCst);
                inner.prio_queue.push(event);
            }
            IOChannelType::Read => {
                let _ = inner.read_count.fetch_add(1, Ordering::SeqCst);
                inner.read_queue.push(event);
            }
            IOChannelType::Write => {
                let _ = inner.write_count.fetch_add(1, Ordering::SeqCst);
                inner.write_queue.push(event);
            }
        }
        inner.total_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.noti_sender.try_send(());
        Ok(())
    }

    #[inline(always)]
    pub fn pending_count(&self) -> usize {
        self.inner.total_count.load(Ordering::Acquire)
    }

    pub fn running_count(&self) -> usize {
        let inner = self.get_inner();
        let free = inner.free_slots_count.load(Ordering::SeqCst);
        if free > inner.depth { 0 } else { inner.depth - free }
    }

    #[inline(always)]
    fn get_inner(&self) -> &IOContextInner<C> {
        self.inner.as_ref()
    }
}

impl<C: IOCallbackCustom> IOContextInner<C> {
    #[inline(always)]
    fn verify_result(&self, slot: &mut IOEventTaskSlot<C>, info: &aio::io_event) -> bool {
        if info.res <= 0 {
            slot.set_error((-info.res) as i32, &self.cb_workers);
            return true;
        }
        if slot.set_written(info.res as usize, &self.cb_workers) {
            return true;
        }
        trace!("io not enough, resubmit");
        // Write data not enough, resubmit.
        let mut arr: [*mut aio::iocb; 1] = [&mut slot.iocb as *mut aio::iocb];
        'submit: loop {
            let result = aio::io_submit(self.context, 1, arr.as_mut_ptr() as *mut *mut aio::iocb);
            if result < 0 {
                if -result == Errno::EINTR as i64 {
                    continue 'submit;
                }
                error!("io_re_submit error: {}", result);
                slot.set_error(-result as i32, &self.cb_workers);
                return true;
            } else if result > 0 {
                return false;
            }
        }
    }

    fn worker_poll(&self, free_sender: Sender<u16>) {
        let depth = self.depth;
        let mut infos = Vec::<aio::io_event>::with_capacity(depth);
        let context = self.context;
        let slots: &mut Vec<IOEventTaskSlot<C>> = unsafe { transmute(self.slots.get()) };
        let ts = aio::timespec { tv_sec: 2, tv_nsec: 0 };
        loop {
            infos.clear();
            let result = aio::io_getevents(context, 1, depth as i64, infos.as_mut_ptr(), unsafe {
                std::mem::transmute::<&aio::timespec, *mut aio::timespec>(&ts)
            });
            if result < 0 {
                if -result == Errno::EINTR as i64 {
                    continue;
                }
                if !self.running.load(Ordering::Acquire) {
                    // device error and we are stopping
                    break;
                }
                error!("io_getevents errno: {}", -result);
                continue;
            } else if result == 0 {
                if !self.running.load(Ordering::Acquire) {
                    // wait for all submmited io return
                    if self.free_slots_count.load(Ordering::SeqCst) == self.depth {
                        break;
                    }
                }
                continue;
            }
            let _ = self.free_slots_count.fetch_add(result as usize, Ordering::SeqCst);
            unsafe {
                infos.set_len(result as usize);
            }
            for ref info in &infos {
                let slot_id = (*info).data as usize;
                if self.verify_result(&mut slots[slot_id], info) {
                    let _ = free_sender.send(slot_id as u16);
                }
            }
        }
        info!("io_poll worker exit due to closing");
        let _ = aio::io_destroy(self.context);
    }

    fn worker_submit(&self, noti_recv: Receiver<()>, free_recv: Receiver<u16>) {
        let depth = self.depth;
        let mut events = VecDeque::<Box<IOEvent<C>>>::with_capacity(depth);
        let mut iocbs = Vec::<*mut aio::iocb>::with_capacity(depth);
        let context = self.context;
        let slots: &mut Vec<IOEventTaskSlot<C>> = unsafe { transmute(self.slots.get()) };
        let mut last_write: bool = false;

        'outer: loop {
            if events.len() == 0 && self.total_count.load(Ordering::Acquire) == 0 {
                if noti_recv.recv().is_err() {
                    info!("io_submit worker exit due to closing");
                    return;
                }
            }
            'inner_queue: while events.len() < depth {
                let mut got = false;
                macro_rules! probe_queue {
                    ($queue: expr, $count: expr) => {
                        loop {
                            if events.len() < depth {
                                if let Some(event) = $queue.pop() {
                                    got = true;
                                    $count.fetch_sub(1, Ordering::SeqCst);
                                    self.total_count.fetch_sub(1, Ordering::SeqCst);
                                    events.push_back(event);
                                } else {
                                    break;
                                }
                            } else {
                                break 'inner_queue;
                            }
                        }
                    };
                }
                if self.prio_count.load(Ordering::Acquire) > 0 {
                    probe_queue!(self.prio_queue, self.prio_count);
                }
                if last_write {
                    last_write = false;
                    if self.read_count.load(Ordering::Acquire) > 0 {
                        probe_queue!(self.read_queue, self.read_count);
                    }
                    if self.write_count.load(Ordering::Acquire) > 0 {
                        probe_queue!(self.write_queue, self.write_count);
                    }
                } else {
                    last_write = true;
                    if self.write_count.load(Ordering::Acquire) > 0 {
                        probe_queue!(self.write_queue, self.write_count);
                    }
                    if self.read_count.load(Ordering::Acquire) > 0 {
                        probe_queue!(self.read_queue, self.read_count);
                    }
                }
                if got {
                    // we got something from queue in this loop, try to get more.
                    continue 'inner_queue;
                } else {
                    // nothing in queue
                    if events.len() > 0 {
                        break;
                    } else {
                        // continue to block
                        continue 'outer;
                    }
                }
            }
            log_debug_assert!(
                events.len() <= self.depth,
                "events {} {} {}",
                events.len(),
                events.capacity(),
                self.depth
            );
            log_debug_assert!(events.len() > 0, "events.len()>0");
            while events.len() > 0 {
                let slot_id = {
                    if iocbs.len() == 0 {
                        free_recv.recv().unwrap()
                    } else {
                        if let Ok(_slot_id) = free_recv.try_recv() {
                            _slot_id
                        } else {
                            break;
                        }
                    }
                };
                let event = events.pop_front().unwrap();
                let slot = &mut slots[slot_id as usize];
                slot.fill_slot(event, slot_id);
                iocbs.push(&mut slot.iocb as *mut aio::iocb);
            }
            let mut done: libc::c_long = 0;
            let mut left = iocbs.len();
            if left > 0 {
                'submit: loop {
                    let _ = self.free_slots_count.fetch_sub(left, Ordering::SeqCst);
                    let result = unsafe {
                        let arr = iocbs.as_mut_ptr().add(done as usize);
                        //trace!("io_submiting done {} left {}", done, left);
                        aio::io_submit(context, left as libc::c_long, arr)
                    };
                    if result < 0 {
                        let _ = self.free_slots_count.fetch_add(left, Ordering::SeqCst); // submit failed add back
                        if -result == Errno::EINTR as i64 {
                            continue 'submit;
                        }
                        error!("io_submit error: {}", result);
                        // TODO Error
                    } else {
                        if result == left as libc::c_long {
                            trace!("io submit {} events", result);
                            break 'submit;
                        } else {
                            let _ = self
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
}
