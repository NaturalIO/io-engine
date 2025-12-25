// Copyright (c) 2025 NaturalIO

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crossbeam::{
    channel::{Sender, bounded},
    queue::SegQueue,
};

use crate::callback_worker::IOWorkers;
use crate::driver::aio::AioDriver;
use crate::tasks::{IOCallbackCustom, IOEvent};

pub struct IoSharedContext<C: IOCallbackCustom> {
    pub depth: usize,
    pub prio_count: AtomicUsize,
    pub read_count: AtomicUsize,
    pub write_count: AtomicUsize,
    pub total_count: AtomicUsize,
    // shared by submitting worker and polling worker
    pub prio_queue: SegQueue<Box<IOEvent<C>>>,
    pub read_queue: SegQueue<Box<IOEvent<C>>>,
    pub write_queue: SegQueue<Box<IOEvent<C>>>,
    pub running: AtomicBool,
    pub cb_workers: IOWorkers<C>,
    pub free_slots_count: AtomicUsize,
}

pub struct IOContext<C: IOCallbackCustom> {
    pub(crate) inner: Arc<IoSharedContext<C>>,
    pub(crate) noti_sender: Sender<()>,
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
        let (s_noti, r_noti) = bounded::<()>(1);

        let inner = Arc::new(IoSharedContext {
            depth,
            running: AtomicBool::new(true),
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

        AioDriver::start(inner.clone(), s_noti.clone(), r_noti)?;

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
    fn get_inner(&self) -> &IoSharedContext<C> {
        self.inner.as_ref()
    }
}
