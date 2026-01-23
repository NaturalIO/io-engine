// Copyright (c) 2025 NaturalIO

use crate::callback_worker::IOWorkers;
use crate::driver::aio::AioDriver;
use crate::driver::uring::UringDriver; // Import UringDriver
use crate::tasks::{IOCallback, IOEvent};
use crossfire::BlockingRxTrait;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub enum Driver {
    Aio,
    Uring,
}

pub(crate) struct CtxShared<C: IOCallback, Q> {
    pub depth: usize,
    pub queue: Q,
    pub cb_workers: IOWorkers<C>,
    pub free_slots_count: AtomicUsize,
}

unsafe impl<C: IOCallback, Q: Send> Send for CtxShared<C, Q> {}
unsafe impl<C: IOCallback, Q: Send> Sync for CtxShared<C, Q> {}

pub struct IOContext<C: IOCallback, Q> {
    pub(crate) inner: Arc<CtxShared<C, Q>>,
}

impl<C: IOCallback, Q> IOContext<C, Q>
where
    Q: BlockingRxTrait<IOEvent<C>> + Send + 'static,
{
    pub fn new(
        depth: usize,
        queue: Q,
        cbs: &IOWorkers<C>,
        driver_type: Driver, // New parameter
    ) -> Result<Arc<Self>, io::Error> {
        let inner = Arc::new(CtxShared {
            depth,
            queue,
            cb_workers: cbs.clone(),
            free_slots_count: AtomicUsize::new(depth),
        });

        match driver_type {
            Driver::Aio => AioDriver::start(inner.clone())?,
            Driver::Uring => UringDriver::start(inner.clone())?,
        }

        Ok(Arc::new(Self { inner }))
    }

    #[inline]
    pub fn get_depth(&self) -> usize {
        self.inner.depth
    }

    pub fn running_count(&self) -> usize {
        let inner = self.inner.as_ref();
        let free = inner.free_slots_count.load(Ordering::SeqCst);
        if free > inner.depth { 0 } else { inner.depth - free }
    }
}
