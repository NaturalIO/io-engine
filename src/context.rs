// Copyright (c) 2025 NaturalIO

use crate::callback_worker::IOWorkers;
use crate::driver::aio::AioDriver;
use crate::driver::uring::UringDriver; // Import UringDriver
use crate::tasks::{IOEvent, IoCallback};
use crossfire::BlockingRxTrait;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub enum IoEngineType {
    Aio,
    Uring,
}

pub struct IoCtxShared<C: IoCallback, Q> {
    pub depth: usize,
    pub queue: Q,
    pub cb_workers: IOWorkers<C>,
    pub free_slots_count: AtomicUsize,
}

unsafe impl<C: IoCallback, Q: Send> Send for IoCtxShared<C, Q> {}
unsafe impl<C: IoCallback, Q: Send> Sync for IoCtxShared<C, Q> {}

pub struct IOContext<C: IoCallback, Q> {
    pub(crate) inner: Arc<IoCtxShared<C, Q>>,
}

impl<C: IoCallback, Q> IOContext<C, Q>
where
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
{
    pub fn new(
        depth: usize,
        queue: Q,
        cbs: &IOWorkers<C>,
        driver_type: IoEngineType, // New parameter
    ) -> Result<Arc<Self>, io::Error> {
        let inner = Arc::new(IoCtxShared {
            depth,
            queue,
            cb_workers: cbs.clone(),
            free_slots_count: AtomicUsize::new(depth),
        });

        match driver_type {
            IoEngineType::Aio => AioDriver::start(inner.clone())?,
            IoEngineType::Uring => UringDriver::start(inner.clone())?,
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
