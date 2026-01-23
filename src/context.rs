use crate::callback_worker::Worker;
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

pub(crate) struct CtxShared<C: IOCallback, Q, W> {
    pub depth: usize,
    pub queue: Q,
    pub cb_workers: W,
    pub free_slots_count: AtomicUsize,
    pub _marker: std::marker::PhantomData<C>,
}

unsafe impl<C: IOCallback, Q: Send, W: Send> Send for CtxShared<C, Q, W> {}
unsafe impl<C: IOCallback, Q: Send, W: Send> Sync for CtxShared<C, Q, W> {}

/// IOContext manages the submission of IO tasks to the underlying driver.
///
/// It is generic over the callback type `C`, the submission queue `Q`, and the worker type `W`.
///
/// # Channel Selection for `W` (Worker)
///
/// When configuring the `IOContext` with a worker `W` (usually a channel sender `cb_workers`),
/// you should choose the `crossfire` channel type based on your sharing model:
///
/// * **Shared Worker (Multiple Contexts):** If you have multiple `IOContext` instances sharing the same
///   callback worker, use the [IOWorkers](crate::callback_worker::IOWorkers) struct,
///   or pass `crossfire::MTx` (from `mpsc` or `mpmc` channels) with your custom worker implementation.
///   This allows multiple producers (contexts) to send completion events to a single consumer (worker).
///
/// * **Single Instance (Dedicated Worker):** If you have a single `IOContext` with its own dedicated
///   callback worker, use `crossfire::Tx` (from `spsc` channels). This is more efficient for single-producer
///   scenarios.
///
/// * **inline callback:** If you have a very light callback logic, you can use [Inline](crate::callback_worker::Inline)
pub struct IOContext<C: IOCallback, Q, W> {
    pub(crate) inner: Arc<CtxShared<C, Q, W>>,
}

impl<C: IOCallback, Q, W> IOContext<C, Q, W>
where
    Q: BlockingRxTrait<IOEvent<C>> + Send + 'static,
    W: Worker<C> + Send + 'static,
{
    pub fn new(
        depth: usize,
        queue: Q,
        cbs: W,
        driver_type: Driver, // New parameter
    ) -> Result<Arc<Self>, io::Error> {
        let inner = Arc::new(CtxShared {
            depth,
            queue,
            cb_workers: cbs,
            free_slots_count: AtomicUsize::new(depth),
            _marker: std::marker::PhantomData,
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
