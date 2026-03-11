use crate::callback_worker::Worker;
use crate::driver::aio::AioDriver;
use crate::driver::uring::UringDriver; // Import UringDriver
use crate::tasks::{IOCallback, IOEvent};
use crossfire::BlockingRxTrait;
use std::io;

pub enum Driver {
    Aio,
    Uring,
}

/// Setup the submission of IO tasks to the underlying driver.
///
/// It is generic over the callback type `C`, the submission queue `Q`, and the worker type `W`.
///
/// # Channel Selection for `W` (Worker)
///
/// When configuring the worker `W` (usually a channel sender `cb_workers`),
/// you should choose the `crossfire` channel type based on your sharing model:
///
/// * **Shared Worker (Multiple Producers):** If you have multiple submission channels sharing the same
///   callback worker, use the [IOWorkers](crate::callback_worker::IOWorkers) struct,
///   or pass `crossfire::MTx` (from `mpsc` or `mpmc` channels) with your custom worker implementation.
///   This allows multiple producers to send completion events to a single consumer (worker).
///
/// * **Single Instance (Dedicated Worker):** If you have a single submission channel with its own dedicated
///   callback worker, use `crossfire::Tx` (from `spsc` channels). This is more efficient for single-producer
///   scenarios.
///
/// * **inline callback:** If you have a very light callback logic, you can use [Inline](crate::callback_worker::Inline)
pub fn setup<C, Q, W>(
    depth: usize,
    rx: Q,
    cb_workers: W,
    driver_type: Driver, // New parameter
) -> io::Result<()>
where
    C: IOCallback,
    Q: BlockingRxTrait<Box<IOEvent<C>>> + Send + 'static,
    W: Worker<C> + Send + 'static,
{
    match driver_type {
        Driver::Aio => AioDriver::<C, Q, W>::start(depth, rx, cb_workers),
        Driver::Uring => UringDriver::<C, Q, W>::start(depth as u32, rx, cb_workers),
    }
}
