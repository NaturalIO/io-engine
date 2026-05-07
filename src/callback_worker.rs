use crate::tasks::{CbArgs, IOEvent};
use crossfire::{MTx, Tx, flavor::Flavor};
use io_buffer::Buffer;
use rustix::io::Errno;

/// A trait for workers that accept IO events.
///
/// This allows using either `IOWorkers` (wrappers around channels) or direct channels
/// (`MTx`, `Tx`) or any other sink, or even process inline.
pub trait Worker<C: CbArgs>: Send + 'static {
    fn done(&self, event: Box<IOEvent<C>>);
}

// Implement Worker for Crossfire MTx (Multi-Producer)
impl<C, F> Worker<C> for MTx<F>
where
    F: Flavor<Item = Box<IOEvent<C>>>,
    C: CbArgs,
{
    fn done(&self, item: Box<IOEvent<C>>) {
        let _ = self.send(item);
    }
}

// Implement Worker for Crossfire Tx (Single-Producer)
impl<C, F> Worker<C> for Tx<F>
where
    F: Flavor<Item = Box<IOEvent<C>>>,
    C: CbArgs,
{
    fn done(&self, item: Box<IOEvent<C>>) {
        let _ = self.send(item);
    }
}

/// Example Inline worker that executes callbacks directly without spawning threads.
/// Use this for very lightweight callback logic to avoid thread context switching overhead.
///
/// # Safety
///
/// It does not resubmit short I/O
pub struct InlineClosure<C: CbArgs>(pub Box<dyn Fn(C, i64, Result<Option<Buffer>, Errno>) + Send>);

impl<C: CbArgs> Worker<C> for InlineClosure<C> {
    fn done(&self, event: Box<IOEvent<C>>) {
        event.callback_unchecked(&self.0);
    }
}
