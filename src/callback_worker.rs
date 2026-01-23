use crate::tasks::{IOCallback, IOEvent};
use crossfire::{MTx, Tx, flavor::Flavor, mpmc};

/// A trait for workers that accept IO events.
///
/// This allows using either `IOWorkers` (wrappers around channels) or direct channels
/// (`MTx`, `Tx`) or any other sink, or even process inline.
pub trait Worker<C: IOCallback>: Send + 'static {
    fn done(&self, event: IOEvent<C>);
}

/// Implement with crossfire::mpmc, can be shared among multiple IOContext instances.
pub struct IOWorkers<C: IOCallback>(pub(crate) MTx<mpmc::Array<IOEvent<C>>>);

impl<C: IOCallback> IOWorkers<C> {
    pub fn new(workers: usize) -> Self {
        let (tx, rx) = mpmc::bounded_blocking::<IOEvent<C>>(100000);
        for _i in 0..workers {
            let _rx = rx.clone();
            std::thread::spawn(move || {
                loop {
                    match _rx.recv() {
                        Ok(event) => event.callback_merged(),
                        Err(_) => {
                            debug!("IOWorkers exit");
                            return;
                        }
                    }
                }
            });
        }
        Self(tx)
    }
}

impl<C: IOCallback> Clone for IOWorkers<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C: IOCallback> Worker<C> for IOWorkers<C> {
    fn done(&self, item: IOEvent<C>) {
        let _ = self.0.send(item);
    }
}

// Implement Worker for Crossfire MTx (Multi-Producer)
impl<C, F> Worker<C> for MTx<F>
where
    F: Flavor<Item = IOEvent<C>> + crossfire::flavor::FlavorMP,
    C: IOCallback,
{
    fn done(&self, item: IOEvent<C>) {
        let _ = self.send(item);
    }
}

// Implement Worker for Crossfire Tx (Single-Producer)
impl<C, F> Worker<C> for Tx<F>
where
    F: Flavor<Item = IOEvent<C>>,
    C: IOCallback,
{
    fn done(&self, item: IOEvent<C>) {
        let _ = self.send(item);
    }
}

/// Inline worker that executes callbacks directly without spawning threads.
/// Use this for very lightweight callback logic to avoid thread context switching overhead.
pub struct Inline;

impl<C: IOCallback> Worker<C> for Inline {
    fn done(&self, event: IOEvent<C>) {
        event.callback_merged();
    }
}
