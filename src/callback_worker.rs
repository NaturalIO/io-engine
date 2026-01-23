use crate::tasks::{IOCallback, IOEvent};
use crossfire::{MTx, mpmc};

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

    #[inline(always)]
    pub fn send(&self, event: IOEvent<C>) {
        let _ = self.0.send(event);
    }
}

impl<C: IOCallback> Clone for IOWorkers<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
