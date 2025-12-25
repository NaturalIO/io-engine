use crate::tasks::{IOEvent, IoCallback};
use crossfire::{MTx, mpmc};

pub struct IOWorkers<C: IoCallback>(pub(crate) MTx<mpmc::Array<Box<IOEvent<C>>>>);

impl<C: IoCallback> IOWorkers<C> {
    pub fn new(workers: usize) -> Self {
        let (tx, rx) = mpmc::bounded_blocking::<Box<IOEvent<C>>>(100000);
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
    pub fn send(&self, event: Box<IOEvent<C>>) {
        let _ = self.0.send(event);
    }
}

impl<C: IoCallback> Clone for IOWorkers<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
