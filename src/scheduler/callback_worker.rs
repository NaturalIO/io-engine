use crossbeam::channel::{Sender, bounded};

use super::{IOCallbackCustom, IOEvent};

pub struct IOWorkers<C: IOCallbackCustom>(pub(crate) Sender<Box<IOEvent<C>>>);

impl<C: IOCallbackCustom> IOWorkers<C> {
    pub fn new(workers: usize) -> Self {
        let (tx, rx) = bounded::<Box<IOEvent<C>>>(100000);
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

impl<C: IOCallbackCustom> Clone for IOWorkers<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
