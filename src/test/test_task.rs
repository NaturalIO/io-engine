use crate::buffer::Buffer;
use crate::scheduler::*;
use nix::errno::Errno;
use std::sync::Arc;

struct A {}

#[allow(dead_code)]
impl A {
    fn do_io(self: Arc<Self>) {
        let buffer = Buffer::aligned(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(buffer, IOAction::Write, 0);
        let self1 = self.clone();
        let cb = move |mut _event: Box<IOEvent<ClosureCb>>| {
            Self::done_event(self1, _event);
        };
        event.set_callback(ClosureCb(Box::new(cb)));
        event.callback();
    }

    fn done_event(_self: Arc<Self>, mut _event: Box<IOEvent<ClosureCb>>) {
        println!("done event");
    }
}

#[test]
fn test_ioevent() {
    let buffer = Buffer::aligned(4096).unwrap();
    let mut event = IOEvent::<ClosureCb>::new(buffer, IOAction::Write, 0);
    assert!(!event.is_done());
    event.set_ok();
    assert!(event.is_done());
    assert!(event.get_result().is_ok());
    event.set_error(Errno::EINTR as i32);
    assert!(event.get_result().is_err());
    let err = event.get_result().err().unwrap();
    assert_eq!(err, Errno::EINTR);
}
