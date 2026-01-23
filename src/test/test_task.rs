use crate::tasks::{ClosureCb, IOAction, IOEvent};
use io_buffer::Buffer;
use nix::errno::Errno;
use std::sync::Arc;

struct A {}

#[allow(dead_code)]
impl A {
    fn do_io(self: Arc<Self>) {
        let buffer = Buffer::aligned(4096).unwrap();
        let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
        let self1 = self.clone();
        let cb = move |mut _event: IOEvent<ClosureCb>| {
            Self::done_event(self1, _event);
        };
        event.set_callback(ClosureCb(Box::new(cb)));
        event.callback();
    }

    fn done_event(_self: Arc<Self>, mut _event: IOEvent<ClosureCb>) {
        println!("done event");
    }
}

#[test]
fn test_ioevent() {
    let buffer = Buffer::aligned(4096).unwrap();
    let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
    assert!(!event.is_done());
    event.set_copied(4096);
    assert!(event.is_done());
    assert!(event.get_write_result().is_ok());

    let buffer = Buffer::aligned(4096).unwrap();
    let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
    event.set_error(Errno::EINTR as i32);
    assert!(event.get_write_result().is_err());

    let buffer = Buffer::aligned(4096).unwrap();
    let mut event = IOEvent::<ClosureCb>::new(0, buffer, IOAction::Write, 0);
    event.set_error(Errno::EINTR as i32);
    let err = event.get_write_result().err().unwrap();
    assert_eq!(err, Errno::EINTR);
}
