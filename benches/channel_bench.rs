use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use crossfire::mpsc::bounded_blocking;
use io_buffer::Buffer;
use io_engine::tasks::{ClosureCb, IOAction, IOEvent};
use std::os::fd::RawFd;
use std::thread;

const MSG_COUNT: u64 = 1_000_00;
const CHANNEL_CAPACITY: usize = 1000;

fn create_ioevent() -> IOEvent<ClosureCb> {
    let buf = Buffer::aligned(4096).unwrap();
    IOEvent::new(0 as RawFd, buf, IOAction::Read, 0)
}

fn bench_throughput_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_throughput");
    group.throughput(Throughput::Elements(MSG_COUNT));

    group.bench_function("ioevent_direct", |b| {
        b.iter(|| {
            let (tx, rx) = bounded_blocking::<IOEvent<ClosureCb>>(CHANNEL_CAPACITY);

            let tx_thread = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    tx.send(create_ioevent()).unwrap();
                }
            });

            let rx_thread = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    let _event = rx.recv().unwrap();
                    black_box(_event);
                }
            });

            tx_thread.join().unwrap();
            rx_thread.join().unwrap();
        });
    });

    group.bench_function("ioevent_boxed", |b| {
        b.iter(|| {
            let (tx, rx) = bounded_blocking::<Box<IOEvent<ClosureCb>>>(CHANNEL_CAPACITY);

            let tx_thread = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    tx.send(Box::new(create_ioevent())).unwrap();
                }
            });

            let rx_thread = thread::spawn(move || {
                for _ in 0..MSG_COUNT {
                    let _event = rx.recv().unwrap();
                    black_box(_event);
                }
            });

            tx_thread.join().unwrap();
            rx_thread.join().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput_direct);
criterion_main!(benches);
