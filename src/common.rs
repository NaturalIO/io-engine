/*
Copyright (c) NaturalIO Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use crate::context::IoSharedContext;
use crate::tasks::{IOCallbackCustom, IOEvent};
use std::sync::atomic::Ordering;

pub trait SlotCollection<C: IOCallbackCustom> {
    fn push(&mut self, event: Box<IOEvent<C>>);
    fn len(&self) -> usize;
    fn is_full(&self) -> bool;
}

impl<C: IOCallbackCustom> SlotCollection<C> for Vec<Box<IOEvent<C>>> {
    fn push(&mut self, event: Box<IOEvent<C>>) {
        self.push(event);
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn is_full(&self) -> bool {
        false // Vec can grow
    }
}

pub fn poll_request_from_queues<C, I>(
    ctx: &IoSharedContext<C>, quota: usize, slots: &mut I, last_write: &mut bool,
) where
    C: IOCallbackCustom,
    I: SlotCollection<C>,
{
    'inner_queue: while slots.len() < quota {
        let mut got = false;

        // Prioritize Prio queue
        if ctx.prio_count.load(Ordering::SeqCst) > 0 {
            loop {
                if slots.len() < quota {
                    if let Some(event) = ctx.prio_queue.pop() {
                        got = true;
                        ctx.prio_count.fetch_sub(1, Ordering::SeqCst);
                        ctx.total_count.fetch_sub(1, Ordering::SeqCst);
                        slots.push(event);
                    } else {
                        break;
                    }
                } else {
                    break 'inner_queue;
                }
            }
        }

        macro_rules! probe_queue {
            ($queue: expr, $count: expr) => {
                loop {
                    if slots.len() < quota {
                        if let Some(event) = $queue.pop() {
                            got = true;
                            $count.fetch_sub(1, Ordering::SeqCst);
                            ctx.total_count.fetch_sub(1, Ordering::SeqCst);
                            slots.push(event);
                        } else {
                            break;
                        }
                    } else {
                        break 'inner_queue;
                    }
                }
            };
        }

        if *last_write {
            *last_write = false;
            if ctx.read_count.load(Ordering::SeqCst) > 0 {
                probe_queue!(ctx.read_queue, ctx.read_count);
            }
            if ctx.write_count.load(Ordering::SeqCst) > 0 {
                probe_queue!(ctx.write_queue, ctx.write_count);
            }
        } else {
            *last_write = true;
            if ctx.write_count.load(Ordering::SeqCst) > 0 {
                probe_queue!(ctx.write_queue, ctx.write_count);
            }
            if ctx.read_count.load(Ordering::SeqCst) > 0 {
                probe_queue!(ctx.read_queue, ctx.read_count);
            }
        }

        if got {
            // we got something from queue in this loop, try to get more.
            continue 'inner_queue;
        } else {
            // nothing in queue
            break;
        }
    }
}
