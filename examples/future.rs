extern crate threads_pool;

use threads_pool::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

struct Yields(u32);

impl Future for Yields {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 == 0 {
            // done
            Poll::Ready(1)
        } else {
            // self decreasing
            self.0 -= 1;

            // simulate expansive calculations
            thread::sleep(Duration::from_micros(200));

            // make sure we will wake self up!
            cx.waker().wake_by_ref();

            // putting the future back to the poll queue
            Poll::Pending
        }
    }
}

fn main() {
    println!("Done with output: {}", block_on(Yields(100)).unwrap_or(0));
}
