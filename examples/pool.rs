extern crate threads_pool;

use std::thread;
use std::time::{Duration, Instant};
use threads_pool::prelude::*;

fn main() {
    let size = 16;
    let bound: usize = 400;

    let mut pool = ThreadPool::new(size);
    let now = Instant::now();

    for num in 0..bound {
        pool.exec(
            move || {
                let mut count = 1;
                let mut sum = 0;

                while count <= num {
                    count += 1;
                    sum += count + count % size;
                }

                if num % size == 0 {
                    // this will "sync" the parallel executions since printing to screen is a
                    // lock-protected, single thread-ish action.
                    println!("Executing job: {} ==> {}", num, sum);
                }

                thread::sleep(Duration::from_millis(100));
            },
            false,
        )
        .ok();
    }

    println!("All jobs are sent, now blocking on ...");

    let result = pool
        .block_on(|| {
            let mut sum = 0;
            for i in 0..100 {
                sum += i * i;
            }

            sum
        })
        .unwrap_or_default();

    println!("The block on is released with result: {}", result);

    pool.clear();
    println!("All workers cleared...");

    pool.close();

    println!("Elapsed time: {} ms", now.elapsed().as_millis());
    println!("Zero-cost time to be: {} ms", bound / size * 100);
}
