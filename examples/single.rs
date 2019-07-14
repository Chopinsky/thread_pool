extern crate threads_pool;

use std::thread;
use std::time::{Duration, Instant};
use threads_pool::shared_mode;

fn main() {
    let size = 16;
    let bound: usize = 400;

    shared_mode::initialize(size);

    let debug = true;
    let now = Instant::now();

    for num in 0..bound {
        shared_mode::run(move || {
            let mut count = 1;
            let mut sum = 0;

            while count <= num {
                count += 1;
                sum += count + count % size;
            }

            if debug && num % size == 0 {
                // this will "sync" the parallel executions since printing to screen is a
                // lock-protected, single thread-ish action.
                println!("Executing job: {} ==> {}", num, sum);
            }

            thread::sleep(Duration::from_millis(100));
        });
    }

    println!("All jobs are sent...");

    shared_mode::close();

    println!("Elapsed time: {} ms", now.elapsed().as_millis());
    println!("Zero-cost time to be: {} ms", bound / size * 100);
}
