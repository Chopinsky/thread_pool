extern crate threads_pool;

use std::thread;
use std::time::{Duration, Instant};
use threads_pool::shared_mode;

fn main() {
    shared_mode::initialize(4);

    let debug = true;
    let now = Instant::now();

    for num in 0..=203 {
        println!("Sending job: {}...", num);

        shared_mode::run(move || {
            let mut count: u32 = 1;
            let mut sum = 0;

            while count <= num {
                count += 1;
                sum += count + count % 7;
            }

            if debug && num % 7 == 0 {
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
    println!("Zero-cost time to be: {} ms", 204/4*100);
}
