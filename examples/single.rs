extern crate threads_pool;

use std::thread::sleep;
use std::time::Duration;
use threads_pool::shared_mode;

fn main() {
    shared_mode::initialize(4);

    for num in 0..100 {
        shared_mode::run(move || {
            println!("I'm in with: {}", num);
            sleep(Duration::from_millis(10));
        });
    }

    shared_mode::close();
}
