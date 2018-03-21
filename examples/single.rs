extern crate thread_pool;

use std::time::Duration;
use std::thread::sleep;
use thread_pool::shared_mode;

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