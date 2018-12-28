extern crate threads_pool;

use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use threads_pool::index_mode;

fn main() {
    let key_one = String::from("pool_one");
    let key_two = String::from("pool_two");

    let mut pool_def = HashMap::with_capacity(2);
    pool_def.insert(key_one.clone(), 2);
    pool_def.insert(key_two.clone(), 2);

    index_mode::initialize(pool_def);

    let t1 = thread::spawn(move || {
        for num in 0..50 {
            let pool_key = key_one.clone();
            index_mode::run_with(pool_key, move || {
                println!("I'm in with key_one: {}", num);
                thread::sleep(Duration::from_millis(1));
            });
        }
    });

    let t2 = thread::spawn(move || {
        for num in 0..100 {
            let pool_key = key_two.clone();
            index_mode::run_with(pool_key, move || {
                println!("I'm in with key_two: {}", num);
                thread::sleep(Duration::from_millis(1));
            });
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    println!("We're done!");

    index_mode::close();
}