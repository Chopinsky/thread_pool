# Threads Pool

This package provides an easy way to create and manage thread pools, so you don't have to. 

## How to use
In your project's `Cargo.toml`, add dependency:
```cargo
[dependencies]
threads_pool = "^0.1.0"
```

Then in your code: 
```rust
extern crate threads_pool;

use std::time::Duration;
use std::thread::sleep;
use threads_pool::*;

fn main() {
    // The pool lives as long as the `pool` variable, when pool goes out of 
    // the scope, the thread pool will be destroyed gracefully -- all threads 
    // will finish their current job and then garnered.   
    let pool = ThreadPool::new(8);
    
    for num in 0..100 {
        pool.execute(move || {
            // Your code here...
            println!("I'm in with: {}", num);
            sleep(Duration::from_millis(10));    
        });
    }
}
```

The package also provide a static pool which you can create once, and use it everywhere in your application. 
This is called the `shared_mode`: 
```rust
extern crate thread_pool;

use std::time::Duration;
use std::thread::sleep;
use threads_pool::shared_mode;

fn main() {
    // Create the pool here, then you can use the pool everywhere. If run a task without 
    // creating the pool, it will be equivalent of calling 'thread::spawn' on the task.
    shared_mode::initialize(8);

    for num in 0..100 {
        shared_mode::run(move || {
            // Your code here...
            println!("I'm in with: {}", num);
            sleep(Duration::from_millis(10));
        });
    }

    // The static pool must be closed, or unfinished threads will be destroyed prematurely and could cause panic.
    // this is different from the managed pool where it can be notified to shutdown automatically when out of the scope.
    shared_mode::close();
}
```