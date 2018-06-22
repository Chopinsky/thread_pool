#![allow(dead_code)]

use std::mem;
use std::thread;
use std::thread::JoinHandle;
use std::sync::{Once, ONCE_INIT};
use std::time::Duration;
use super::scheduler::{PoolManager, ThreadPool};

static ONCE: Once = ONCE_INIT;
static mut POOL: Option<Pool> = None;

struct Pool {
    store: Box<ThreadPool>,
    auto_adjust_handler: Option<JoinHandle<()>>,
}

pub fn initialize(size: usize) {
    initialize_with_auto_adjustment(size, None);
}

pub fn initialize_with_auto_adjustment(size: usize, period: Option<Duration>) {
    let pool_size = match size {
        0 => 1,
        _ => size,
    };

    unsafe {
        if POOL.is_some() {
            panic!("You are trying to initialize the thread pools multiple times!");
        }

        ONCE.call_once(|| {
            create(pool_size, period);
        });
    }
}

pub fn run<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    unsafe {
        if let Some(ref pool) = POOL {
            // if pool has been created
            pool.store.execute(f);
            return;
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);
    }
}

pub fn close() {
    unsafe {
        if let Some(mut pool) = POOL.take() {
            pool.store.clear();
        }
    }
}

pub fn resize(size: usize) -> JoinHandle<()> {
    thread::spawn(move || {
        if size == 0 {
            close();
        }

        unsafe {
            if let Some(ref mut pool) = POOL {
                pool.store.resize(size);
                return;
            }
        }

        create(size, None);
    })
}

fn create(size: usize, auto_adjustment: Option<Duration>) {
    if size == 0 {
        return;
    }

    let handler = if let Some(period) = auto_adjustment {
        Some(start_auto_adjustment(period))
    } else {
        None
    };

    unsafe {
        // Make the pool
        let pool = Some(Pool {
            store: Box::new(ThreadPool::new(size)),
            auto_adjust_handler: handler,
        });

        // Put it in the heap so it can outlive this call
        POOL = mem::transmute(pool);
    }
}

pub fn update_auto_adjustment(period: Option<Duration>) {
    unsafe {
        if let Some(ref mut pool) = POOL {
            if let Some(mut handler) = pool.auto_adjust_handler.take() {
                handler.join().unwrap_or_else(|e| {
                    eprintln!("Unable to join the thread: {:?}", e);
                });
            }

            if let Some(actual_period) = period {
                pool.auto_adjust_handler = Some(start_auto_adjustment(actual_period));
            }
        }
    }
}

fn trigger_auto_adjustment() {
    unsafe {
        if let Some(ref mut pool) = POOL {
            pool.store.auto_adjust();
        }
    }
}

fn start_auto_adjustment(period: Duration) -> JoinHandle<()> {
    let one_second = Duration::from_secs(1);
    let actual_period = if period < one_second {
        one_second
    } else {
        period
    };

    thread::spawn(move || {
        thread::sleep(actual_period);

        loop {
            trigger_auto_adjustment();
            thread::sleep(actual_period);
        }
    })
}
