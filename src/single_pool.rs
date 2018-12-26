#![allow(dead_code)]

use super::scheduler::{PoolManager, ThreadPool};
use std::mem;
use std::sync::{Once, ONCE_INIT};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

static ONCE: Once = ONCE_INIT;
static mut POOL: Option<Pool> = None;

struct Pool {
    store: Box<ThreadPool>,
    auto_mode: bool,
    auto_adjust_handler: Option<JoinHandle<()>>,
}

#[inline]
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
    }

    ONCE.call_once(|| {
        create(pool_size, period);
    });
}

pub fn run<F: FnOnce() + Send + 'static>(f: F) {
    unsafe {
        if let Some(ref mut pool) = POOL {
            // if pool has been created, execute in proper mode.
            match &pool.auto_mode {
                &true => pool.store.execute_automode(f),
                &false => pool.store.execute(f),
            }

            return;
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);

        // meanwhile, lazy initialize (again?) the pool
        if POOL.is_none() {
            initialize(1);
        }
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

pub fn update_auto_adjustment_mode(enable: bool) {
    unsafe {
        if let Some(ref mut pool) = POOL {
            if pool.auto_mode == enable {
                return;
            }

            if !enable && pool.auto_adjust_handler.is_some() {
                stop_auto_adjustment(pool);
            }

            pool.auto_mode = enable;
        }
    }
}

pub fn reset_auto_adjustment_period(period: Option<Duration>) {
    unsafe {
        if let Some(ref mut pool) = POOL {
            // stop the previous auto job regardless
            stop_auto_adjustment(pool);

            // initiate the new auto adjustment job if configured
            if let Some(actual_period) = period {
                pool.auto_mode = true;
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

fn stop_auto_adjustment(pool: &mut Pool) {
    if let Some(handler) = pool.auto_adjust_handler.take() {
        handler.join().unwrap_or_else(|e| {
            eprintln!("Unable to join the thread: {:?}", e);
        });

        if pool.auto_mode {
            pool.auto_mode = false;
        }
    }
}

fn create(size: usize, auto_adjustment: Option<Duration>) {
    if size == 0 {
        return;
    }

    let (auto_mode, handler) = if let Some(period) = auto_adjustment {
        (true, Some(start_auto_adjustment(period)))
    } else {
        (false, None)
    };

    unsafe {
        // Make the pool
        let pool = Some(Pool {
            store: Box::new(ThreadPool::new(size)),
            auto_mode,
            auto_adjust_handler: handler,
        });

        // Put it in the heap so it can outlive this call
        POOL = mem::transmute(pool);
    }
}
