#![allow(dead_code)]

use super::scheduler::{PoolManager, PoolState, ThreadPool};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::{Once, ONCE_INIT};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

static ONCE: Once = ONCE_INIT;
static mut MULTI_POOL: Option<PoolStore> = None;

struct PoolStore {
    store: HashMap<String, PoolInfo>,
    auto_adjust_period: Option<Duration>,
    auto_adjust_handler: Option<JoinHandle<()>>,
    auto_adjust_register: HashSet<String>,
}

struct PoolInfo {
    pool: Box<ThreadPool>,
}

#[inline]
pub fn initialize(keys: HashMap<String, usize>) {
    initialize_with_auto_adjustment(keys, None);
}

pub fn initialize_with_auto_adjustment(keys: HashMap<String, usize>, period: Option<Duration>) {
    if keys.is_empty() {
        return;
    }

    unsafe {
        if MULTI_POOL.is_some() {
            panic!("You are trying to initialize the thread pools multiple times!");
        }
    }

    ONCE.call_once(|| {
        create(keys, period);
    });
}

pub fn run_with<F: FnOnce() + Send + 'static>(key: String, f: F) {
    unsafe {
        if let Some(ref mut pool) = MULTI_POOL {
            // if pool has been created
            if let Some(ref mut pool_info) = pool.store.get_mut(&key) {
                if pool.auto_adjust_register.contains(&key) {
                    pool_info.pool.execute_automode(f);
                } else {
                    pool_info.pool.execute(f);
                }

                return;
            }
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);

        // meanwhile, lazy initialize (again?) the pool
        if MULTI_POOL.is_none() {
            let mut base_pool = HashMap::with_capacity(1);
            base_pool.insert(key, 1);
            initialize(base_pool);
        }
    }
}

pub fn close() {
    unsafe {
        if let Some(pool) = MULTI_POOL.take() {
            for (_, mut pool_info) in pool.store {
                pool_info.pool.clear();
            }
        }
    }
}

pub fn resize_pool(pool_key: String, size: usize) {
    if pool_key.is_empty() {
        return;
    }

    thread::spawn(move || unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if let Some(mut pool_info) = pools.store.get_mut(&pool_key) {
                pool_info.pool.resize(size);
            }
        }
    });
}

pub fn remove_pool(key: String) -> Option<JoinHandle<()>> {
    if key.is_empty() {
        return None;
    }

    let handler = thread::spawn(move || unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if let Some(mut pool_info) = pools.store.remove(&key) {
                pool_info.pool.clear();
            }
        }
    });

    Some(handler)
}

pub fn add_pool(key: String, size: usize) -> Option<JoinHandle<()>> {
    if key.is_empty() || size == 0 {
        return None;
    }

    let handler = thread::spawn(move || unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if let Some(mut pool_info) = pools.store.get_mut(&key) {
                if pool_info.pool.get_size() != size {
                    pool_info.pool.resize(size);
                    return;
                }
            }

            let new_pool = Box::new(ThreadPool::new(size));
            pools.store.insert(key, PoolInfo { pool: new_pool });
        }
    });

    Some(handler)
}

fn create(keys: HashMap<String, usize>, period: Option<Duration>) {
    let size = keys.len();
    let mut store = HashMap::with_capacity(size);

    for (key, size) in keys {
        if key.is_empty() || size == 0 {
            continue;
        }

        let new_pool = Box::new(ThreadPool::new(size));
        store.entry(key).or_insert(PoolInfo { pool: new_pool });
    }

    // Make the pool
    let pool = Some(PoolStore {
        store,
        auto_adjust_period: period,
        auto_adjust_handler: None,
        auto_adjust_register: HashSet::with_capacity(size),
    });

    unsafe {
        // Put it in the heap so it can outlive this call
        MULTI_POOL = mem::transmute(pool);
    }
}

pub fn start_auto_adjustment(period: Duration) {
    unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if pools.auto_adjust_register.is_empty() {
                return;
            }

            if pools.auto_adjust_handler.is_some() {
                stop_auto_adjustment();
            }

            let five_second = Duration::from_secs(5);
            let actual_period = if period < five_second {
                five_second
            } else {
                period
            };

            pools.auto_adjust_period = Some(actual_period.clone());
            pools.auto_adjust_handler = Some(thread::spawn(move || {
                thread::sleep(actual_period);

                loop {
                    trigger_auto_adjustment();
                    thread::sleep(actual_period);
                }
            }));
        }
    }
}

pub fn stop_auto_adjustment() {
    unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if let Some(handler) = pools.auto_adjust_handler.take() {
                handler.join().unwrap_or_else(|e| {
                    eprintln!("Unable to join the thread: {:?}", e);
                });
            }

            if !pools.auto_adjust_register.is_empty() {
                pools.auto_adjust_register = HashSet::with_capacity(pools.store.len());
            }

            pools.auto_adjust_period = None;
        }
    }
}

pub fn reset_auto_adjustment_period(period: Option<Duration>) {
    // stop the previous auto job regardless
    stop_auto_adjustment();

    // initiate the new auto adjustment job if configured
    if let Some(actual_period) = period {
        start_auto_adjustment(actual_period);
    }
}

pub fn toggle_pool_auto_mode(key: String, auto_adjust: bool) {
    unsafe {
        if let Some(ref mut pool) = MULTI_POOL {
            if pool.auto_adjust_register.is_empty() && !auto_adjust {
                return;
            }

            match auto_adjust {
                true => {
                    let to_launch_handler = pool.auto_adjust_register.is_empty();

                    pool.auto_adjust_register.insert(key);

                    if to_launch_handler {
                        if let Some(period) = pool.auto_adjust_period {
                            start_auto_adjustment(period);
                        } else {
                            start_auto_adjustment(Duration::from_secs(10));
                        }
                    }
                }
                false => {
                    pool.auto_adjust_register.remove(&key);
                    if pool.auto_adjust_register.is_empty() {
                        stop_auto_adjustment();
                    }
                }
            };
        }
    }
}

pub fn is_pool_in_auto_mode(key: String) -> bool {
    unsafe {
        if let Some(ref mut pool) = MULTI_POOL {
            return pool.auto_adjust_register.contains(&key);
        }

        false
    }
}

fn trigger_auto_adjustment() {
    unsafe {
        if let Some(ref mut pools) = MULTI_POOL {
            if pools.auto_adjust_register.is_empty() {
                return;
            }

            for key in pools.auto_adjust_register.iter() {
                if let Some(mut pool_info) = pools.store.get_mut(key) {
                    pool_info.pool.auto_adjust();
                }
            }
        }
    }
}
