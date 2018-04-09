#![allow(dead_code)]

use std::collections::HashMap;
use std::mem;
use std::thread;
use std::thread::JoinHandle;
use std::sync::{Mutex, Once, ONCE_INIT};
use super::common::{PoolManager, PoolState, ThreadPool};

static ONCE: Once = ONCE_INIT;
static mut MULTI_POOL: Option<PoolStore> = None;

struct PoolStore {
    store: HashMap<String, Mutex<Box<ThreadPool>>>,
}

pub fn initialize(keys: HashMap<String, usize>) {
    if keys.is_empty() {
        return;
    }

    unsafe {
        if MULTI_POOL.is_some() {
            panic!("You are trying to initialize the thread pools multiple times!");
        }

        ONCE.call_once(|| {
            let mut store = HashMap::with_capacity(keys.len());

            for (key, size) in keys {
                if key.is_empty() || size == 0 {
                    continue;
                }

                let work_pool = Mutex::new(Box::new(ThreadPool::new(size)));
                store.entry(key).or_insert(work_pool);
            }

            // Make the pool
            let pool = Some(PoolStore { store });

            // Put it in the heap so it can outlive this call
            MULTI_POOL = mem::transmute(pool);
        });
    }
}

pub fn run_with<F>(pool_key: String, f: F)
where
    F: FnOnce() + Send + 'static,
{
    unsafe {
        if let Some(ref pool) = MULTI_POOL {
            // if pool has been created
            if let Some(worker_pool) = pool.store.get(&pool_key) {
                if let Ok(workers) = worker_pool.lock() {
                    workers.execute(f);
                    return;
                }
            }
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);
    }
}

pub fn close() {
    unsafe {
        if let Some(pool) = MULTI_POOL.take() {
            for (_, pool) in pool.store {
                if let Ok(mut workers) = pool.lock() {
                    workers.clear();
                }
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
            if let Some(pool) = pools.store.get_mut(&pool_key) {
                if let Ok(mut workers) = pool.lock() {
                    workers.resize(size);
                }
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
            if let Some(pool) = pools.store.remove(&key) {
                if let Ok(mut workers) = pool.lock() {
                    workers.clear();
                }
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
            if let Some(pool) = pools.store.get_mut(&key) {
                if let Ok(mut workers) = pool.lock() {
                    if workers.get_size() != size {
                        workers.resize(size);
                        return;
                    }
                }
            }

            let new_pool = Mutex::new(Box::new(ThreadPool::new(size)));
            pools.store.insert(key, new_pool);
        }
    });

    Some(handler)
}
