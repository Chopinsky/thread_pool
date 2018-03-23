#![allow(dead_code)]

use std::collections::HashMap;
use std::mem;
use std::thread;
use std::sync::{Once, ONCE_INIT};
use super::common::{PoolManager, ThreadPool};

static ONCE: Once = ONCE_INIT;
static mut MUTLTI_POOL: Option<PoolStore> = None;

struct PoolStore {
    store: HashMap<String, Box<ThreadPool>>,
}

pub fn initialize(keys: HashMap<String, usize>) {
    if keys.is_empty() {
        return;
    }

    unsafe {
        if MUTLTI_POOL.is_some() {
            panic!("You are trying to initialize the thread pools multiple times!");
        }

        ONCE.call_once(|| {
            let mut store = HashMap::with_capacity(keys.len());

            for (key, size) in keys {
                if key.is_empty() || size == 0 { continue; }

                let work_pool = Box::new(ThreadPool::new(size));
                store.entry(key).or_insert(work_pool);
            }

            // Make the pool
            let pool = Some(PoolStore { store });

            // Put it in the heap so it can outlive this call
            MUTLTI_POOL = mem::transmute(pool);
        });
    }
}

pub fn run<F>(pool_key: String, f: F) where F: FnOnce() + Send + 'static {
    unsafe {
        if let Some(ref pool) = MUTLTI_POOL {
            // if pool has been created
            if let Some(worker_pool) = pool.store.get(&pool_key) {
                worker_pool.execute(f);
                return;
            }
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);
    }
}

pub fn close() {
    unsafe {
        if let Some(pool) = MUTLTI_POOL.take() {
            for (_, mut pool) in pool.store {
                pool.clear();
            }
        }
    }
}

pub fn resize(pool_key: String, size: usize) {

//TODO: implement this function

//    if  { }
//
//    if size == 0 {
//        close();
//    }
//
//    unsafe {
//        if let Some(ref mut pool) = POOL {
//            pool.store.resize(size);
//        } else {
//            create(size);
//        }
//    }
}

//TODO: impl many things in single mode