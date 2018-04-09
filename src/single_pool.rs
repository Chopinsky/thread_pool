use std::mem;
use std::thread;
use std::thread::JoinHandle;
use std::sync::{Mutex, Once, ONCE_INIT};
use super::common::{PoolManager, ThreadPool};

static ONCE: Once = ONCE_INIT;
static mut POOL: Option<Pool> = None;

struct Pool {
    store: Mutex<Box<ThreadPool>>,
}

pub fn initialize(size: usize) {
    let pool_size = match size {
        0 => 1,
        _ => size,
    };

    unsafe {
        if POOL.is_some() {
            panic!("You are trying to initialize the thread pools multiple times!");
        }

        ONCE.call_once(|| {
            create(pool_size);
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
            if let Ok(store) = pool.store.lock() {
                store.execute(f);
            }

            return;
        }

        // otherwise, spawn to a new thread for the work;
        thread::spawn(f);
    }
}

pub fn close() {
    unsafe {
        if let Some(pool) = POOL.take() {
            if let Ok(mut store) = pool.store.lock() {
                store.clear();
            }
        }
    }
}

pub fn resize(size: usize) -> JoinHandle<()> {
    thread::spawn(move || {
        if size == 0 {
            close();
        }

        unsafe {
            if let Some(ref pool) = POOL {
                if let Ok(mut store) = pool.store.lock() {
                    store.resize(size);
                    return;
                }
            }
        }

        create(size);
    })
}

fn create(size: usize) {
    if size == 0 {
        return;
    }

    unsafe {
        // Make the pool
        let pool = Some(Pool {
            store: Mutex::new(Box::new(ThreadPool::new(size))),
        });

        // Put it in the heap so it can outlive this call
        POOL = mem::transmute(pool);
    }
}
