#![allow(dead_code)]

use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicI8, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::config::{Config, ConfigStatus};
use crate::debug::is_debug_mode;
use crate::model::{concede_update, reset_lock, spin_update, Backoff, StaticStore};
use crate::scheduler::{ThreadPool, PoolManager, PoolState};
use hashbrown::{HashMap, HashSet};
use parking_lot::{Once, OnceState, ONCE_INIT};

/// Atomic constants
static ONCE: Once = ONCE_INIT;
static CLOSING: AtomicBool = AtomicBool::new(false);

/// The actual store
static mut MULTI_POOL: StaticStore<PoolStore> = StaticStore::init(); //Option<PoolStore> = None;

struct PoolStore {
    store: HashMap<String, ThreadPool>,
    mutating: AtomicI8,
    auto_adjust_period: Option<Duration>,
    auto_adjust_handler: Option<JoinHandle<()>>,
    auto_adjust_register: HashSet<String>,
}

impl PoolStore {
    #[inline]
    fn inner() -> Result<&'static mut PoolStore, ErrorKind> {
        unsafe {
            if !CLOSING.load(Ordering::Acquire) {
                MULTI_POOL.as_mut()
            } else {
                Err(ErrorKind::PermissionDenied)
            }
        }
    }

    fn take() -> Result<&'static mut PoolStore, ErrorKind> {
        if CLOSING.compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            unsafe { MULTI_POOL.as_mut() }
        } else {
            Err(ErrorKind::PermissionDenied)
        }
    }
}

impl Drop for PoolStore {
    fn drop(&mut self) {
        if !CLOSING.load(Ordering::Acquire) {
            shut_down(false);
        }
    }
}

impl Backoff for PoolStore {
    fn spin_update(&self, new: i8) {
        spin_update(&self.mutating, new);
    }

    fn concede_update(&self, new: i8) -> bool {
        concede_update(&self.mutating, new)
    }

    #[inline(always)]
    fn reset_lock(&self) {
        reset_lock(&self.mutating)
    }
}

#[inline]
pub fn initialize<S>(keys: std::collections::HashMap<String, usize, S>)
where
    S: std::hash::BuildHasher,
{
    init_with_config(keys, Config::default());
}

pub fn initialize_with_auto_adjustment<S>(
    keys: std::collections::HashMap<String, usize, S>,
    period: Option<Duration>,
) where
    S: std::hash::BuildHasher,
{
    let mut config = Config::default();
    config.set_refresh_period(period);
    init_with_config(keys, config);
}

pub fn init_with_config<S>(keys: std::collections::HashMap<String, usize, S>, config: Config)
where
    S: std::hash::BuildHasher,
{
    if keys.is_empty() {
        return;
    }

    assert_eq!(
        ONCE.state(),
        OnceState::New,
        "The pool has already been initialized..."
    );

    // copy to more efficient structure
    let mut map = HashMap::with_capacity(keys.len());
    for (k, v) in keys.into_iter() {
        map.entry(k).or_insert(v);
    }

    ONCE.call_once(|| {
        create(map, config);
    });
}

pub fn run_with<F: FnOnce() + Send + 'static>(key: String, f: F) {
    match PoolStore::inner() {
        Ok(pool) => {
            // if pool has been created
            if let Some(p) = pool.store.get_mut(&key) {
                if p.exec(f, false).is_err() && is_debug_mode() {
                    eprintln!("The execution of this job has failed...");
                }
            } else if is_debug_mode() {
                eprintln!("Unable to identify the pool with given key: {}", key);
            }
        }
        Err(e) => {
            // pool could have closed, just execute the job
            thread::spawn(f);

            if is_debug_mode() {
                eprintln!("The pool is in invalid state: {:?}, the thread pool should be restarted...", e);
            }
        }
    };
}

pub fn close() {
    shut_down(false);
}

pub fn force_close() {
    shut_down(true);
}

pub fn resize_pool(pool_key: String, size: usize) {
    if pool_key.is_empty() {
        return;
    }

    thread::spawn(move || {
        if let Ok(pools) = PoolStore::inner() {
            if let Some(pool_inner) = pools.store.get_mut(&pool_key) {
                pool_inner.resize(size);
            }
        }
    });
}

pub fn remove_pool(key: String) -> Option<JoinHandle<()>> {
    if key.is_empty() {
        return None;
    }

    //TODO: remove from the auto_adjust_handlers as well...

    let handler = thread::spawn(move || {
        if let Ok(pools) = PoolStore::inner() {
            pools.concede_update(-1);
            if let Some(mut pool_inner) = pools.store.remove(&key) {
                pool_inner.close();
            }

            pools.reset_lock();
        }
    });

    Some(handler)
}

pub fn add_pool(key: String, size: usize) -> Option<JoinHandle<()>> {
    if key.is_empty() || size == 0 {
        return None;
    }

    let handler = thread::spawn(move || {
        if let Ok(pools) = PoolStore::inner() {
            pools.concede_update(1);

            if let Some(pool_info) = pools.store.get_mut(&key) {
                if pool_info.get_size() != size {
                    pool_info.resize(size);
                    return;
                }
            }

            pools.store.insert(key, ThreadPool::new(size));
            pools.reset_lock();
        }
    });

    Some(handler)
}

fn create<S>(keys: HashMap<String, usize, S>, config: Config)
where
    S: std::hash::BuildHasher,
{
    let size = keys.len();
    let mut store = HashMap::with_capacity(size);

    for (key, size) in keys {
        if key.is_empty() || size == 0 {
            continue;
        }

        store
            .entry(key)
            .or_insert_with(|| ThreadPool::new_with_config(size, config.clone()));
    }

    unsafe {
        // Put it in the heap so it can outlive this call
        MULTI_POOL.set(PoolStore {
            store,
            mutating: AtomicI8::new(0),
            auto_adjust_period: config.refresh_period(),
            auto_adjust_handler: None,
            auto_adjust_register: HashSet::with_capacity(size),
        });
    }
}

pub fn start_auto_adjustment(period: Duration) {
    if let Ok(pools) = PoolStore::inner() {
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

        pools.auto_adjust_period = Some(actual_period);
        pools.auto_adjust_handler = Some(thread::spawn(move || {
            thread::sleep(actual_period);

            loop {
                trigger_auto_adjustment();
                thread::sleep(actual_period);
            }
        }));
    }
}

pub fn stop_auto_adjustment() {
    if let Ok(pools) = PoolStore::inner() {
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

pub fn reset_auto_adjustment_period(period: Option<Duration>) {
    // stop the previous auto job regardless
    stop_auto_adjustment();

    // initiate the new auto adjustment job if configured
    if let Some(actual_period) = period {
        start_auto_adjustment(actual_period);
    }
}

pub fn toggle_pool_auto_mode(key: String, auto_adjust: bool) {
    if let Ok(pool) = PoolStore::inner() {
        if !pool.store.contains_key(&key) {
            return;
        }

        if pool.auto_adjust_register.is_empty() && !auto_adjust {
            return;
        }

        if let Some(pool_info) = pool.store.get_mut(&key) {
            pool_info.toggle_auto_scale(auto_adjust);
        }

        if auto_adjust {
            let to_launch_handler = pool.auto_adjust_register.is_empty();

            pool.auto_adjust_register.insert(key);

            if to_launch_handler {
                if let Some(period) = pool.auto_adjust_period {
                    start_auto_adjustment(period);
                } else {
                    start_auto_adjustment(Duration::from_secs(10));
                }
            }
        } else {
            pool.auto_adjust_register.remove(&key);
            if pool.auto_adjust_register.is_empty() {
                stop_auto_adjustment();
            }
        }
    }
}

pub fn is_pool_in_auto_mode(key: String) -> bool {
    if let Ok(pool) = PoolStore::inner() {
        return pool.auto_adjust_register.contains(&key);
    }

    false
}

fn trigger_auto_adjustment() {
    if let Ok(pools) = PoolStore::inner() {
        if pools.auto_adjust_register.is_empty() {
            return;
        }

        for key in pools.auto_adjust_register.iter() {
            if let Some(pool) = pools.store.get_mut(key) {
                pool.auto_adjust();
            }
        }
    }
}

fn shut_down(forced: bool) {
    match ONCE.state() {
        OnceState::InProgress => {
            panic!("The pool can't be closed while it's still being initializing...");
        }
        OnceState::Done => {
            if let Ok(pool_inner) = PoolStore::take() {
                pool_inner.store.values_mut().for_each(|pool| {
                    if !forced {
                        pool.close();
                    } else {
                        pool.force_close();
                    }
                });
            }
        }
        _ => (),
    }
}
