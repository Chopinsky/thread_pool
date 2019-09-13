#![allow(dead_code)]

use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
//use std::mem::MaybeUninit;

use crate::config::{Config, ConfigStatus};
use crate::debug::is_debug_mode;
use crate::model::StaticStore;
use crate::scheduler::{PoolManager, ThreadPool};
use parking_lot::{Once, OnceState, ONCE_INIT};

/// Atomic flags
static ONCE: Once = ONCE_INIT;
static CLOSING: AtomicBool = AtomicBool::new(false);

/// The actual pool storage
static mut POOL: StaticStore<Pool> = StaticStore::init();
//static mut POOL: Option<Pool> = None;
//static mut S_POOL: MaybeUninit<Pool> = MaybeUninit::new(Pool::default());

struct Pool {
    store: ThreadPool,
    auto_mode: bool,
    auto_adjust_handler: Option<JoinHandle<()>>,
}

impl Pool {
    #[inline]
    fn inner() -> Result<&'static mut Pool, ErrorKind> {
        if !CLOSING.load(Ordering::Acquire) {
            unsafe { POOL.as_mut() }
        } else {
            Err(ErrorKind::PermissionDenied)
        }
    }

    fn take() -> Result<&'static mut Pool, ErrorKind> {
        if CLOSING.compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            unsafe { POOL.as_mut() }
        } else {
            Err(ErrorKind::PermissionDenied)
        }
    }

    #[inline]
    fn toggle_auto_mode(&mut self, enabled: bool) {
        if self.auto_mode == enabled {
            return;
        }

        self.auto_mode = enabled;
        self.store.toggle_auto_scale(enabled);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        if !CLOSING.load(Ordering::Acquire) {
            // don't double drop
            close();
        }
    }
}

impl Default for Pool {
    fn default() -> Self {
        Pool {
            store: ThreadPool::build(1),
            auto_mode: false,
            auto_adjust_handler: None,
        }
    }
}

#[inline]
pub fn initialize(size: usize) {
    init_with_config(size, Config::default());
}

pub fn initialize_with_auto_adjustment(size: usize, period: Option<Duration>) {
    let mut config = Config::default();
    config.set_refresh_period(period);
    init_with_config(size, config);
}

pub fn init_with_config(size: usize, config: Config) {
    assert_eq!(
        ONCE.state(),
        OnceState::New,
        "The pool has already been initialized..."
    );

    let pool_size = match size {
        0 => 1,
        _ => size,
    };

    ONCE.call_once(|| {
        create(pool_size, config);
    });
}

pub fn run<F: FnOnce() + Send + 'static>(f: F) {
    match Pool::inner() {
        Ok(pool) => {
            if pool.store.exec(f, false).is_err() && is_debug_mode() {
                eprintln!("The execution of this job has failed...");
            }
        }
        Err(e) => {
            // This could happen after the pool is closed, just execute the job
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

pub fn resize(size: usize) -> JoinHandle<()> {
    thread::spawn(move || {
        if size == 0 {
            close();
        }

        if let Ok(pool) = Pool::inner() {
            pool.store.resize(size);
            return;
        }

        create(size, Config::default());
    })
}

pub fn update_auto_adjustment_mode(enabled: bool) {
    if let Ok(pool) = Pool::inner() {
        if pool.auto_mode == enabled {
            return;
        }

        if !enabled && pool.auto_adjust_handler.is_some() {
            stop_auto_adjustment(pool);
        }

        pool.toggle_auto_mode(enabled);
    }
}

pub fn reset_auto_adjustment_period(period: Option<Duration>) {
    if let Ok(pool) = Pool::inner() {
        // stop the previous auto job regardless
        stop_auto_adjustment(pool);

        // initiate the new auto adjustment job if configured
        if let Some(actual_period) = period {
            pool.toggle_auto_mode(true);
            pool.auto_adjust_handler = Some(start_auto_adjustment(actual_period));
        }
    }
}

fn trigger_auto_adjustment() {
    if let Ok(pool) = Pool::inner() {
        pool.store.auto_adjust();
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
            pool.toggle_auto_mode(false);
        }
    }
}

fn create(size: usize, config: Config) {
    if size == 0 {
        return;
    }

    let (auto_mode, handler) = if let Some(period) = config.refresh_period() {
        (true, Some(start_auto_adjustment(period)))
    } else {
        (false, None)
    };

    // Make the pool
    let mut store = ThreadPool::new_with_config(size, config.clone());
    store.toggle_auto_scale(auto_mode);

    // Put it in the heap so it can outlive this call
    unsafe {
        POOL.set(Pool {
            store,
            auto_mode,
            auto_adjust_handler: handler,
        });

        /*
                S_POOL.as_mut_ptr().write(Pool {
                    store,
                    auto_mode,
                    auto_adjust_handler: handler,
                });
        */
    }
}

fn shut_down(forced: bool) {
    match ONCE.state() {
        OnceState::InProgress => {
            panic!("The pool can't be closed while it's still being initializing...");
        }
        OnceState::Done => {
            if let Ok(pool_inner) = Pool::take() {
                if !forced {
                    pool_inner.store.close();
                } else {
                    pool_inner.store.force_close();
                }
            }
        }
        _ => (),
    }
}
