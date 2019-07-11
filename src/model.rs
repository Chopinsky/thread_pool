use std::sync::atomic::{self, AtomicI8, Ordering};
use std::thread;

// Constant flags
pub(crate) const FLAG_NORMAL: u8 = 0;
pub(crate) const FLAG_CLOSING: u8 = 1;
pub(crate) const FLAG_FORCE_CLOSE: u8 = 2;
pub(crate) const FLAG_HIBERNATING: u8 = 4;
pub(crate) const FLAG_LAZY_INIT: u8 = 8;
pub(crate) const EXPIRE_PERIOD: usize = 64;
const BACKOFF_RETRY_LIMIT: usize = 16;

// Enum ...
pub(crate) enum Message {
    NewJob(Job),
    Terminate(Vec<usize>),
}

// Base types
pub(crate) type Job = Box<FnBox + Send + 'static>;
pub(crate) type WorkerUpdate = fn(id: usize);

// Traits
pub(crate) trait Backoff {
    fn spin_update(&self, new: i8);
    fn concede_update(&self, new: i8) -> bool;
    fn reset_lock(&self);
}

pub(crate) trait FnBox {
    fn call_box(self: Box<Self>);
}

// Impl
impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

// Shared utilities
pub(crate) fn spin_update(state: &AtomicI8, new: i8) {
    // retry counter
    let mut retry = 0;

    // wait till the mutating state is restored to state 0
    while state.compare_exchange_weak(
        0, new, Ordering::SeqCst, Ordering::Relaxed
    ) != Ok(0) {
        if retry < BACKOFF_RETRY_LIMIT {
            retry += 1;
            cpu_relax(retry);
        } else {
            thread::yield_now();
        }
    }
}

pub(crate) fn concede_update(state: &AtomicI8, new: i8) -> bool {
    // should use reset in this case
    if new == 0 {
        return false;
    }

    // we're moving in the same direction, skip this one
    if state.load(Ordering::Acquire) * new > 0 {
        return false;
    }

    // spin update to the new val
    spin_update(state, new);

    true
}

#[inline(always)]
pub(crate) fn reset_lock(state: &AtomicI8) {
    state.store(0, Ordering::Release);
}

pub(crate) fn cpu_relax(count: usize) {
    for _ in 0..(1 << count) {
        atomic::spin_loop_hint()
    }
}

