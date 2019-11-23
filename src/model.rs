#![allow(unused)]

use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, atomic::{self, AtomicI8, Ordering}};
use std::future::Future;
use std::thread::{self, Thread};
use std::time::Duration;
use crossbeam_channel::Sender;

// Constant flags
pub(crate) const FLAG_NORMAL: u8 = 0;
pub(crate) const FLAG_CLOSING: u8 = 1;
pub(crate) const FLAG_FORCE_CLOSE: u8 = 2;
pub(crate) const FLAG_HIBERNATING: u8 = 4;
pub(crate) const FLAG_LAZY_INIT: u8 = 8;
pub(crate) const FLAG_REST: u8 = 16;
pub(crate) const EXPIRE_PERIOD: usize = 64;

const BACKOFF_RETRY_LIMIT: usize = 16;
const ERR_MSG: &str = "Undefined behavior: the pool has been invoked without being initialized ...";

// Enum ...
pub(crate) enum Message {
    ThroughJob(Job),
    FutureJob(FutJob),
    Terminate(Vec<usize>),
}

// Base types
pub(crate) type Job = Box<dyn FnBox + Send + 'static>;
pub(crate) type FutJob = Box<dyn Future<Output = ()> + Send + 'static>;
//pub(crate) type FutJob = Box<LocalFutureObj<'static, ()>> + Send>;
pub(crate) type BlockJob<R> = Box<dyn FnResBox<R> + Send + 'static>;
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

pub(crate) trait FnResBox<R> {
    fn call_box(self: Box<Self>) -> R;
}

// Impl
impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

impl<R: Send, F: FnOnce() -> R> FnResBox<R> for F {
    fn call_box(self: Box<Self>) -> R {
        (*self)()
    }
}

/// The inner storage wrapper struct
pub(crate) struct StaticStore<T>(Option<T>);

/// The struct that will hold the actual pool. The implementation is sound because all usage is internal
/// and we're guaranteed that before each call, the real values are actually set ahead.
impl<T> StaticStore<T> {
    pub(crate) const fn init() -> Self {
        StaticStore(None)
    }

    pub(crate) fn as_mut(&mut self) -> Result<&mut T, ErrorKind> {
        self.0.as_mut().ok_or(ErrorKind::NotFound)
    }

    pub(crate) fn as_ref(&self) -> Result<&T, ErrorKind> {
        self.0.as_ref().ok_or(ErrorKind::NotFound)
    }

    pub(crate) fn set(&mut self, val: T) {
        self.0.replace(val);
    }

    pub(crate) fn take(&mut self) -> Option<T> {
        self.0.take()
    }
}

impl<T> Deref for StaticStore<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(ERR_MSG)
    }
}

impl<T> DerefMut for StaticStore<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(ERR_MSG)
    }
}

impl<T> Drop for StaticStore<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            // manually call the drop before we leave
            drop(inner);
        }
    }
}

// Shared utilities
pub(crate) fn spin_update(state: &AtomicI8, new: i8) {
    // retry counter
    let mut retry = 0;

    // wait till the mutating state is restored to state 0
    while state.compare_exchange_weak(0, new, Ordering::SeqCst, Ordering::Relaxed) != Ok(0) {
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

// Future related
