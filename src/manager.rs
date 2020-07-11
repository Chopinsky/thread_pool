#![allow(dead_code)]
use std::sync::atomic::{AtomicI8, AtomicU64, Ordering};

use crate::config::{Config, ConfigStatus};
use crate::debug::is_debug_mode;
use crate::model::{
    concede_update, reset_lock, spin_update, Backoff, Message, WorkerUpdate, EXPIRE_PERIOD,
};
use crate::pool::PoolStatus;
use crate::worker::Worker;
use crossbeam_channel::Receiver;
use std::sync::Arc;

/// The first id that can be taken by workers. All previous ones are reserved for future use in the
/// graveyard. Note that the `INIT_ID` is still the one reserved, and manager's first valid
/// `last_worker_id` shall be greater than this id number.
const INIT_ID: usize = 15;

#[doc(hidden)]
pub(crate) struct Manager {
    config: Config,
    workers: Vec<Worker>,
    mutating: AtomicI8,
    last_worker_id: usize,
    idle_threshold: IdleThreshold,
    chan: (Receiver<Message>, Receiver<Message>),
}

impl Manager {
    pub(crate) fn build(
        config: Config,
        range: usize,
        status: PoolStatus,
        pri_rx: Receiver<Message>,
        rx: Receiver<Message>,
        lazy_built: bool,
    ) -> Manager {
        let idle_threshold = IdleThreshold {
            inner: Arc::new((
                AtomicU64::new(EXPIRE_PERIOD),
                AtomicU64::new(4 * EXPIRE_PERIOD),
            )),
        };

        let mut m = Manager {
            config,
            workers: Vec::new(),
            mutating: AtomicI8::new(0),
            last_worker_id: INIT_ID,
            idle_threshold,
            chan: (pri_rx, rx),
        };

        if !lazy_built {
            m.add_workers(range, true, status);

            if is_debug_mode() {
                println!("Pool has been initialized with {} pools", m.workers.len());
            }
        }

        m
    }

    pub(crate) fn remove_all(&mut self, sync_remove: bool) {
        // nothing to remove now
        if self.workers.is_empty() {
            return;
        }

        // wait for the in-progress process to finish
        self.spin_update(-1);

        // the behaviors
        let behavior = self.config.worker_behavior();

        // the remove signal should have been sent by now, or this will forever block
        for mut worker in self.workers.drain(..) {
            let id = worker.get_id();

            if is_debug_mode() {
                println!("Sync retiring worker {}", id);
            }

            // call retire so we will block until all workers have been awakened again, meaning
            // all their work is now done and threads joined. Only do this if the shutdown message
            // is delivered such that workers may be able to quit the infinite-loop and join the
            // thread later.
            if sync_remove {
                behavior.before_drop(id);
                worker.retire();
                behavior.after_drop(id);
            }
        }

        // release worker lock
        self.reset_lock();

        // also clear the graveyard
        self.last_worker_id = INIT_ID;
    }

    pub(crate) fn add_workers(&mut self, count: usize, privileged: bool, status: PoolStatus) {
        if count == 0 {
            return;
        }

        if self.last_worker_id > INIT_ID {
            // before adding new workers, remove killed ones.
            self.worker_cleanup();
        }

        // if someone is actively adding workers, we skip this op
        if !self.concede_update(1) {
            return;
        }

        // reserve rooms for workers
        self.workers.reserve(count);

        // the start id is the next integer from the last worker's id
        let base_name = self.config.pool_name().cloned();
        let stack_size = self.config.thread_size();

        (1..=count).for_each(|offset| {
            // Worker is created to subscribe, but would register self later when pulled from the
            // workers queue
            let id = self.last_worker_id + offset;
            let (rx, pri_rx) = (self.chan.0.clone(), self.chan.1.clone());

            let worker_name = match base_name.as_ref() {
                Some(name) => Some(format!("{}-{}", name, id)),
                None => None,
            };

            self.workers.push(Worker::new(
                worker_name,
                id,
                stack_size,
                privileged,
                (pri_rx, rx),
                (status.clone(), self.idle_threshold.clone()),
                self.config.worker_behavior(),
            ));
        });

        self.reset_lock();
        self.last_worker_id += count;
    }

    pub(crate) fn wake_up(&self) {
        // call everyone to wake up and work
        self.workers.iter().for_each(|worker| worker.wake_up());
    }

    pub(crate) fn worker_cleanup(&mut self) {
        let (mut pos, mut end) = (0usize, self.workers.len());
        while pos < end {
            let worker: &mut Worker = &mut self.workers[pos];

            if worker.is_terminated() {
                worker.retire();
                self.workers.swap_remove(pos);
                end -= 1;
            } else {
                pos += 1;
            }
        }
    }
}

#[doc(hidden)]
pub(crate) trait WorkerManagement {
    fn workers_count(&self) -> usize;
    fn worker_auto_sleep(&mut self, life_in_ms: usize);
    fn worker_auto_expire(&mut self, life_in_ms: usize);
    fn extend_by(&mut self, more: usize, status: PoolStatus);
    fn shrink_by(&mut self, less: usize) -> Vec<usize>;
    fn dismiss_worker(&mut self, id: usize) -> Option<usize>;
    fn first_worker_id(&self) -> usize;
    fn last_worker_id(&self) -> usize;
    fn next_worker_id(&self, curr_id: usize) -> usize;
}

impl WorkerManagement for Manager {
    fn workers_count(&self) -> usize {
        self.workers.len()
    }

    fn worker_auto_sleep(&mut self, life_in_ms: usize) {
        self.idle_threshold
            .inner
            .0
            .store(life_in_ms as u64, Ordering::SeqCst);
    }

    fn worker_auto_expire(&mut self, life_in_ms: usize) {
        self.idle_threshold
            .inner
            .1
            .store(life_in_ms as u64, Ordering::SeqCst);
    }

    fn extend_by(&mut self, more: usize, status: PoolStatus) {
        self.add_workers(more, true, status);
    }

    fn shrink_by(&mut self, less: usize) -> Vec<usize> {
        if less == 0 {
            return Vec::new();
        }

        let start = self.workers.len() - less;

        // make sure we've exclusive lock before modifying the workers vec
        if !self.concede_update(-1) {
            return Vec::new();
        }

        let workers = self
            .workers
            .drain(start..)
            .map(|mut w| {
                let id = w.get_id();
                w.wake_up();
                w.retire();
                id
            })
            .collect();

        self.reset_lock();
        workers
    }

    fn dismiss_worker(&mut self, id: usize) -> Option<usize> {
        let mut res: Option<usize> = None;

        for idx in 0..self.workers.len() {
            let worker = &self.workers[idx];
            if worker.get_id() == id {
                // make sure the worker is awaken and hence can go away
                worker.wake_up();

                // get the lock, swap out the worker, use swap_remove for better performance.
                self.spin_update(-1);
                let mut retired = self.workers.swap_remove(idx);
                self.reset_lock();

                // now update the return value and notify worker to dismiss
                res.replace(retired.get_id());
                retired.retire();

                break;
            }
        }

        res
    }

    fn first_worker_id(&self) -> usize {
        match self.workers.first() {
            Some(worker) => worker.get_id(),
            None => 0,
        }
    }

    fn last_worker_id(&self) -> usize {
        match self.workers.last() {
            Some(worker) => worker.get_id(),
            None => 0,
        }
    }

    fn next_worker_id(&self, curr_id: usize) -> usize {
        let mut found = false;
        for worker in self.workers.iter() {
            let id = worker.get_id();
            if found {
                return id;
            }

            if id == curr_id {
                found = true;
            }
        }

        0
    }
}

#[doc(hidden)]
pub(crate) trait JobManagement {
    fn drop_one(&self, from: u8) -> usize;
    fn drop_many(&self, from: u8, target: usize) -> usize;
}

impl JobManagement for Manager {
    fn drop_one(&self, from: u8) -> usize {
        let chan = if from == 0 {
            // pop one task from the priority queue
            &self.chan.0
        } else {
            // pop one task from the normal queue
            &self.chan.1
        };

        if chan.is_empty() {
            return 0;
        }

        if chan.try_recv().is_err() {
            return 0;
        }

        1
    }

    fn drop_many(&self, from: u8, target: usize) -> usize {
        let chan = if from == 0 {
            // pop one task from the priority queue
            &self.chan.0
        } else {
            // pop one task from the normal queue
            &self.chan.1
        };

        if chan.is_empty() {
            return 0;
        }

        let mut count = 0;
        while count < target {
            // empty or disconnected, we're done either way
            if chan.try_recv().is_err() {
                break;
            }

            // update the counter
            count += 1;
        }

        count
    }
}

impl Backoff for Manager {
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

#[derive(Clone)]
pub struct StatusBehaviors {
    before_start: Option<WorkerUpdate>,
    after_start: Option<WorkerUpdate>,
    before_drop: Option<WorkerUpdate>,
    after_drop: Option<WorkerUpdate>,
}

impl StatusBehaviors {
    fn new() -> Self {
        StatusBehaviors {
            before_start: None,
            after_start: None,
            before_drop: None,
            after_drop: None,
        }
    }
}

pub trait StatusBehaviorSetter {
    fn set_before_start(&mut self, behavior: WorkerUpdate);
    fn set_after_start(&mut self, behavior: WorkerUpdate);
    fn set_before_drop(&mut self, behavior: WorkerUpdate);
    fn set_after_drop(&mut self, behavior: WorkerUpdate);
}

impl StatusBehaviorSetter for StatusBehaviors {
    fn set_before_start(&mut self, behavior: WorkerUpdate) {
        self.before_start.replace(behavior);
    }

    fn set_after_start(&mut self, behavior: WorkerUpdate) {
        self.after_start.replace(behavior);
    }

    fn set_before_drop(&mut self, behavior: WorkerUpdate) {
        self.before_drop.replace(behavior);
    }

    fn set_after_drop(&mut self, behavior: WorkerUpdate) {
        self.after_drop.replace(behavior);
    }
}

pub(crate) trait StatusBehaviorDefinitions {
    fn before_start(&self, id: usize);
    fn after_start(&self, id: usize);
    fn before_drop(&self, id: usize);
    fn after_drop(&self, id: usize);
    fn before_drop_clone(&self) -> Option<WorkerUpdate>;
    fn after_drop_clone(&self) -> Option<WorkerUpdate>;
}

impl StatusBehaviorDefinitions for StatusBehaviors {
    fn before_start(&self, id: usize) {
        if let Some(behavior) = self.before_start {
            behavior(id);
        }
    }

    fn after_start(&self, id: usize) {
        if let Some(behavior) = self.after_start {
            behavior(id);
        }
    }

    fn before_drop(&self, id: usize) {
        if let Some(behavior) = self.before_drop {
            behavior(id);
        }
    }

    fn after_drop(&self, id: usize) {
        if let Some(behavior) = self.after_drop {
            behavior(id);
        }
    }

    fn before_drop_clone(&self) -> Option<WorkerUpdate> {
        self.before_drop
    }

    fn after_drop_clone(&self) -> Option<WorkerUpdate> {
        self.after_drop
    }
}

impl Default for StatusBehaviors {
    fn default() -> Self {
        Self::new()
    }
}

// Wrapper
pub(crate) struct IdleThreshold {
    inner: Arc<(AtomicU64, AtomicU64)>,
}

impl IdleThreshold {
    pub(crate) fn idle_stat(&self, period: u64) -> u8 {
        let hibernate: u64 = self.inner.0.load(Ordering::Acquire);
        let retire: u64 = self.inner.1.load(Ordering::Acquire);

        match (hibernate, retire) {
            (h, r) if h > 0 && r > 0 => {
                if period < h {
                    // ok
                    0
                } else if period < r {
                    // hibernate
                    1
                } else {
                    // retire
                    2
                }
            }
            (h, r) if h > 0 && r == 0 => {
                if period < h {
                    0
                } else {
                    1
                }
            }
            (h, r) if h == 0 && r > 0 => {
                if period < r {
                    0
                } else {
                    2
                }
            }
            _ => 0,
        }
    }
}

impl Clone for IdleThreshold {
    fn clone(&self) -> Self {
        IdleThreshold {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for IdleThreshold {}
unsafe impl Sync for IdleThreshold {}
