#![allow(dead_code)]

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use crossbeam_channel::Receiver;
use hashbrown::HashSet;
use parking_lot::RwLock;
use crate::debug::is_debug_mode;
use crate::model::{Message, WorkerUpdate};
use crate::worker::{Worker, WorkerConfig};

pub(crate) struct Manager {
    name: Option<String>,
    workers: Vec<Worker>,
    last_worker_id: usize,
    graveyard: Arc<RwLock<HashSet<usize>>>,
    max_idle: Arc<AtomicUsize>,
    status_behavior: StatusBehaviors,
    chan: (Receiver<Message>, Receiver<Message>),
}

impl Manager {
    pub(crate) fn new(
        name: Option<String>,
        range: usize,
        pri_rx: Receiver<Message>,
        rx: Receiver<Message>,
        behavior: StatusBehaviors,
    ) -> Manager
    {
        let mut m = Self::lazy_create(name, pri_rx, rx, behavior);
        m.add_workers(range);
        m.graveyard.write().reserve(range);

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", m.workers.len());
        }

        m
    }

    pub(crate) fn lazy_create(
        name: Option<String>,
        pri_rx: Receiver<Message>,
        rx: Receiver<Message>,
        behavior: StatusBehaviors,
    ) -> Manager
    {
        Manager {
            name,
            workers: Vec::new(),
            last_worker_id: 0,
            graveyard: Arc::new(RwLock::new(HashSet::new())),
            max_idle: Arc::new(AtomicUsize::new(0)),
            status_behavior: behavior,
            chan: (pri_rx, rx),
        }
    }

    pub(crate) fn remove_all(&mut self, wait: bool) {
        for mut worker in self.workers.drain(..) {
            let id = worker.get_id();

            if is_debug_mode() {
                println!("Sync retiring worker {}", id);
            }

            // call retire so we will block until all workers have been awakened again, meaning
            // all their work is now done and threads joined. Only do this if the shutdown message
            // is delivered such that workers may be able to quit the infinite-loop and join the
            // thread later.
            if wait {
                self.status_behavior.before_drop(id);
                worker.retire();
                self.status_behavior.after_drop(id);
            }
        }

        // also clear the graveyard
        self.graveyard.write().clear();
        self.last_worker_id = 0;
    }

    pub(crate) fn add_workers(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        if self.last_worker_id > 0 {
            // before adding new workers, remove killed ones.
            self.graveyard_trim();
        }

        // reserve rooms for workers
        self.workers.reserve(count);

        // the start id is the next integer from the last worker's id
        (1..=count).for_each(|offset| {
            // Worker is created to subscribe, but would register self later when pulled from the
            // workers queue
            let id = self.last_worker_id + offset;
            let worker_name = self.name.as_ref().and_then(|name| {
                Some(format!("{}-{}", name, id))
            });

            let (rx, pri_rx) = (self.chan.0.clone(), self.chan.1.clone());

            self.workers.push(
                Worker::new(
                    id,
                    pri_rx,
                    rx,
                    Arc::clone(&self.graveyard),
                    WorkerConfig::new(
                        worker_name,
                        0,
                        false,
                        Arc::clone(&self.max_idle),
                    ),
                    &self.status_behavior
                )
            );
        });

        self.last_worker_id += count;
    }

    fn graveyard_trim(&mut self) {
        let g = {
            let mut g = self.graveyard.write();

            if g.is_empty() {
                return;
            }

            g.drain().collect::<HashSet<usize>>()
        };

        let mut last = self.workers.len();
        let mut index = 0;

        while index < last {
            if g.contains(&self.workers[index].get_id()) {
                last -= 1;
                if self.workers.swap_remove(index).get_id() == self.last_worker_id {
                    // update the last worker id, since the last one has been released
                    self.last_worker_id -= 1;
                }
            }

            index += 1;
        }
    }
}

pub(crate) trait WorkerManagement {
    fn workers_count(&self) -> usize;
    fn worker_auto_expire(&mut self, life_in_millis: usize);
    fn extend_by(&mut self, more: usize);
    fn shrink_by(&mut self, less:usize) -> Vec<usize>;
    fn dismiss_worker(&mut self, id: usize) -> Option<usize>;
    fn first_worker_id(&self) -> usize;
    fn last_worker_id(&self) -> usize;
    fn next_worker_id(&self, curr_id: usize) -> usize;
}

impl WorkerManagement for Manager {
    fn workers_count(&self) -> usize {
        self.workers.len()
    }

    fn worker_auto_expire(&mut self, life_in_millis: usize) {
        self.max_idle.swap(life_in_millis, Ordering::SeqCst);
    }

    fn extend_by(&mut self, more: usize) {
        self.add_workers(more);
    }

    fn shrink_by(&mut self, less: usize) -> Vec<usize> {
        if less == 0 {
            return Vec::new();
        }

        let start = self.workers.len() - less;
        self.workers
            .drain(start..)
            .map(|w| w.get_id())
            .collect()
    }

    fn dismiss_worker(&mut self, id: usize) -> Option<usize> {
        for idx in 0..self.workers.len() {
            if self.workers[idx].get_id() == id {
                // swap out the worker, use swap_remove for better performance.
                return Some(self.workers.swap_remove(idx).get_id());
            }
        }

        None
    }

    fn first_worker_id(&self) -> usize {
        match self.workers.first() {
            Some(worker) => worker.get_id(),
            None => 0
        }
    }

    fn last_worker_id(&self) -> usize {
        match self.workers.last() {
            Some(worker) => worker.get_id(),
            None => 0
        }
    }

    fn next_worker_id(&self, curr_id: usize) -> usize {
        for worker in self.workers.iter() {
            let id = worker.get_id();
            if id == curr_id {
                return id;
            }
        }

        0
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.remove_all(true);
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