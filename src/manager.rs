#![allow(dead_code)]

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::vec;

use crossbeam_channel::Receiver;
use parking_lot::RwLock;
use crate::debug::is_debug_mode;
use crate::model::{Message, WorkerUpdate};
use crate::worker::{Worker, WorkerConfig};

pub(crate) struct Manager {
    name: Option<String>,
    workers: Vec<Worker>,
    last_worker_id: usize,
    graveyard: Arc<RwLock<Vec<i8>>>,
    max_idle: Arc<AtomicUsize>,
    status_behavior: StatusBehaviors,
}

impl Manager {
    pub(crate) fn new(
        name: Option<String>,
        range: usize,
        rx: &Receiver<Message>,
        pri_rx: &Receiver<Message>,
        behavior: StatusBehaviors,
    ) -> Manager
    {
        let mut workers = Vec::with_capacity(range);
        let max_idle = Arc::new(AtomicUsize::new(0));

        let graveyard =
            Arc::new(RwLock::new(vec::from_elem(0i8, range + 1)));

        (1..=range).for_each(|id| {
            let worker_name = name.as_ref().and_then(|name| {
                Some(format!("{}-{}", name, id))
            });

            workers.push(Worker::new(
                id,
                rx.clone(),
                pri_rx.clone(),
                Arc::clone(&graveyard),
                WorkerConfig::new(
                    worker_name,
                    0,
                    true,
                    Arc::clone(&max_idle)
                ),
                &behavior
            ));
        });

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", workers.len());
        }

        Manager {
            name,
            workers,
            last_worker_id: range,
            graveyard,
            max_idle,
            status_behavior: behavior,
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
    }

    fn mark_worker(&mut self, id: usize, status: i8) {
        let mut g = self.graveyard.write();
        if id >= g.len() {
            return;
        }

        (*g)[id] = status;
    }

    fn graveyard_trim(&mut self) {
        let g_read = self.graveyard.read();

        let mut idx = g_read.len();
        while idx > 1 {
            idx -= 1;
            if g_read[idx] != -1 {
                break;
            }
        }

        if idx > 0 && idx < g_read.len() {
            let mut g_write = self.graveyard.write();
            (*g_write).truncate(idx + 1);  // index is 0-based, so length should be +1.
            self.last_worker_id = idx;
        }
    }
}

pub(crate) trait WorkerManagement {
    fn workers_count(&self) -> usize;
    fn worker_auto_expire(&mut self, life_in_millis: usize);
    fn extend_by(&mut self, more: usize, receiver: &Receiver<Message>, priority_receiver: &Receiver<Message>);
    fn shrink_by(&mut self, less:usize) -> Vec<Worker>;
    fn dismiss_worker(&mut self, id: usize) -> Option<Worker>;
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

    fn extend_by(&mut self, more: usize, receiver: &Receiver<Message>, priority_receiver: &Receiver<Message>) {
        if more == 0 {
            return;
        }

        // remove killed ids.
        self.graveyard_trim();

        // the start id is the next integer from the last worker's id
        (1..=more).for_each(|offset| {
            // Worker is created to subscribe, but would register self later when pulled from the
            // workers queue
            let id = self.last_worker_id + offset;
            let worker_name = self.name.as_ref().and_then(|name| {
                Some(format!("{}-{}", name, id))
            });

            self.workers
                .push(Worker::new(
                    id,
                    receiver.clone(),
                    priority_receiver.clone(),
                    Arc::clone(&self.graveyard),
                    WorkerConfig::new(
                        worker_name, 0, false, Arc::clone(&self.max_idle)
                    ),
                    &self.status_behavior
                ));
        });

        {
            let mut g = self.graveyard.write();
            (*g).reserve_exact(more);
            (*g).extend(vec::from_elem(0, more));
        }

        self.last_worker_id += more;
    }

    fn shrink_by(&mut self, less: usize) -> Vec<Worker> {
        if less == 0 {
            return Vec::new();
        }

        let start = self.workers.len() - less;
        let workers: Vec<Worker> = self.workers.drain(start..).collect();

        {
            let mut g = self.graveyard.write();
            for worker in workers.iter(){
                let id = worker.get_id();
                if id < g.len() {
                    (*g)[id] = -1;
                }
            }
        }

        // remove killed ids.
        self.graveyard_trim();

        workers
    }

    fn dismiss_worker(&mut self, id: usize) -> Option<Worker> {
        for idx in 0..self.workers.len() {
            if self.workers[idx].get_id() == id {
                // swap out the worker, use swap_remove for better performance.
                self.mark_worker(id, -1);
                return Some(self.workers.swap_remove(idx));
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