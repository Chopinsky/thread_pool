use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use crossbeam_channel::Receiver;
use crate::debug::is_debug_mode;
use crate::model::Message;
use crate::worker::Worker;

const START_ID: usize = 1;

pub(crate) struct Manager {
    workers: Vec<Worker>,
    last_id: usize,
    graveyard: Arc<RwLock<HashSet<usize>>>,
}

impl Manager {
    pub(crate) fn new(range: usize, rx: &Receiver<Message>) -> Manager {
        let mut workers = Vec::with_capacity(range);
        let graveyard =
            Arc::new(RwLock::new(HashSet::with_capacity(range)));

        (START_ID..START_ID + range).for_each(|id| {
            workers.push(Worker::new(
                id,
                rx.clone(),
                Arc::clone(&graveyard),
                true
            ));
        });

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", workers.len());
        }

        Manager {
            workers,
            last_id: START_ID + range - 1,
            graveyard,
        }
    }

    pub(crate) fn remove_all(&mut self) {
        for mut worker in self.workers.drain(..) {
            if is_debug_mode() {
                println!("Sync retiring worker {}", worker.get_id());
            }

            // call retire so we will block until all workers have been awakened again, meaning
            // all their work is now done and threads joined.
            worker.retire();
        }
    }
}

pub(crate) trait WorkerManagement {
    fn workers_count(&self) -> usize;
    fn extend_by(&mut self, more: usize, receiver: &Receiver<Message>);
    fn shrink_by(&mut self, less:usize) -> Vec<Worker>;
    fn dismiss_worker(&mut self, id: usize) -> bool;
    fn first_worker_id(&self) -> usize;
    fn last_worker_id(&self) -> usize;
    fn next_worker_id(&self, curr_id: usize) -> usize;
}

impl WorkerManagement for Manager {
    fn workers_count(&self) -> usize {
        self.workers.len()
    }

    fn extend_by(&mut self, more: usize, receiver: &Receiver<Message>) {
        if more == 0 {
            return;
        }

        // the start id is the next integer from the last worker's id
        (0..more).for_each(|id| {
            // Worker is created to subscribe, but would register self later when pulled from the
            // workers queue
            self.workers
                .push(Worker::new(
                    self.last_id + 1 + id,
                    receiver.clone(),
                    Arc::clone(&self.graveyard),
                    false
                ));
        });

        self.last_id += more;
    }

    fn shrink_by(&mut self, less: usize) -> Vec<Worker> {
        if less == 0 {
            return Vec::new();
        }

        let start = self.workers.len() - less;
        self.workers.drain(start..).collect()
    }

    fn dismiss_worker(&mut self, id: usize) -> bool {
        for idx in 0..self.workers.len() {
            if self.workers[idx].get_id() == id {
                // swap out the worker, use swap_remove for better performance.
                self.workers.swap_remove(idx);
                return true;
            }
        }

        false
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
        self.remove_all();
    }
}