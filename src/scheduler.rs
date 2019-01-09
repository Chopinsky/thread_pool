use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use crossbeam_channel as channel;
use crossbeam_channel::{SendError, SendTimeoutError};
use crate::debug::is_debug_mode;
use crate::model::*;
use crate::worker::*;

const TIMEOUT_RETRY: i8 = 4;
const THRESHOLD: usize = 100000;
const AUTO_EXTEND_TRIGGER_SIZE: usize = 2;

//TODO: use atomic int for read-write locking...

pub enum ExecutionError {
    Timeout,
    Disconnected,
    PoolPoisoned,
}

pub struct ThreadPool {
    init_size: usize,
    last_id: usize,
    workers: Vec<Worker>,
    graveyard: Arc<RwLock<HashSet<usize>>>,
    sender: channel::Sender<Message>,
    receiver: channel::Receiver<Message>,
    auto_extend_threshold: usize,
    queue_timeout: Option<Duration>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (sender, receiver) = channel::unbounded();
        let graveyard = Arc::new(RwLock::new(HashSet::new()));

        let start = 1;
        let mut workers = Vec::with_capacity(pool_size);

        (start..start + pool_size).for_each(|id| {
            workers.push(Worker::new(id, receiver.clone(), Arc::clone(&graveyard), true));
        });

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", workers.len());
        }

        ThreadPool {
            init_size: pool_size,
            last_id: start + pool_size - 1,
            workers,
            graveyard,
            sender,
            receiver,
            auto_extend_threshold: THRESHOLD,
            queue_timeout: None,
        }
    }

    pub fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<(), ExecutionError> {
        match self.dispatch(Message::NewJob(Box::new(f)), -1) {
            Err(SendTimeoutError::Timeout(_)) => Err(ExecutionError::Timeout),
            Err(SendTimeoutError::Disconnected(_)) => Err(ExecutionError::Disconnected),
            Ok(_) => Ok(()),
        }
    }

    pub fn execute_with_autoscale<F: FnOnce() + Send + 'static>(&mut self, f: F) -> Result<(), ExecutionError> {
        match self.dispatch(Message::NewJob(Box::new(f)), 0) {
            Err(SendTimeoutError::Timeout(_)) => Err(ExecutionError::Timeout),
            Err(SendTimeoutError::Disconnected(_)) => Err(ExecutionError::Disconnected),
            Ok(is_busy) => {
                if is_busy {
                    if let Some(change) = self.resize_target(self.init_size) {
                        self.resize(change);
                    }
                }

                Ok(())
            },
        }
    }

    fn resize_target(&self, queue_length: usize) -> Option<usize> {
        if queue_length == 0 {
            return None;
        }

        let worker_count = self.workers.len();
        if queue_length > AUTO_EXTEND_TRIGGER_SIZE && worker_count <= self.auto_extend_threshold {
            // The workers size may be larger than the threshold, but that's okay since we won't
            // add more workers from this point on, unless some workers are killed.
            Some(worker_count + queue_length)
        } else if queue_length == 0 && worker_count > self.init_size {
            if worker_count == (self.init_size + 1) {
                Some(self.init_size)
            } else {
                Some(((worker_count + self.init_size) / 2) as usize)
            }
        } else {
            None
        }
    }

    fn dispatch(&self, message: Message, retry: i8) -> Result<bool, SendTimeoutError<Message>> {
        let is_busy = if !self.sender.is_empty() {
            // if the channel is not empty -- meaning workers are all busy at the moment, we need
            // to double the size of the workers pool
            true
        } else {
            false
        };

        match self.queue_timeout {
            Some(wait_period) => {
                let factor = if retry > 0 {
                    retry as u32
                } else {
                    1
                };

                return match self.sender.send_timeout(message, factor * wait_period) {
                    Ok(()) => Ok(is_busy),
                    Err(SendTimeoutError::Disconnected(msg)) => Err(SendTimeoutError::Disconnected(msg)),
                    Err(SendTimeoutError::Timeout(msg)) => {
                        if retry < 0 || retry > TIMEOUT_RETRY {
                            return Err(SendTimeoutError::Timeout(msg));
                        }

                        return self.dispatch(msg, retry + 1);
                    },
                };
            },
            None => {
                if let Err(SendError(message)) = self.sender.send(message) {
                    return Err(SendTimeoutError::Disconnected(message));
                };
            }
        }

        Ok(is_busy)
    }
}

pub trait PoolManager {
    fn extend(&mut self, more: usize);
    fn shrink(&mut self, less: usize);
    fn resize(&mut self, total: usize);
    fn auto_adjust(&mut self);
    fn auto_expire(&mut self);
    fn kill_worker(&mut self, id: usize);
    fn clear(&mut self);
    fn close(&mut self);
}

impl PoolManager for ThreadPool {
    fn extend(&mut self, more: usize) {
        if more == 0 {
            return;
        }

        // the start id is the next integer from the last worker's id
        let start = self.last_id + 1;

        (0..more).for_each(|id| {
            // Worker is created to subscribe, but would register self later when pulled from the
            // workers queue
            self.workers
                .push(Worker::new(
                    start + id,
                    self.receiver.clone(),
                    Arc::clone(&self.graveyard),
                    false
                ));
        });

        self.last_id += more;
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        let count = self.workers.len() - less;
        let (_, workers) = self.workers.split_at(count);

        for worker in workers {
            //todo: instead of sending message, operate now!

            // send the termination message -- async kill as this is not an urgent task
            if self.sender.send(Message::Terminate(worker.get_id())).is_err() && is_debug_mode() {
                eprintln!("Failed to send the termination message to worker: {}", worker.get_id());
            }
        }
    }

    fn resize(&mut self, total: usize) {
        if total == 0 {
            return;
        }

        let worker_count = self.workers.len();
        if total == worker_count {
            return;
        } else if total > worker_count {
            self.extend(total - worker_count);
        } else {
            self.shrink(worker_count - total);
        }
    }

    fn auto_adjust(&mut self) {
        if let Some(change) = self.resize_target(self.get_queue_length()) {
            self.resize(change);
        }
    }

    // Let extended workers to expire when idling for too long.
    fn auto_expire(&mut self) {
        // TODO: set query messages
    }

    fn kill_worker(&mut self, id: usize) {
        for idx in 0..self.workers.len() {
            if self.workers[idx].get_id() == id {
                // swap out the worker, use swap_remove for better performance.
                let worker = self.workers.swap_remove(idx);
                if self.sender.send(Message::Terminate(worker.get_id())).is_err() && is_debug_mode() {
                    eprintln!("Failed to send the termination message to worker: {}", id);
                }

                if is_debug_mode() {
                    println!("Worker {} is told to be terminated...", worker.get_id());
                }

                return;
            }
        }
    }

    fn clear(&mut self) {
        let mut sent = false;
        if let Ok(()) = self.sender.send(Message::Terminate(0)) {
            sent = true;
        }

        if !sent {
            // abort the clear process if we can't send the terminate message
            if is_debug_mode() {
                eprintln!("Failed to send the terminate message, please try again...");
            }

            return;
        }

        for worker in self.workers.drain(..) {
            if is_debug_mode() {
                println!("Retiring worker {}", worker.get_id());
            }
        }
    }

    fn close(&mut self) {
        self.clear();

    }
}

pub trait PoolState {
    fn get_size(&self) -> usize;
    fn get_queue_length(&self) -> usize;
    fn get_queue_size_threshold(&self) -> usize;
    fn set_queue_size_threshold(&mut self, threshold: usize);
    fn get_first_worker_id(&self) -> Option<usize>;
    fn get_last_worker_id(&self) -> Option<usize>;
    fn get_next_worker_id(&self, id: usize) -> Option<usize>;
}

impl PoolState for ThreadPool {
    #[inline]
    fn get_size(&self) -> usize {
        self.workers.len()
    }

    #[inline]
    fn get_queue_length(&self) -> usize {
        self.sender.len()
    }

    #[inline]
    fn get_queue_size_threshold(&self) -> usize {
        self.auto_extend_threshold
    }

    fn set_queue_size_threshold(&mut self, threshold: usize) {
        if threshold > THRESHOLD && is_debug_mode() {
            eprintln!(
                "WARNING: You're trying to set the queue size larger than the soft maximum threshold of 100000, this could cause drop of performance"
            );
        }

        self.auto_extend_threshold = if threshold > self.init_size {
            threshold
        } else {
            self.init_size
        };
    }

    fn get_first_worker_id(&self) -> Option<usize> {
        if let Some(worker) = self.workers.first() {
            return Some(worker.get_id());
        }

        None
    }

    fn get_last_worker_id(&self) -> Option<usize> {
        if let Some(worker) = self.workers.last() {
            return Some(worker.get_id());
        }

        None
    }

    fn get_next_worker_id(&self, current_id: usize) -> Option<usize> {
        if current_id >= self.workers.len() {
            return None;
        }

        let mut found = false;
        for worker in &self.workers {
            if found {
                return Some(worker.get_id());
            }

            if worker.get_id() == current_id {
                found = true;
            }
        }

        None
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if is_debug_mode() {
            println!("Job done, sending terminate message to all workers.");
        }

        self.clear();
    }
}