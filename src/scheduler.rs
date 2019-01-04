use crate::debug::is_debug_mode;
use crate::inbox::*;
use crate::model::*;
use crossbeam_channel as channel;
use std::sync::{Arc, RwLock, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

static THRESHOLD: usize = 100000;
static AUTO_EXTEND_TRIGGER_SIZE: usize = 2;

pub enum ExecutionError {
    Timeout,
    Disconnected,
    PoolPoisoned,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    last_id: usize,
    sender: RwLock<channel::Sender<Message>>,
    receiver: Arc<Mutex<Inbox>>,
    auto_extend_threshold: usize,
    init_size: usize,
    queue_timeout: Option<Duration>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (inbox, sender) = Inbox::new();
        let shared_receiver = Arc::new(Mutex::new(inbox));

        let start = 1;
        let mut workers = Vec::with_capacity(pool_size);

        if let Ok(mut inner) = shared_receiver.lock() {
            for id in 0..pool_size {
                let worker_id = start + id;
                inner.insert(worker_id);
                workers.push(Worker::new(worker_id, Arc::clone(&shared_receiver), true));
            }


            if is_debug_mode() {
                println!("Pool has been initialized with {} pools", workers.len());
            }
        }

        ThreadPool {
            workers,
            last_id: start + pool_size - 1,
            sender: RwLock::new(sender),
            receiver: shared_receiver,
            auto_extend_threshold: THRESHOLD,
            init_size: pool_size,
            queue_timeout: None,
        }
    }

    pub fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<(), ExecutionError> {
        if let Ok(sender) = self.sender.write() {
            match self.queue_timeout {
                Some(wait_period) => {
                    if sender
                        .send_timeout(Message::NewJob(Box::new(f)), wait_period)
                        .is_err()
                    {
                        return Err(ExecutionError::Timeout);
                    }
                },
                None => {
                    if sender.send(Message::NewJob(Box::new(f))).is_err() {
                        return Err(ExecutionError::Disconnected);
                    }
                }
            }

            return Ok(())
        }

        Err(ExecutionError::PoolPoisoned)
    }

    pub fn execute_and_balance<F: FnOnce() + Send + 'static>(&mut self, f: F) -> Result<(), ExecutionError> {
        let mut base_change = 0;

        if let Ok(sender) = self.sender.write() {
            if !sender.is_empty() {
                // if the channel is not empty -- meaning workers are all busy at the moment, we need
                // to double the size of the workers pool
                base_change = sender.len();
            }

            match self.queue_timeout {
                Some(wait_period) => {
                    if sender
                        .send_timeout(Message::NewJob(Box::new(f)), wait_period)
                        .is_err()
                    {
                        return Err(ExecutionError::Timeout);
                    }
                },
                None => {
                    if sender.send(Message::NewJob(Box::new(f))).is_err() {
                        return Err(ExecutionError::Disconnected);
                    }
                }
            }
        } else {
            return Err(ExecutionError::PoolPoisoned);
        };

        if let Some(change) = self.resize_target(base_change) {
            self.resize(change);
        }

        Ok(())
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
}

pub trait PoolManager {
    fn extend(&mut self, more: usize);
    fn shrink(&mut self, less: usize);
    fn resize(&mut self, total: usize);
    fn auto_adjust(&mut self);
    fn auto_expire(&mut self);
    fn kill_worker(&mut self, id: usize);
    fn clear(&mut self);
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
                .push(Worker::new(start + id, Arc::clone(&self.receiver), false));
        });

        self.last_id += more;
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        let count = self.workers.len() - less;
        let (_, workers) = self.workers.split_at(count);

        if let Ok(sender) = self.sender.read() {
            for worker in workers {
                // send the termination message -- async kill as this is not an urgent task
                if sender.send(Message::Terminate(worker.id)).is_err() && is_debug_mode() {
                    eprintln!("Failed to send the termination message to worker: {}", worker.id);
                }
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
        let curr = if let Ok(sender) = self.sender.read() {
            sender.len()
        } else {
            0
        };

        if let Some(change) = self.resize_target(curr) {
            self.resize(change);
        }
    }

    // Let extended workers to expire when idling for too long.
    fn auto_expire(&mut self) {
        // send query messages
    }

    fn kill_worker(&mut self, id: usize) {
        let mut index = 0;

        while index < self.workers.len() {
            if self.workers[index].id == id {
                // swap out the worker, use swap_remove for better performance.
                let worker = self.workers.swap_remove(index);
                if let Ok(sender) = self.sender.write() {
                    if sender.send(Message::Terminate(worker.id)).is_err() && is_debug_mode() {
                        eprintln!("Failed to send the termination message to worker: {}", id);
                    }
                }

                if is_debug_mode() {
                    println!("Worker {} is told to be terminated...", worker.id);
                }

                return;
            }

            index += 1;
        }
    }

    fn clear(&mut self) {
        let mut sent = false;
        if let Ok(sender) = self.sender.write() {
            if let Ok(()) = sender.send(Message::Terminate(0)) {
                sent = true;
            }
        }

        if !sent {
            // abort the clear process if we can't send the terminate message
            if is_debug_mode() {
                eprintln!("Failed to send the terminate message, please try again...");
            }

            return;
        }

        self.workers.clear();
    }
}

pub trait PoolState {
    fn get_size(&self) -> usize;
    fn get_queue_length(&self) -> Result<usize, &'static str>;
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

    fn get_queue_length(&self) -> Result<usize, &'static str> {
        match self.sender.read() {
            Ok(sender) => Ok(sender.len()),
            Err(_) => Err("Unable to obtain message queue size"),
        }
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
            return Some(worker.id);
        }

        None
    }

    fn get_last_worker_id(&self) -> Option<usize> {
        if let Some(worker) = self.workers.last() {
            return Some(worker.id);
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
                return Some(worker.id);
            }

            if worker.id == current_id {
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

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

struct WorkerCommand {
    done: bool,
    work: Option<Job>,
}

impl Worker {
    fn new(my_id: usize, inbox: Arc<Mutex<Inbox>>, is_privilege: bool) -> Worker {
        let thread = thread::spawn(move || {
            let mut command = WorkerCommand {
                done: false,
                work: None,
            };

            let mut since = if is_privilege {
                None
            } else {
                Some(SystemTime::now())
            };

            loop {
                if let Ok(mut locked_box) = inbox.lock() {
                    // update subscribers
                    if !locked_box.verify(my_id) {
                        return;
                    }

                    //TODO: auto expire, if used, take effect here...

                    match locked_box.receive() {
                        Ok(message) => {
                            // message is the only place that can update the "done" field
                            Worker::unpack_message(message, my_id, &mut locked_box, &mut command);

                            if command.done {
                                return;
                            }
                        },
                        Err(channel::RecvTimeoutError::Timeout) => {
                            // yield the receiver lock to the next worker
                            continue;
                        },
                        Err(channel::RecvTimeoutError::Disconnected) => {
                            // sender has been dropped
                            return;
                        }
                    }
                }

                if command.work.is_some() {
                    Worker::handle_work(command.work.take(), &mut since)
                }
            }
        });

        Worker {
            id: my_id,
            thread: Some(thread),
        }
    }

    fn handle_work(work: Option<Job>, since: &mut Option<SystemTime>) {
        if let Some(work) = work {
            work.call_box();

            if since.is_some() {
                *since = Some(SystemTime::now());
            }
        }
    }

    fn unpack_message(message: Message, worker_id: usize, inbox: &mut Inbox, command: &mut WorkerCommand) {
        match message {
            Message::NewJob(job) => {
                command.work = Some(job);
                return;
            },
            Message::Terminate(target_id) => {
                if target_id == 0 && inbox.len() > 0 {
                    // all clear signal, remove self from both lists
                    inbox.kill(worker_id, false);
                    inbox.clear();
                } else if target_id == worker_id {
                    // rare case, where the worker happen to pick up the message
                    // to kill self, then do it
                    inbox.kill(worker_id, false);
                } else {
                    // async kill signal, update the lists with the target id
                    inbox.kill(target_id, true);
                    return;
                }
            }
        }

        command.done = true;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if is_debug_mode() {
            println!("Dropping worker {}", self.id);
        }

        if let Some(thread) = self.thread.take() {
            // make sure the work is done
            thread.join().unwrap_or_else(|err| {
                eprintln!("Unable to drop worker: {}, error: {:?}", self.id, err);
            });
        }
    }
}
