use crate::debug::is_debug_mode;
use crossbeam_channel as channel;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;

type Job = Box<FnBox + Send + 'static>;

static THRESHOLD: usize = 100000;
static AUTO_EXTEND_TRIGGER_SIZE: usize = 2;

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

enum Message {
    NewJob(Job),
    Terminate(usize),
}

struct Dispatcher {
    receiver: channel::Receiver<Message>,
    subscribers: HashSet<usize>,
    graveyard: HashSet<usize>,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    last_id: usize,
    sender: Mutex<channel::Sender<Message>>,
    receiver: Arc<Mutex<Dispatcher>>,
    auto_extend_threshold: usize,
    init_size: usize,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (sender, receiver) = channel::unbounded();
        let receiver = Arc::new(Mutex::new(Dispatcher {
            receiver,
            subscribers: HashSet::new(),
            graveyard: HashSet::new(),
        }));

        let start = 1;

        let mut workers = Vec::with_capacity(pool_size);
        let mut worker_ids: HashSet<usize> = HashSet::with_capacity(pool_size);

        for id in 0..pool_size {
            let worker_id = start + id;
            worker_ids.insert(worker_id);
            workers.push(Worker::new(worker_id, Arc::clone(&receiver), true));
        }

        if let Ok(mut recv_inner) = receiver.lock() {
            recv_inner.subscribers = worker_ids;
        }

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", workers.len());
        }

        ThreadPool {
            workers,
            last_id: start + pool_size - 1,
            sender: Mutex::new(sender),
            receiver,
            auto_extend_threshold: THRESHOLD,
            init_size: pool_size,
        }
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) {
        if let Ok(sender) = self.sender.lock() {
            sender.send(Message::NewJob(Box::new(f)));
        } else if is_debug_mode() {
            eprintln!("Unable to execute the job: the sender seems to have been closed.");
        }
    }

    pub fn execute_and_balance<F: FnOnce() + Send + 'static>(&mut self, f: F) {
        let adjustment = if let Ok(sender) = self.sender.lock() {
            sender.send(Message::NewJob(Box::new(f)));
            sender.len()
        } else {
            0
        };

        if let Some(change) = self.get_adjustment_target(adjustment) {
            self.resize(change);
        }
    }

    fn get_adjustment_target(&self, queue_length: usize) -> Option<usize> {
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

        if let Ok(sender) = self.sender.lock() {
            for worker in workers {
                // send the termination message -- async kill as this is not an urgent task
                sender.send(Message::Terminate(worker.id));
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
        let adjustment = if let Ok(sender) = self.sender.lock() {
            sender.len()
        } else {
            0
        };

        if let Some(change) = self.get_adjustment_target(adjustment) {
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
                if let Ok(sender) = self.sender.lock() {
                    sender.send(Message::Terminate(worker.id));
                }

                return;
            }

            index += 1;
        }
    }

    fn clear(&mut self) {
        if let Ok(sender) = self.sender.lock() {
            for _ in &mut self.workers {
                sender.send(Message::Terminate(0));
            }
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap_or_else(|e| {
                    eprintln!("Unable to join the thread: {:?}", e);
                });

                if is_debug_mode() {
                    println!("Worker {} is done...", worker.id);
                }
            }
        }
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
        match self.sender.lock() {
            Ok(sender) => Ok(sender.len()),
            Err(_) => Err("Unable to obtain message queue size."),
        }
    }

    #[inline]
    fn get_queue_size_threshold(&self) -> usize {
        self.auto_extend_threshold
    }

    fn set_queue_size_threshold(&mut self, threshold: usize) {
        if threshold > THRESHOLD {
            eprintln!(
                "WARNING: You're trying to set the queue size larger than the soft maximum threshold of 100000, this could cause drop of performance");
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

impl Worker {
    fn new(my_id: usize, receiver: Arc<Mutex<Dispatcher>>, is_privilege: bool) -> Worker {
        let thread = thread::spawn(move || {
            let mut assignment = None;
            let mut since = if is_privilege {
                None
            } else {
                Some(SystemTime::now())
            };

            loop {
                if let Ok(mut rx) = receiver.lock() {
                    // update subscribers
                    if !rx.subscribers.contains(&my_id) {
                        if rx.graveyard.contains(&my_id) {
                            // actually remove the subscriber and clean up the graveyard
                            rx.graveyard.remove(&my_id);
                            return;
                        } else {
                            // async add the new worker, if not killed yet
                            rx.subscribers.insert(my_id);
                        }
                    }

                    if let Some(message) = rx.receiver.recv() {
                        match message {
                            Message::NewJob(job) => assignment = Some(job),
                            Message::Terminate(target_id) => {
                                if target_id == 0 {
                                    // all clear signal, remove self from both lists
                                    rx.subscribers.remove(&my_id);
                                    rx.graveyard.remove(&my_id);
                                    return;
                                } else {
                                    // async kill signal, update the lists with the target id
                                    rx.subscribers.remove(&target_id);
                                    rx.graveyard.insert(target_id);
                                }
                            }
                        }
                    } else {
                        // sender has been dropped
                        return;
                    }
                }

                if assignment.is_some() {
                    Worker::handle_work(&mut assignment, &mut since)
                }
            }
        });

        Worker {
            id: my_id,
            thread: Some(thread),
        }
    }

    fn handle_work(assignment: &mut Option<Job>, since: &mut Option<SystemTime>) {
        if let Some(job) = assignment.take() {
            job.call_box();

            if since.is_some() {
                *since = Some(SystemTime::now());
            }
        }
    }
}
