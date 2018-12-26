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

    pub fn execute_automode<F: FnOnce() + Send + 'static>(&mut self, f: F) {
        let adjustment = if let Ok(sender) = self.sender.lock() {
            let adjustment_private = get_adjustment_target(&self, &sender);
            sender.send(Message::NewJob(Box::new(f)));

            adjustment_private
        } else {
            if is_debug_mode() {
                eprintln!("Unable to execute the job: the sender seems to have been closed.");
            }

            None
        };

        if let Some(change) = adjustment {
            self.resize(change)
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

        for id in 0..more {
            let worker = Worker::new(start + id, Arc::clone(&self.receiver), false);
            self.workers.push(worker);
        }

        //TODO: update subscribers

        self.last_id += more;
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        let mut count = less;
        while count > 0 {
            if self.workers.len() == 0 {
                break;
            } else {
                if let Some(mut worker) = self.workers.pop() {
                    worker.terminate();
                }

                count -= 1;
            }
        }

        //TODO: update subscribers
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
            get_adjustment_target(&self, &sender)
        } else {
            None
        };

        if let Some(change) = adjustment {
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
                let mut worker = self.workers.swap_remove(index);
                worker.terminate();

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
            let mut done = false;

            let mut since = if is_privilege {
                None
            } else {
                Some(SystemTime::now())
            };

            loop {
                if let Ok(rx) = receiver.lock() {
                    if let Some(message) = rx.receiver.recv() {
                        // receiving a new message
                        assignment = Some(message);
                        done = rx.subscribers.contains(&my_id);
                    } else {
                        // sender has been dropped
                        return;
                    }
                }

                if let Some(message) = assignment.take() {
                    match message {
                        Message::NewJob(job) => {
                            job.call_box();

                            if done {
                                return;
                            }

                            if since.is_some() {
                                since = Some(SystemTime::now());
                            }
                        }
                        Message::Terminate(job_id) => {
                            if job_id == 0 {
                                return;
                            }
                            if job_id == my_id {
                                return;
                            }
                        }
                    }
                }
            }
        });

        Worker {
            id: my_id,
            thread: Some(thread),
        }
    }

    fn terminate(&mut self) {
        if let Some(thread) = self.thread.take() {
            // >_> kill the thread in another thread is less ideal, but we're not blocking!
            thread::spawn(move || {
                thread.join().unwrap_or_else(|e| {
                    eprintln!("Failed to join the associated thread: {:?}", e);
                });
            });
        }
    }
}

fn get_adjustment_target(pool: &ThreadPool, sender: &channel::Sender<Message>) -> Option<usize> {
    let worker_count = pool.workers.len();
    let queue_length = sender.len();

    if queue_length > AUTO_EXTEND_TRIGGER_SIZE && worker_count <= pool.auto_extend_threshold {
        // The workers size may be larger than the threshold, but that's okay since we won't
        // add more workers from this point on, unless some workers are killed.
        Some(worker_count + queue_length)
    } else if queue_length == 0 && worker_count > pool.init_size {
        if worker_count == (pool.init_size + 1) {
            Some(pool.init_size)
        } else {
            Some(((worker_count + pool.init_size) / 2) as usize)
        }
    } else {
        None
    }
}
