use std::thread;
use std::sync::{Arc, Mutex};
use debug::is_debug_mode;
use crossbeam_channel as channel;

type Job = Box<FnBox + Send + 'static>;
static THRESHOLD: usize = 100000;

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

pub struct ThreadPool {
    workers: Vec<Worker>,
    last_id: usize,
    sender: Mutex<channel::Sender<Message>>,
    receiver: Arc<Mutex<channel::Receiver<Message>>>,
    auto_extend_threshold: usize,
    init_size: usize
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (sender, receiver) = channel::unbounded();
        let receiver = Arc::new(Mutex::new(receiver));
        let start = 1;

        let mut workers = Vec::with_capacity(pool_size);
        for id in 0..pool_size {
            workers.push(Worker::new(start + id, Arc::clone(&receiver)));
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

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Ok(sender) = self.sender.lock() {
            sender.send(Message::NewJob(Box::new(f)));
        } else if is_debug_mode() {
            eprintln!("Unable to execute the job: the sender seems to have been closed.");
        }
    }

    //TODO: add execute_flexible to extend the worker queue when full or above threshold
}

pub trait PoolManager {
    fn extend(&mut self, more: usize);
    fn shrink(&mut self, less: usize);
    fn resize(&mut self, total: usize);
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
            let worker = Worker::new(start + id, Arc::clone(&self.receiver));
            self.workers.push(worker);
        }

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
                count -= 1;
            }

            if let Some(mut worker) = self.workers.pop() {
                Worker::terminate(&self, &mut worker);
            }
        }
    }

    fn resize(&mut self, total: usize) {
        let len = self.workers.len();

        if total == len {
            return;
        } else if total > len {
            self.extend(total - len);
        } else {
            self.shrink(len - total);
        }
    }

    fn kill_worker(&mut self, id: usize) {
        let mut index = 0;

        while index < self.workers.len() {
            if self.workers[index].id == id {
                // swap out the worker, use swap_remove for better performance.
                let mut worker = self.workers.swap_remove(index);
                Worker::terminate(&self, &mut worker);

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

        self.auto_extend_threshold =
            if threshold > self.init_size {
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
    fn new(my_id: usize, receiver: Arc<Mutex<channel::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || {
            let mut new_assignment = None;

            loop {
                if let Ok(rx) = receiver.lock() {
                    if let Some(message) = rx.recv() {
                        // receiving a new message
                        new_assignment = Some(message);
                    } else {
                        // sender has been dropped
                        break;
                    }
                }

                if let Some(message) = new_assignment.take() {
                    match message {
                        Message::NewJob(job) => job.call_box(),
                        Message::Terminate(job_id) => {
                            if job_id == 0 {
                                break;
                            }
                            if job_id == my_id {
                                break;
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

    fn terminate(pool: &ThreadPool, worker: &mut Worker) {
        if let Ok(sender) = pool.sender.lock() {
            sender.send(Message::Terminate(worker.id));
        }

        if let Some(thread) = worker.thread.take() {
            thread
                .join()
                .expect("Couldn't join on the associated thread");
        }
    }
}
