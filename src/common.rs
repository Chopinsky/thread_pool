use std::thread;
use std::sync::{Arc, mpsc, Mutex};
use debug::is_debug_mode;

type Job = Box<FnBox + Send + 'static>;

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
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ => size,
        };

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let start = 1;

        let mut workers = Vec::with_capacity(pool_size);
        for id in 0..pool_size {
            workers.push(Worker::new((start + id), Arc::clone(&receiver)));
        }

        if is_debug_mode() {
            println!("Pool has been initialized with {} pools", workers.len());
        }

        ThreadPool {
            workers,
            last_id: start + pool_size - 1,
            sender,
            receiver,
        }
    }

    pub fn execute<F>(&self, f: F) where F: FnOnce() + Send + 'static {
        let job = Box::new(f);
        self.sender.send(Message::NewJob(job)).unwrap_or_else(|err| {
            eprintln!("Unable to distribute the job: {}", err);
        });
    }
}

pub trait PoolManager {
    fn extend(&mut self, more: usize);
    fn shrink(&mut self, less: usize);
    fn resize(&mut self, size: usize);
    fn kill_worker(&mut self, id: usize);
    fn clear(&mut self);
}

impl PoolManager for ThreadPool {
    fn extend(&mut self, more: usize) {
        if more == 0 { return; }

        // the start id is the next integer from the last worker's id
        let start = self.last_id + 1;

        for id in 0..more {
            let worker = Worker::new((start + id), Arc::clone(&self.receiver));
            self.workers.push(worker);
        }

        self.last_id += more;
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 { return; }

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

    fn resize(&mut self, size: usize) {
        let len = self.workers.len();

        if size == len {
            return;
        } else if size > len {
            self.extend(size - len);
        } else {
            self.shrink(len - size);
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
        for _ in &mut self.workers {
            self.sender.send(Message::Terminate(0)).unwrap_or_else(|err| {
                eprintln!("Unable to send message: {}", err);
            });
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().expect("Couldn't join on the associated thread");
            }
        }
    }
}

pub trait PoolState {
    fn get_size(&self) -> usize;
    fn get_first_worker_id(&self) -> Option<usize>;
    fn get_last_worker_id(&self) -> Option<usize>;
    fn get_next_worker_id(&self, id: usize) -> Option<usize>;
}

impl PoolState for ThreadPool {
    #[inline]
    fn get_size(&self) -> usize {
        self.workers.len()
    }

    fn get_first_worker_id(&self) -> Option<usize> {
        if let Some(worker) = self.workers.first() {
            return Some(worker.id)
        }

        None
    }

    fn get_last_worker_id(&self) -> Option<usize> {
        if let Some(worker) = self.workers.last() {
            return Some(worker.id)
        }

        None
    }

    fn get_next_worker_id(&self, current_id: usize) -> Option<usize> {
        if current_id >= self.workers.len() { return None; }

        let mut found = false;
        for worker in &self.workers {
            if found { return Some(worker.id) }
            if worker.id == current_id { found = true; }
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
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || {
            Worker::launch(id, receiver);
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }

    fn launch(my_id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) {
        let mut next_message: Option<Message> = None;

        loop {
            if let Ok(rx) = receiver.lock() {
                if let Ok(message) = rx.recv() {
                    // grab the message and release the queue, so we don't block the queue.
                    next_message = Some(message);
                }
            }

            if let Some(msg) = next_message.take() {
                match msg {
                    Message::NewJob(job) => job.call_box(),
                    Message::Terminate(id) => {
                        if id == 0 { break; }
                        if my_id == id { break; }
                    }
                }
            }
        }
    }

    fn terminate(pool: &ThreadPool, worker: &mut Worker) {
        pool.sender.send(Message::Terminate(worker.id)).unwrap_or_else(|err| {
            eprintln!("Unable to send message: {}", err);
        });

        if let Some(thread) = worker.thread.take() {
            thread.join().expect("Couldn't join on the associated thread");
        }
    }
}