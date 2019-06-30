use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel as channel;
use crossbeam_channel::{Sender, SendTimeoutError, TrySendError, SendError};
use crate::config::{Config, ConfigStatus};
use crate::debug::is_debug_mode;
use crate::model::*;
use crate::manager::*;

const RETRY_LIMIT: i8 = 4;
const CHAN_CAP: usize = 16;
const THRESHOLD: usize = 65535;
const AUTO_EXTEND_TRIGGER_SIZE: usize = 2;
static FORCE_CLOSE: AtomicBool = AtomicBool::new(false);

pub enum ExecutionError {
    Timeout,
    Uninitialized,
    Disconnected,
    PoolPoisoned,
}

pub struct ThreadPool {
    manager: Manager,
    chan: (Sender<Message>, Sender<Message>),
    init_size: usize,
    upgrade_threshold: usize,
    auto_extend_threshold: usize,
    auto_scale: bool,
    blocking: bool,
    closing: bool,
    queue_timeout: Option<Duration>,
}

impl ThreadPool {
    /// Create a `ThreadPool` with default configurations
    pub fn new(size: usize) -> ThreadPool {
        Self::new_with_config(size, Config::default())
    }

    /// Create a `ThreadPool` with supplied configurations
    pub fn new_with_config(size: usize, config: Config) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (tx, rx) = channel::bounded(CHAN_CAP);
        let (pri_tx, pri_rx) = channel::bounded(CHAN_CAP);

        let manager = Manager::new(
            config.pool_name(),
            pool_size,
            pri_rx,
            rx,
            config.worker_behavior()
        );

        ThreadPool {
            manager,
            chan: (pri_tx, tx),
            init_size: pool_size,
            upgrade_threshold: CHAN_CAP / 2,
            auto_extend_threshold: THRESHOLD,
            queue_timeout: None,
            auto_scale: false,
            blocking: config.blocking(),
            closing: false,
        }
    }

    pub fn lazy_create(size: usize) -> ThreadPool {
        Self::lazy_create_with_config(size, Config::default())
    }

    pub fn lazy_create_with_config(size: usize, config: Config) -> ThreadPool {
        //TODO: replace with real impl ...
        Self::new_with_config(size, config)
    }

    /// Set the time out period for a job to be queued. If `timeout` is defined as some duration,
    /// we will keep the new jobs in the queue for at least them amount of time when all workers of
    /// the pool are busy. If it's set to `None` and all workers are busy at the moment a job comes,
    /// we will either block the caller when pool's blocking setting is turned on, or return timeout
    /// error immediately when the setting is turned off.
    pub fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    /// Toggle if we should block the execution APIs when all workers are busy and we can't expand
    /// the thread pool anymore for any reasons.
    pub fn toggle_blocking(&mut self, blocking: bool) {
        self.blocking = blocking;
    }

    /// Toggle if we can add temporary workers when the `ThreadPool` is under pressure. The temporary
    /// works will retire after they have been idle for a period of time.
    pub fn toggle_auto_scale(&mut self, auto_scale: bool) {
        self.auto_scale = auto_scale;
    }

    /// `exec` will dispatch a closure to a free thread to be executed. If no thread are free at the
    /// moment and the pool setting allow adding new workers at pressure, some temporary workers will
    /// be created and added to the pool for executing the accumulated pending jobs; otherwise, this
    /// API will work the same way as the alternative: `execute`, that it will block the caller until
    /// a worker becomes available, or the job queue timed out, where the job will be dropped and the
    /// caller needs to send the job to queue for execution again, if it's needed.
    ///
    /// For highly competitive environments, such as using the pool to enable async operations for a
    /// web server, disabling `auto_scale` could cause starvation deadlock. However, a server can only
    /// have a limited resources for disposal, we can't create unlimited workers, there will be a
    /// limit where we have to either choose to drop the job, or put it into a queue for later execution,
    /// though that will mostly lie on caller's discretion.
    ///
    /// In addition, this API will take a `prioritized` parameter, which will allow more urgent job
    /// to be taken by the workers sooner than jobs without a priority.
    ///
    /// Note that if the `ThreadPool` is created using delayed pool initialization, i.e. created using
    /// either `lazy_create` or `lazy_create_with_config` APIs, then the pool will be initialized at
    /// the first time a job is to be executed.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate threads_pool;
    /// use threads_pool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut pool = ThreadPool::new(4);
    ///
    /// for id in 0..10 {
    ///     pool.exec(|| {
    ///         thread::sleep(Duration::from_secs(1));
    ///         println!("thread {} has been waken after 1 seconds ... ", id);
    ///     }, true);
    /// }
    /// ```
    pub fn exec<F: FnOnce() + Send + 'static>(&mut self, f: F, prioritized: bool)
        -> Result<(), ExecutionError>
    {
        if self.closing {
            return Err(ExecutionError::Disconnected);
        }

        if self.manager.workers_count() == 0 {
            if self.init_size > 0 {
                // lazy init the pool at the first job, or regenerate workers when all are purged
                self.manager.add_workers(self.init_size);
            } else {
                // no worker to take the job
                return Err(ExecutionError::Uninitialized);
            }
        }

        let retry = if self.auto_scale {
            0
        } else {
            -1
        };
        
        self.dispatch(Message::NewJob(Box::new(f)), retry, prioritized)
            .map(|busy| {
                if busy && self.auto_scale {
                    if let Some(target) = self.resize_target(self.init_size) {
                        self.resize(target);
                    }
                }
            })
            .map_err(|err| {
                match err {
                    SendTimeoutError::Timeout(_) => ExecutionError::Timeout,
                    SendTimeoutError::Disconnected(_) => ExecutionError::Disconnected,
                }
            })
    }

    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F)
        -> Result<(), ExecutionError>
    {
        if self.closing {
            return Err(ExecutionError::Disconnected);
        }

        // no worker to take the job
        if self.manager.workers_count() < 1 {
            return Err(ExecutionError::Uninitialized);
        }

        self.dispatch(Message::NewJob(Box::new(f)), -1, !self.chan.0.is_full())
            .map(|_| {})
            .map_err(|err| {
                match err {
                    SendTimeoutError::Timeout(_) => ExecutionError::Timeout,
                    SendTimeoutError::Disconnected(_) => ExecutionError::Disconnected,
                }
            })
    }

    fn dispatch(&self, message: Message, retry: i8, with_priority: bool)
        -> Result<bool, SendTimeoutError<Message>>
    {
        let chan =
            if with_priority || (self.chan.1.is_empty() && self.chan.0.len() <= self.upgrade_threshold) {
                // squeeze the work into the priority chan first even if some normal work is in queue
                &self.chan.0
            } else {
                // normal work and then priority queue is full
                &self.chan.1
            };

        let was_busy = chan.is_full();

        match self.queue_timeout {
            Some(wait_period) => {
                let factor = if retry > 0 {
                    retry as u32
                } else {
                    1
                };

                match chan.send_timeout(message, factor * wait_period) {
                    Ok(()) => Ok(was_busy),
                    Err(SendTimeoutError::Disconnected(msg)) => Err(SendTimeoutError::Disconnected(msg)),
                    Err(SendTimeoutError::Timeout(msg)) => {
                        if retry < 0 || retry > RETRY_LIMIT {
                            return Err(SendTimeoutError::Timeout(msg));
                        }

                        self.dispatch(msg, retry + 1, with_priority)
                    },
                }
            },
            None => {
                if self.blocking {
                    // wait until a worker is ready to take new work
                    match chan.send(message) {
                        Ok(()) => Ok(was_busy),
                        Err(SendError(msg)) => Err(SendTimeoutError::Disconnected(msg)),
                    }
                } else {
                    // timeout immediately if all workers are busy
                    match chan.try_send(message) {
                        Ok(()) => Ok(was_busy),
                        Err(TrySendError::Disconnected(msg)) => Err(SendTimeoutError::Disconnected(msg)),
                        Err(TrySendError::Full(msg)) => Err(SendTimeoutError::Timeout(msg)),
                    }
                }
            }
        }
    }

    fn resize_target(&self, queue_length: usize) -> Option<usize> {
        if queue_length == 0 {
            return None;
        }

        let worker_count = self.manager.workers_count();
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

    pub(crate) fn is_forced_close() -> bool {
        FORCE_CLOSE.load(Ordering::SeqCst)
    }
}

pub trait ThreadPoolStates {
    fn set_exec_timeout(&mut self, timeout: Option<Duration>);
    fn get_exec_timeout(&self) -> Option<Duration>;
    fn toggle_auto_scale(&mut self, auto_scale: bool);
    fn auto_scale_enabled(&self) -> bool;
}

impl ThreadPoolStates for ThreadPool {
    fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    fn get_exec_timeout(&self) -> Option<Duration> {
        self.queue_timeout
    }

    fn toggle_auto_scale(&mut self, auto_scale: bool) {
        self.auto_scale = auto_scale;
    }

    fn auto_scale_enabled(&self) -> bool {
        self.auto_scale
    }
}

pub trait PoolManager {
    fn extend(&mut self, more: usize);
    fn shrink(&mut self, less: usize);
    fn resize(&mut self, total: usize);
    fn auto_adjust(&mut self);
    fn auto_expire(&mut self, life: Option<Duration>);
    fn kill_worker(&mut self, id: usize);
    fn clear(&mut self);
    fn close(&mut self);
    fn force_close(&mut self);
}

impl PoolManager for ThreadPool {
    fn extend(&mut self, more: usize) {
        if more == 0 {
            return;
        }

        // manager will update the graveyard
        self.manager.extend_by(more);
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        // manager will update the graveyard
        self.manager.shrink_by(less);
    }

    fn resize(&mut self, total: usize) {
        if total == 0 {
            return;
        }

        let worker_count = self.manager.workers_count();
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
    fn auto_expire(&mut self, life: Option<Duration>) {
        let actual_life = if let Some(l) = life {
            l.as_millis() as usize
        } else {
            0usize
        };

        self.manager.worker_auto_expire(actual_life);
    }

    fn kill_worker(&mut self, id: usize) {
        if self.manager.dismiss_worker(id).is_none() {
            // can't find the worker with the given id, quit now.
            return;
        }

        if self.chan.0.send(Message::Terminate(id)).is_err() && is_debug_mode() {
            eprintln!("Failed to send the termination message to worker: {}", id);
        }

        if is_debug_mode() {
            println!("Worker {} is told to be terminated...", id);
        }
    }

    fn clear(&mut self) {
        let mut sent = false;
        if let Ok(()) = self.chan.0.send(Message::Terminate(0)) {
            sent = true;
        }

        if !sent && is_debug_mode(){
            // abort the clear process if we can't send the terminate message
            eprintln!("Failed to send the terminate message, please try again...");
        }

        self.manager.remove_all(sent);
    }

    fn close(&mut self) {
        self.closing = true;
        self.clear();
    }

    fn force_close(&mut self) {
        FORCE_CLOSE.store(true, Ordering::SeqCst);
        self.close();
    }
}

pub trait PoolState {
    fn get_size(&self) -> usize;
    fn get_queue_length(&self) -> usize;
    fn get_priority_queue_length(&self) -> usize;
    fn get_queue_size_threshold(&self) -> usize;
    fn set_queue_size_threshold(&mut self, threshold: usize);
    fn get_first_worker_id(&self) -> Option<usize>;
    fn get_last_worker_id(&self) -> Option<usize>;
    fn get_next_worker_id(&self, id: usize) -> Option<usize>;
}

impl PoolState for ThreadPool {
    #[inline]
    fn get_size(&self) -> usize {
        self.manager.workers_count()
    }

    #[inline]
    fn get_queue_length(&self) -> usize {
        self.chan.0.len() + self.chan.1.len()
    }

    #[inline]
    fn get_priority_queue_length(&self) -> usize {
        self.chan.0.len()
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
        match self.manager.first_worker_id() {
            0 => None,
            id => Some(id),
        }
    }

    fn get_last_worker_id(&self) -> Option<usize> {
        match self.manager.last_worker_id() {
            0 => None,
            id => Some(id),
        }
    }

    fn get_next_worker_id(&self, current_id: usize) -> Option<usize> {
        match self.manager.next_worker_id(current_id) {
            0 => None,
            id => Some(id),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if is_debug_mode() {
            println!("Shutting down this individual pool, sending terminate message to all workers.");
        }

        self.clear();
    }
}