use std::future::Future;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;
use std::vec;

use crate::config::{Config, ConfigStatus, TimeoutPolicy};
use crate::debug::is_debug_mode;
use crate::manager::*;
use crate::model::*;
use channel::{SendError, SendTimeoutError, Sender, TryRecvError, TrySendError};
use crossbeam_channel as channel;

const RETRY_LIMIT: u8 = 4;
const CHAN_CAP: usize = 16;
const THRESHOLD: usize = 1024;
const AUTO_EXTEND_TRIGGER_SIZE: usize = 2;

/// Enumeration to indicate possible reasons a job execution request is rejected. User will need to
/// resubmit the job again, since closure's state may have been stale at the execution error.
pub enum ExecutionError {
    /// The job can't be executed because the queue is full when the new job is submitted and no new
    /// worker becomes available before predetermined timeout period.
    Timeout,

    /// The pool hasn't been initialized (i.e. lazy created), or all workers have been terminated, such
    /// that there is no working threads to execute the job
    Uninitialized,

    /// The pool is shutting down, or the internal pipeline is broken for whatever reasons.
    Disconnected,

    /// Pool's internal states have been corrupted
    PoolPoisoned,
}

/// The standalone thread pool, which gives users more controls on the pool and where the hosted pool
/// shall live.
///
/// # Examples
///
/// ```
/// extern crate threads_pool;
///
/// use threads_pool as pool;
/// use std::thread;
/// use std::time::Duration;
///
/// // lazy-create the pool
/// let mut pool = pool::ThreadPool::build(4);
///
/// // get ready to submit parallel jobs, first of all, activate the pool. Not that this step is
/// // optional if the caller will only use the `exec` API to submit jobs.
/// pool.execute(|| {
///     // this closure could cause panic if the pool has not been activated yet.
///     println!("Executing jobs ... ");
/// });
///
/// // do parallel stuff.
/// for id in 0..10 {
///     // even `id` jobs always get prioritized
///     let priority = if id % 2 == 0 { true } else { false };
///
///     // API `exec` can set if a job needs to be prioritized for execution or not.
///     pool.exec(|| {
///         thread::sleep(Duration::from_secs(1));
///         println!("thread {} has slept for around 1 second ... ", id);
///     }, priority);
/// }
/// ```
pub struct ThreadPool {
    /// The worker manager struct, hide details of managing workers.
    manager: Manager,

    /// The job queue deliverer. chan.0 is the priority channel, chan.1 is the normal channel.
    chan: (Sender<Message>, Sender<Message>),

    /// Initial pool size. This will be used to create worker threads when the pool is lazy created,
    /// among a few other things
    init_size: usize,

    /// The threshold of which a job can be automatically upgraded to a prioritized job, if the
    /// prioritized job queue has less jobs in queue than this threshold. This is mainly used to
    /// boost the through out ratio if only a small potion of jobs have priorities, such that we
    /// can make more workers busy though their life-time.
    upgrade_threshold: usize,

    /// This is the threshold for the number of the threads that can be created for this pool. It's
    /// default to 1024, but the threshold can be adjusted through the `set_queue_size_threshold` API.
    auto_extend_threshold: usize,

    /// Determine the pool status: 1) Closing -- this will serve as the gate keeper for rejecting any
    /// jobs _after_ a pool shutting down is scheduled; 2) Hibernating -- this will serve as a gate
    /// keeper from accepting any new jobs if the pool is hibernating.
    status: PoolStatus,

    /// If we allow the pool to scale itself without explicitly API calls. If this option is turned
    /// on, we will automatically add more workers to the reservoir when the pool is under pressure
    /// (i.e. more submitted jobs in queue than workers can handle), These temp workers will stick
    /// around for a while, and eventually retire after being idle for a period of time.
    auto_scale: bool,

    /// Determine if the job handover shall return immediately, i.e. in `non-blocking` mode.
    non_blocking: bool,

    /// Set the timeout period before we retry or give up executing the submitted job. If leaving this
    /// field for `None`, the job submission will block the caller until a thread worker is freed up
    /// in blocking mode, or return with error immediately if in the non-blocking mode (i.e.
    /// `non_blocking` field set to `true`).
    queue_timeout: Option<Duration>,

    /// Determine the timeout policy when the queue is full and we have timed out on sending the job:
    ///     1. Drop       -> (default behavior) The job will be dropped.
    ///     2. DirectRun  -> The job will be run in the current thread and will keep blocking the
    ///                      caller until the job is done.
    ///     3. LossyRetry -> If we choose to drop oldest tasks when the pool is full, such that we
    ///                      will not block the channels;
    timeout_policy: TimeoutPolicy,
}

impl ThreadPool {
    /// Create a `ThreadPool` with default configurations
    pub fn new(size: usize) -> ThreadPool {
        Self::create_pool(size, Config::default(), false)
    }

    /// Create a `ThreadPool` with supplied configurations
    pub fn new_with_config(size: usize, config: Config) -> ThreadPool {
        Self::create_pool(size, config, false)
    }

    /// Create the `ThreadPool` with default pool configuration settings. When ready to activate the
    /// pool, invoke the `activate_pool` API before submitting jobs for execution. You can also
    /// call `exec` API to automatically activate the pool, however, calling the alternative immutable
    /// API `execute` will always lead to an error.
    pub fn build(size: usize) -> ThreadPool {
        Self::create_pool(size, Config::default(), true)
    }

    /// Create the `ThreadPool` with provided pool configuration settings. When ready to activate the
    /// pool, invoke the `activate_pool` API before submitting jobs for execution. You can also
    /// call `exec` API to automatically activate the pool, however, calling the alternative immutable
    /// API `execute` will always lead to an error.
    pub fn build_with_config(size: usize, config: Config) -> ThreadPool {
        Self::create_pool(size, config, true)
    }

    /// If the pool is lazy created, user is responsible for activating the pool before submitting jobs
    /// for execution. API `exec` will initialized the pool since it takes the mutable `self`, and hence
    /// is able to initialize the pool. However, fail to explicitly initialize the pool could cause
    /// unidentified behaviors.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate threads_pool;
    /// use std::thread;
    /// use std::time::Duration;
    /// use threads_pool as pool;
    ///
    /// // lazy-create the pool
    /// let mut pool = pool::ThreadPool::build(4);
    ///
    /// // work on other stuff ...
    /// thread::sleep(Duration::from_secs(4));
    ///
    /// // get ready to submit parallel jobs, first of all, activate the pool. Not that this step is
    /// // optional if the caller will only use the `exec` API to submit jobs.
    /// pool.activate()
    ///     .execute(|| {
    ///         // this closure could cause panic if the pool has not been activated yet.
    ///         println!("Lazy created pool now accepting new jobs...");
    ///     });
    ///
    /// // do parallel stuff.
    /// for id in 0..10 {
    ///     // API `exec` can also activate the pool automatically if it's lazy-created.
    ///     pool.exec(|| {
    ///         thread::sleep(Duration::from_secs(1));
    ///         println!("thread {} has been waken after 1 seconds ... ", id);
    ///     }, true);
    /// }
    /// ```
    pub fn activate(&mut self) -> &mut Self {
        debug_assert!(
            self.init_size > 0,
            "The initial pool size must be equal or larger than 1 ..."
        );

        let status = self.status.load();

        // No need or can't activate ...
        if status != FLAG_HIBERNATING || status != FLAG_LAZY_INIT {
            return self;
        }

        // Wake up everybody
        if status == FLAG_HIBERNATING {
            // call to unhibernate, the `unhibernate` API will handle the status updates
            self.unhibernate();
            return self;
        }

        // Only case to handle: the pool is lazy created, than we need to add workers now.
        let workers_count = self.manager.workers_count();
        if workers_count < self.init_size {
            // lazy init the pool at the first job, or regenerate workers when all are purged
            self.manager
                .add_workers(self.init_size - workers_count, true, self.status.clone());
        }

        // update the status after activation.
        self.set_status(FLAG_NORMAL);
        self
    }

    /// Set the time out period for a job to be queued. If `timeout` is defined as some duration,
    /// we will keep the new jobs in the queue for at least them amount of time when all workers of
    /// the pool are busy. If it's set to `None` and all workers are busy at the moment a job comes,
    /// we will either block the caller when pool's non_blocking setting is turned on, or return timeout
    /// error immediately when the setting is turned off.
    pub fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    /// Toggle if we should block the execution APIs when all workers are busy and we can't expand
    /// the thread pool anymore for any reasons.
    pub fn toggle_blocking(&mut self, non_blocking: bool) {
        self.non_blocking = non_blocking;
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
    /// either `build` or `build_with_config` APIs, then the pool will be initialized at
    /// the first time a job is to be executed.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate threads_pool;
    /// use threads_pool as pool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut pool = pool::ThreadPool::build(4);
    ///
    /// for id in 0..10 {
    ///     pool.exec(|| {
    ///         thread::sleep(Duration::from_secs(1));
    ///         println!("thread {} has been waken after 1 seconds ... ", id);
    ///     }, true);
    /// }
    /// ```
    pub fn exec<F: FnOnce() + Send + 'static>(
        &mut self,
        f: F,
        prioritized: bool,
    ) -> Result<(), ExecutionError> {
        let status = self.status.load();

        // we're closing the pool, take no more new jobs
        if status == FLAG_CLOSING || status == FLAG_FORCE_CLOSE {
            return Err(ExecutionError::Disconnected);
        }

        // if at the hibernation or lazy init mode, activate the pool first
        if status == FLAG_HIBERNATING || status == FLAG_LAZY_INIT {
            self.activate();
        }

        // still no worker to take the job? unexpected and should return error
        let worker_count = self.manager.workers_count();
        if worker_count == 0 {
            return Err(ExecutionError::Uninitialized);
        }

        // if we can auto scale the pool, set the retry stack limit
        let retry = if self.auto_scale { 1 } else { 0 };

        // send the job for execution
        self.dispatch(Message::SingleJob(Box::new(f)), retry, prioritized)
            .map(|busy| {
                if busy && self.auto_scale {
                    // auto scale by adding more workers to take the job
                    if let Some(target) = self.amortized_new_size(self.init_size) {
                        self.manager
                            .add_workers(target - worker_count, false, self.status.clone());
                    }
                }
            })
            .map_err(|err| match err {
                SendTimeoutError::Timeout(_) => ExecutionError::Timeout,
                SendTimeoutError::Disconnected(_) => ExecutionError::Disconnected,
            })
    }

    /// Similar to `exec`, yet this is the simplified version taking an immutable version of the pool.
    /// The job priority will be automatically evaluated based on the queue length, and for pool
    /// under pressure, we will try to balance the queue.
    ///
    /// This method will be no-op if the pool is lazy-created (i.e. created via the `build` functions)
    /// and `activate` has not been called on it yet. If you are not sure if the pool has been
    /// activated or not, alter to use `exec` instead, which will initiate the pool if that's not
    /// done yet.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate threads_pool;
    /// use threads_pool as pool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let pool = pool::ThreadPool::build(4);
    ///
    /// for id in 0..10 {
    ///     pool.execute(|| {
    ///         thread::sleep(Duration::from_secs(1));
    ///         println!("thread {} has been waken after 1 seconds ... ", id);
    ///     });
    /// }
    /// ```
    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<(), ExecutionError> {
        // we're closing, taking no more jobs.
        if self.status.closing() {
            return Err(ExecutionError::Disconnected);
        }

        // no worker to take the job
        if self.manager.workers_count() < 1 {
            return Err(ExecutionError::Uninitialized);
        }

        // send the job to the queue for execution. note that if we're in hibernation, the queue
        // will still take the new job, though no worker will be awaken to take the job.
        let prioritized = self.chan.1.is_empty() && !self.chan.0.is_full();

        self.dispatch(Message::SingleJob(Box::new(f)), 0, prioritized)
            .map(|_| {})
            .map_err(|err| match err {
                SendTimeoutError::Timeout(_) => ExecutionError::Timeout,
                SendTimeoutError::Disconnected(_) => ExecutionError::Disconnected,
            })
    }

    pub fn sync_block<R, F>(&self, f: F) -> Result<R, ExecutionError>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let curr = thread::current();
        let (tx, rx) = channel::bounded(1);

        let clo = Box::new(move || {
            tx.send(f()).unwrap_or_default();
            curr.unpark();
        });

        if self.dispatch(Message::SingleJob(clo), 4, false).is_err() {
            return Err(ExecutionError::Timeout);
        }

        // timeout after 8 seconds of no responses ...
        thread::park_timeout(Duration::from_secs(8));

        rx.try_recv().map_err(|err| match err {
            TryRecvError::Empty => ExecutionError::Timeout,
            TryRecvError::Disconnected => ExecutionError::Disconnected,
        })
    }

    fn dispatch(
        &self,
        message: Message,
        retry: u8,
        with_priority: bool,
    ) -> Result<bool, SendTimeoutError<Message>> {
        // pick the work queue where we shall put this new job into
        let (chan, chan_id) = if with_priority
            || (self.chan.1.is_empty() && self.chan.0.len() <= self.upgrade_threshold)
        {
            // squeeze the work into the priority chan first even if some normal work is in queue
            (&self.chan.0, 0)
        } else {
            // normal work and then priority queue is full
            (&self.chan.1, 1)
        };

        let res = match self.queue_timeout {
            Some(period) => {
                // spin and retry to send the message on timeout
                self.send_timeout((chan, chan_id), message, period, retry)
            }
            None => {
                if !self.non_blocking {
                    // wait until a worker is ready to take new work
                    self.send(chan, message)
                } else {
                    // try send and return (almost) immediately if failed or succeeded
                    self.try_send((chan, chan_id), message)
                }
            }
        };

        res.map(|_| chan.is_full())
    }

    fn amortized_new_size(&self, queue_length: usize) -> Option<usize> {
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

    fn set_status(&mut self, status: u8) {
        self.status.store(status);
    }

    fn update_status(&mut self, old: u8, new: u8) -> bool {
        if old == new {
            return true;
        }

        self.status.compare_exchange(old, new)
    }

    fn shut_down(&mut self, forced: bool) {
        if !forced {
            self.set_status(FLAG_CLOSING);
        } else {
            self.set_status(FLAG_FORCE_CLOSE);
        }

        if is_debug_mode() {
            println!(
                "Remainder work before shutdown signal: {}",
                self.chan.0.len() + self.chan.1.len()
            );
        }

        self.clear();
    }

    fn create_pool(size: usize, config: Config, lazy_built: bool) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (tx, rx) = channel::bounded(CHAN_CAP);
        let (pri_tx, pri_rx) = channel::bounded(CHAN_CAP);

        let non_blocking = config.non_blocking();
        let policy = config.timeout_policy();

        let flag = PoolStatus::new(if !lazy_built {
            FLAG_NORMAL
        } else {
            FLAG_LAZY_INIT
        });

        let manager = Manager::build(config, pool_size, flag.clone(), pri_rx, rx, lazy_built);

        //        let pool = LocalPool::new();
        //        let spawner = pool.spawner();

        ThreadPool {
            manager,
            chan: (pri_tx, tx),
            init_size: pool_size,
            upgrade_threshold: CHAN_CAP / 2,
            auto_extend_threshold: THRESHOLD,
            status: flag,
            auto_scale: false,
            non_blocking,
            queue_timeout: None,
            timeout_policy: policy,
            //            local_pool: Some((pool, spawner)),
        }
    }
}

pub trait Hibernation {
    fn hibernate(&mut self);
    fn unhibernate(&mut self);
    fn is_hibernating(&self) -> bool;
}

impl Hibernation for ThreadPool {
    /// Put the pool into hibernation mode. In this mode, all workers will park itself after finishing
    /// the current job to reduce CPU usage.
    ///
    /// The pool will be prompted back to normal mode on 2 occasions:
    /// 1) calling the `unhibernate` API to wake up the pool, or 2) sending a new job through the
    /// `exec` API, which will automatically assume an unhibernation desire, wake self up, take and
    /// execute the incoming job. Though if you call the immutable API `execute`, the job will be
    /// queued yet not executed. Be aware that if the queue is full, the new job will be dropped and
    /// an execution error will be returned in this case.
    ///
    /// It is recommended to explicitly call `unhibernate` when the caller want to wake up the pool,
    /// to avoid side effect or undefined behaviors.
    fn hibernate(&mut self) {
        self.set_status(FLAG_HIBERNATING);
    }

    /// This will unhibernate the pool if it's currently in the hibernation mode. It will do nothing
    /// if the pool is in any other operating mode, e.g. the working mode or shutting down mode.
    ///
    /// Cautious: calling this API will set the status flag to normal, which may conflict with actions
    /// that would set status flag otherwise.
    fn unhibernate(&mut self) {
        // only wake everyone up if we're in the right status
        if self.update_status(FLAG_HIBERNATING, FLAG_NORMAL) {
            // take the chance to clean up the graveyard
            self.manager.worker_cleanup();
            self.wake_workers();
        }
    }

    /// Check if the pool is in hibernation mode.
    fn is_hibernating(&self) -> bool {
        self.status.load() == FLAG_HIBERNATING
    }
}

pub trait ThreadPoolStates {
    fn set_exec_timeout(&mut self, timeout: Option<Duration>);
    fn get_exec_timeout(&self) -> Option<Duration>;
    fn toggle_auto_scale(&mut self, auto_scale: bool);
    fn auto_scale_enabled(&self) -> bool;
}

impl ThreadPoolStates for ThreadPool {
    /// Set the job timeout period.
    ///
    /// The timeout period is mainly for dropping jobs when the thread pool is under
    /// pressure, i.e. the producer creates new work faster than the consumer can handle them. When
    /// the job queue buffer is full, any additional jobs will be dropped after the timeout period.
    /// Set the `timeout` parameter to `None` to turn this feature off, which is the default behavior.
    /// Note that if the timeout is turned off, sending new jobs to the full pool will block the
    /// caller until some space is freed up in the work queue.
    fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    /// Check the currently set timeout period. If the result is `None`, it means we will not timeout
    /// on submitted jobs when the job queue is full, which implies the caller will be blocked until
    /// some space in the queue is freed up
    fn get_exec_timeout(&self) -> Option<Duration> {
        self.queue_timeout
    }

    /// Toggle if we shall scale the pool automatically when the pool is under pressure, i.e. adding
    /// more threads to the pool to take the jobs. These temporarily added threads will go away once
    /// the pool is able to keep up with the new jobs to release resources.
    fn toggle_auto_scale(&mut self, auto_scale: bool) {
        self.auto_scale = auto_scale;
    }

    /// Check if the auto-scale feature is turned on or not
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
    /// Manually extend the size of the pool. If another operation that's already adding more threads
    /// to the pool, e.g. the pool is under pressure and trigger a pool extension automatically, then
    /// this operation will be cancelled.
    fn extend(&mut self, more: usize) {
        if more == 0 {
            return;
        }

        // manager will update the graveyard
        self.manager.extend_by(more, self.status.clone());
    }

    /// Manually shrink the size of the pool and release system resources. If another operation that's
    /// reducing the size of the pool is undergoing, this shrink-op will be cancelled.
    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        // manager will update the graveyard
        let workers = self.manager.shrink_by(less);
        if self.chan.0.send(Message::Terminate(workers)).is_err() && is_debug_mode() {
            eprintln!("Failed to send the termination message to workers");
        }
    }

    /// Resize the pool to the desired size. This will either trigger a pool extension or contraction.
    /// Note that if another pool-size changing operation is undergoing, the effect may be cancelled
    /// out if we're moving towards the same direction (adding pool size, or reducing pool size).
    fn resize(&mut self, target: usize) {
        if target == 0 {
            return;
        }

        let worker_count = self.manager.workers_count();
        if target > worker_count {
            self.extend(target - worker_count);
        } else if target < worker_count {
            self.shrink(worker_count - target);
        }
    }

    /// Automatically adjust the pool size according to criteria: if the pool is idling and we've
    /// previously added temporary workers, we will tell them to cease work before designated expiration
    /// time; if the pool is overwhelmed and need more workers to handle jobs, we will add more threads
    /// to the pool.
    fn auto_adjust(&mut self) {
        if let Some(target) = self.amortized_new_size(self.get_queue_length()) {
            self.resize(target);
        }
    }

    /// Let extended workers to expire when idling for too long.
    fn auto_expire(&mut self, life: Option<Duration>) {
        let actual_life = if let Some(l) = life {
            l.as_millis() as usize
        } else {
            0usize
        };

        self.manager.worker_auto_expire(actual_life);
    }

    /// Remove a thread worker from the pool with the given worker id.
    fn kill_worker(&mut self, id: usize) {
        if self.manager.dismiss_worker(id).is_none() {
            // can't find the worker with the given id, quit now.
            return;
        }

        if self
            .chan
            .0
            .send(Message::Terminate(vec::from_elem(id, 1)))
            .is_err()
            && is_debug_mode()
        {
            eprintln!("Failed to send the termination message to worker: {}", id);
        }

        if is_debug_mode() {
            println!("Worker {} is told to be terminated...", id);
        }
    }

    /// Clear the pool. Note this will not kill all workers immediately, and the API will block until
    /// all workers have finished their current job. Note that this also means we may leave queued jobs
    /// in place until new threads are added into the pool, otherwise, the jobs will not be executed
    /// and go away on program exit.
    fn clear(&mut self) {
        let status = self.status.load();
        let reset = if status != FLAG_FORCE_CLOSE || status != FLAG_CLOSING {
            // must update the flag if we've not in proper status
            self.set_status(FLAG_REST);
            true
        } else {
            // we're in closing status, no need to reset the flag
            false
        };

        // remove the workers in sync mode
        self.manager.remove_all(true);

        // reset the flag if required
        if reset {
            self.set_status(status);
        }
    }

    /// Signal the threads in the pool that we're closing, but allow them to finish all jobs in the queue
    /// before exiting.
    fn close(&mut self) {
        self.shut_down(false);
    }

    /// Signal the threads that they must quit now, and all queued jobs in the queue will be de-factor
    /// discarded since we're closing the pool.
    fn force_close(&mut self) {
        self.shut_down(true);
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

pub trait FuturesPool<T, F>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    fn block_on(&mut self, f: F) -> Result<T, ExecutionError>;
    fn spawn(&self, f: F) -> Result<(), ExecutionError>;
}

impl<T, F> FuturesPool<T, F> for ThreadPool
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    fn block_on(&mut self, f: F) -> Result<T, ExecutionError> {
        Err(ExecutionError::Uninitialized)
    }

    fn spawn(&self, f: F) -> Result<(), ExecutionError> {
        let future = async move {
            //            Task::get_current()
            f.await
        };

        Ok(())
    }
}

trait DispatchFlavors {
    fn send_timeout(
        &self,
        chan: (&Sender<Message>, u8),
        message: Message,
        timeout: Duration,
        retry: u8,
    ) -> Result<(), SendTimeoutError<Message>>;
    fn send(
        &self,
        chan: &Sender<Message>,
        message: Message,
    ) -> Result<(), SendTimeoutError<Message>>;
    fn try_send(
        &self,
        chan: (&Sender<Message>, u8),
        message: Message,
    ) -> Result<(), SendTimeoutError<Message>>;

    fn wake_workers(&self);
}

impl DispatchFlavors for ThreadPool {
    fn send_timeout(
        &self,
        chan: (&Sender<Message>, u8),
        message: Message,
        timeout: Duration,
        retry: u8,
    ) -> Result<(), SendTimeoutError<Message>> {
        let mut retry_message = message;
        let mut retry = retry;

        loop {
            match chan.0.send_timeout(retry_message, timeout) {
                Ok(()) => return Ok(()),
                Err(SendTimeoutError::Disconnected(msg)) => {
                    return Err(SendTimeoutError::Disconnected(msg))
                }
                Err(SendTimeoutError::Timeout(msg)) => {
                    // put the message back in pristine state
                    retry_message = msg;

                    // try bring any sleeping workers online now
                    self.wake_workers();

                    // if we use a lossy channel, always try to drop messages and try sending
                    // again, even if it means we need to clear the channel (i.e. too many
                    // retries...). Otherwise, check if we shall keep retrying.
                    match self.timeout_policy {
                        TimeoutPolicy::LossyRetry => {
                            // make space for new job submission(s). if a termination message
                            // is dropped, it should be fine since we need hands to get things
                            // done at the moment. Balancing or releasing resources can happen
                            // later.
                            self.manager.drop_many(chan.1, retry as usize);
                        }
                        TimeoutPolicy::DirectRun => {
                            // directly run the job; the termination message will not be
                            // sent in this workflow, so we shall not worry about that.
                            if let Message::SingleJob(job) = retry_message {
                                job.call_box();
                            }

                            // done with it
                            return Ok(());
                        }
                        TimeoutPolicy::Drop => {
                            // done with the retry (or not allowed), return and drop the job
                            if retry == 0 || retry > RETRY_LIMIT {
                                return Err(SendTimeoutError::Timeout(retry_message));
                            }
                        }
                    }

                    // if we shall try again, update the counter
                    retry += 1;
                }
            }
        }
    }

    fn send(
        &self,
        chan: &Sender<Message>,
        message: Message,
    ) -> Result<(), SendTimeoutError<Message>> {
        match chan.send(message) {
            Ok(()) => Ok(()),
            Err(SendError(msg)) => Err(SendTimeoutError::Disconnected(msg)),
        }
    }

    fn try_send(
        &self,
        chan: (&Sender<Message>, u8),
        message: Message,
    ) -> Result<(), SendTimeoutError<Message>> {
        // timeout immediately if all workers are busy
        match chan.0.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(msg)) => Err(SendTimeoutError::Disconnected(msg)),
            Err(TrySendError::Full(msg)) => {
                // bring any offline workers back online first
                self.wake_workers();

                if let TimeoutPolicy::LossyRetry = self.timeout_policy {
                    // drop a message and try again, just once
                    self.manager.drop_one(chan.1);

                    // send the message again and hopefully no one else take the space
                    chan.0.try_send(msg).map_err(|err| match err {
                        TrySendError::Disconnected(msg) => SendTimeoutError::Disconnected(msg),
                        TrySendError::Full(msg) => SendTimeoutError::Timeout(msg),
                    })
                } else {
                    // unable to send the job
                    Err(SendTimeoutError::Timeout(msg))
                }
            }
        }
    }

    fn wake_workers(&self) {
        if self.status.has_hibernate_workers() {
            // wake up workers now
            self.manager.wake_up();
            self.status.toggle_flag(FLAG_SLEEP_WORKERS, false);
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if is_debug_mode() {
            println!(
                "Shutting down this individual pool, sending terminate message to all workers."
            );
        }

        // close the pool in sync mode, that's to wait all workers to quit before unblocking
        if !self.status.closing() {
            self.close();
        }

        // now drop the manually allocated stuff
        unsafe {
            ptr::drop_in_place(self.status.0.as_ptr());
        }
    }
}

// A thinner version of the Arc wrapper over atomic, such that we can save a few atomic op on every
// new worker creation and status checks on the worker's side.
#[doc(hidden)]
pub(crate) struct PoolStatus(NonNull<AtomicU8>);

impl PoolStatus {
    fn new(val: u8) -> Self {
        let wrapper = Box::new(AtomicU8::new(val));
        PoolStatus(unsafe { NonNull::new_unchecked(Box::into_raw(wrapper)) })
    }

    fn closing(&self) -> bool {
        unsafe {
            // FLAG_CLOSING = 1, FLAG_FORCE_CLOSE == 2
            self.0
                .as_ref()
                .fetch_and(FLAG_CLOSING | FLAG_FORCE_CLOSE, Ordering::Acquire)
                > 0
        }
    }

    fn has_hibernate_workers(&self) -> bool {
        unsafe {
            // FLAG_HIBERNATING = 4, FLAG_SLEEP_WORKERS = 32
            self.0
                .as_ref()
                .fetch_and(FLAG_HIBERNATING | FLAG_SLEEP_WORKERS, Ordering::Acquire)
                > 0
        }
    }

    fn calc_new_stat(&self, old: u8, flag: u8, toggle_on: bool) -> Result<u8, ()> {
        if (toggle_on && (old & flag) > 0) || (!toggle_on && (old & flag) == 0) {
            // we already have the desired flag
            return Err(());
        }

        // get the new state
        Ok(if toggle_on { old | flag } else { old ^ flag })
    }

    fn compare_exchange(&self, old: u8, new: u8) -> bool {
        unsafe {
            self.0
                .as_ref()
                .compare_exchange_weak(old, new, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        }
    }

    fn store(&self, new: u8) {
        unsafe {
            self.0.as_ref().store(new, Ordering::SeqCst);
        }
    }

    #[inline]
    pub(crate) fn load(&self) -> u8 {
        unsafe { self.0.as_ref().load(Ordering::Acquire) }
    }

    pub(crate) fn toggle_flag(&self, flag: u8, toggle_on: bool) {
        assert!(flag % 2 == 0 || flag == 1, "forbidden to set multiple flags at the same time");

        unsafe {
            let mut old: u8 = self.0.as_ref().load(Ordering::Acquire);
            let mut new: u8;

            if let Ok(val) = self.calc_new_stat(old, flag, toggle_on) {
                new = val;
            } else {
                return;
            }

            while let Err(curr) =
            self.0
                .as_ref()
                .compare_exchange_weak(old, new, Ordering::SeqCst, Ordering::Relaxed)
                {
                    old = curr;

                    if let Ok(val) = self.calc_new_stat(old, flag, toggle_on) {
                        new = val;
                    } else {
                        return;
                    }
                }
        }
    }
}

impl Clone for PoolStatus {
    fn clone(&self) -> Self {
        PoolStatus(self.0)
    }
}

unsafe impl Send for PoolStatus {}
unsafe impl Sync for PoolStatus {}
