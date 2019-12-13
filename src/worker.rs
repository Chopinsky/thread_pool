#![allow(dead_code)]

//use std::future::Future;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::debug::is_debug_mode;
use crate::manager::{IdleThreshold, StatusBehaviorDefinitions, StatusBehaviors};
use crate::model::*;
use crate::scheduler::PoolStatus;
use crossbeam_channel as channel;

const TIMEOUT: Duration = Duration::from_micros(16);
const LONG_TIMEOUT: Duration = Duration::from_micros(96);
const LOT_COUNTS: usize = 3;
const LONG_PARKING_ROUNDS: u8 = 8;
const SHORT_PARKING_ROUNDS: u8 = 2;

/*
struct FutWorker {
    local_pool: LocalPool,
    tasks_queue: LocalSpawner,
    tasks_counter: usize,
}

impl FutWorker {
    fn new() -> FutWorker {
        let local_pool = LocalPool::new();
        let tasks_queue = local_pool.spawner();

        FutWorker {
            local_pool,
            tasks_queue,
            tasks_counter: 0,
        }
    }

    fn push<F: Future<Output = ()> + Send + 'static>(&mut self, job: F) -> Result<(), String> {
        self.tasks_queue
            .spawn_local(job)
            .and_then(|_| {
                self.tasks_counter += 1;
                Ok(())
            })
            .map_err(|_| String::from("The futures pool has been shutdown ... "))
    }

    fn run(&mut self) -> bool {
        while self.tasks_counter > 0 && self.local_pool.try_run_one() {
            self.tasks_counter -= 1;
        }

        self.tasks_counter == 0
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tasks_counter == 0
    }
}
*/

thread_local!();

pub(crate) struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
    stat: Weak<AtomicUsize>,
    before_drop: Option<WorkerUpdate>,
    after_drop: Option<WorkerUpdate>,
}

struct WorkStatus(i8, Option<Job>);

impl Worker {
    /// Create and spawn the worker, this will dispatch the worker to listen to work queue immediately
    pub(crate) fn new(
        name: Option<String>,
        my_id: usize,
        stack_size: usize,
        privileged: bool,
        rx_pair: (channel::Receiver<Message>, channel::Receiver<Message>),
        shared_info: (PoolStatus, IdleThreshold), // (idle_threshold, pool_status)
        behavior_definition: &StatusBehaviors,
    ) -> Worker {
        behavior_definition.before_start(my_id);

        let (worker, stat) =
            Self::spawn_worker(name, my_id, stack_size, privileged, rx_pair, shared_info);

        behavior_definition.after_start(my_id);

        Worker {
            id: my_id,
            thread: Some(worker),
            stat,
            before_drop: behavior_definition.before_drop_clone(),
            after_drop: behavior_definition.after_drop_clone(),
        }
    }

    /// Get the worker id
    pub(crate) fn get_id(&self) -> usize {
        self.id
    }

    /// Calling `retire` on a worker will block the thread until the worker has done its work, or wake
    /// up from hibernation. This could block the caller for an undetermined amount of time.
    pub(crate) fn retire(&mut self) {
        if let Some(handle) = self.thread.take() {
            if let Some(stat) = self.stat.upgrade() {
                stat.store(1, Ordering::SeqCst);
            }

            // make sure we can wake up and quit
            handle.thread().unpark();

            // make sure the work is done
            handle.join().unwrap_or_else(|err| {
                eprintln!("Unable to drop worker: {}, error: {:?}", self.id, err);
            });
        }
    }

    /// If the worker has been put to sleep (i.e. in `park` mode), wake it up. This API will not check
    /// if the worker is actually hibernating or not.
    pub(crate) fn wake_up(&self) {
        if let Some(handle) = self.thread.as_ref() {
            handle.thread().unpark();
        }
    }

    /// Check if the worker has quit its inner loop and ready to be joined
    pub(crate) fn is_terminated(&self) -> bool {
        if let Some(stat) = self.stat.upgrade() {
            return stat.load(Ordering::Acquire) == 2usize;
        }

        false
    }

    fn spawn_worker(
        name: Option<String>,
        my_id: usize,
        stack_size: usize,
        privileged: bool,
        rx_pair: (channel::Receiver<Message>, channel::Receiver<Message>),
        shared_info: (PoolStatus, IdleThreshold),
    ) -> (thread::JoinHandle<()>, Weak<AtomicUsize>) {
        let mut builder = thread::Builder::new();

        if name.is_some() {
            builder = builder.name(name.unwrap_or_else(|| format!("worker-{}", my_id)));
        }

        if stack_size > 0 {
            builder = builder.stack_size(stack_size);
        }

        let worker_stat = Arc::new(AtomicUsize::new(0));
        let stat_clone = Arc::downgrade(&worker_stat);

        let handle = builder
            .spawn(move || {
                let mut idle_stat: Option<u8>;
                let mut status: u8;
                let mut pri_work_count: u8 = 0;

                let mut since = if privileged {
                    None
                } else {
                    Some(SystemTime::now())
                };

                // unpack the shared info triple
                let (pool_status, idle_threshold) = shared_info;
                let (pri_wait, norm_wait) = match my_id % LOT_COUNTS {
                    0 => (true, false),
                    1 => (false, true),
                    _ => (false, false),
                };

                // main worker loop
                loop {
                    // get ready to take new work from the channel
                    if worker_stat.load(Ordering::SeqCst) == 1usize {
                        return;
                    }

                    // get the pool status code
                    status = pool_status.load();
                    if status == FLAG_FORCE_CLOSE
                        || ((status == FLAG_CLOSING || status == FLAG_REST)
                            && rx_pair.0.is_empty()
                            && rx_pair.1.is_empty())
                    {
                        // if shutting down, check if we can abandon all work by checking forced
                        // close flag, or when all work have been processed.
                        worker_stat.store(1, Ordering::SeqCst);
                        return;
                    }

                    // wait for work loop
                    let work = match Worker::check_queues(
                        &rx_pair.0,
                        &rx_pair.1,
                        pri_wait,
                        norm_wait,
                        &mut pri_work_count,
                    ) {
                        // if the channels are disconnected, return
                        WorkStatus(-1, _) => {
                            worker_stat.store(1, Ordering::SeqCst);
                            return;
                        }
                        WorkStatus(_, job) => job,
                    };

                    // if there's a job, get it done first, and calc the idle period since last actual job
                    idle_stat =
                        // if we have work, do them now
                        Worker::handle_work(
                            work,
                            &mut since
                        )
                        .or_else(|| {
                            // if we don't have the work, calculate the idle period
                            Worker::calc_idle(&since)
                        })
                        .and_then(|idle| {
                            // if idled longer than the expected worker life for unprivileged workers,
                            // then we're done now -- self-purging.
                            let stat_code = idle_threshold.idle_stat(idle.as_secs() as u64);

                            if stat_code > 0 {
                                // mark self as a voluntary retiree
                                worker_stat.store(stat_code as usize, Ordering::SeqCst);
                                return Some(stat_code);
                            }

                            None
                        });

                    /*
                    // if not done and it's a target kill, handle it now
                    done = done
                        || target
                            .and_then(|id_slice| {
                                if id_slice.is_empty() {
                                    return None;
                                }

                                // write and done, keep the write lock scoped and update the graveyard
                                let mut found = false;
                                let mut g = graveyard.write();

                                for id in id_slice {
                                    // update graveyard for clean up purposes
                                    g.insert(id);

                                    // only receiving the universal kill-signal from closing the channel
                                    found = found || id == my_id;
                                }

                                // if my id or a forced kill, just quit
                                if found {
                                    return Some(());
                                }

                                None
                            })
                            .is_some();
                    */

                    match idle_stat {
                        Some(1) => {
                            pool_status.toggle_flag(FLAG_SLEEP_WORKERS, true);
                            thread::park();
                        },
                        Some(2) => return,
                        _ => {}
                    }

                    if status == FLAG_HIBERNATING {
                        thread::park();
                    }
                }
            })
            .unwrap();

        (handle, stat_clone)
    }

    fn check_queues(
        pri_chan: &channel::Receiver<Message>,
        norm_chan: &channel::Receiver<Message>,
        pri_wait: bool,
        norm_wait: bool,
        pri_work_count: &mut u8,
    ) -> WorkStatus {
        // wait for work loop, 1/3 of workers will long-park for priority work, and 1/3 of workers
        // will long-park for normal work, the remainder 1/3 workers will be fluid and constantly
        // query both queues -- whichever yield a task, then it will execute that task.
        if *pri_work_count < 255 {
            // 1/3 of the workers is designated to wait longer for prioritised jobs
            let norm_full = norm_chan.is_full();

            match Worker::fetch_work(pri_chan, norm_full && !pri_wait) {
                Ok(message) => {
                    // message is the only place that can update the "done" field
                    let (job, _) = Worker::unpack_message(message);

                    if *pri_work_count < 4 {
                        // only add if we're below the continuous pri-work cap
                        *pri_work_count += 1;
                    } else if norm_full {
                        // if we've done 4 or more priority work in a row, check if
                        // we should skip if the normal channel is full and maybe
                        // blocking, by setting the special number
                        *pri_work_count = 255;
                    }

                    return WorkStatus(0, job);
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    // sender has been dropped
                    return WorkStatus(-1, None);
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    // if chan empty, do nothing and fall through to the normal chan handle
                    // fall-through
                }
            };
        } else {
            // if the worker has performed 4 consecutive prioritized work and the normal
            // channel is full, we skip the priority work once to pick up a normal work
            // such that it won't be blocked forever; meanwhile, reset the counter.
            *pri_work_count = 0;
        }

        // 1/3 of the workers is designated to wait longer for normal jobs
        match Worker::fetch_work(norm_chan, pri_chan.is_full() && !norm_wait) {
            Ok(message) => {
                // message is the only place that can update the "done" field
                let (job, _) = Worker::unpack_message(message);
                *pri_work_count = 0;

                return WorkStatus(0, job);
            }
            Err(channel::RecvTimeoutError::Disconnected) => {
                // sender has been dropped
                return WorkStatus(-1, None);
            }
            Err(channel::RecvTimeoutError::Timeout) => {
                // nothing to receive yet
            }
        };

        WorkStatus(0, None)
    }

    fn fetch_work(
        main_chan: &channel::Receiver<Message>,
        can_skip: bool,
    ) -> Result<Message, channel::RecvTimeoutError> {
        let mut wait = 0;
        let rounds = if can_skip {
            SHORT_PARKING_ROUNDS
        } else {
            LONG_PARKING_ROUNDS
        };

        loop {
            wait += 1;

            match main_chan.try_recv() {
                Ok(work) => return Ok(work),
                Err(channel::TryRecvError::Disconnected) => {
                    return Err(channel::RecvTimeoutError::Disconnected)
                }
                Err(channel::TryRecvError::Empty) => {
                    if can_skip {
                        // if there're normal work in queue, break to fetch the normal work
                        return Err(channel::RecvTimeoutError::Timeout);
                    }
                }
            }

            if wait > rounds {
                return Err(channel::RecvTimeoutError::Timeout);
            }
        }
    }

    fn handle_work(work: Option<Job>, since: &mut Option<SystemTime>) -> Option<Duration> {
        if let Some(w) = work {
            w.call_box();
        }

        let mut idle = None;
        if since.is_some() {
            idle = Worker::calc_idle(&since);
            since.replace(SystemTime::now());
        }

        idle
    }

    fn unpack_message(message: Message) -> (Option<Job>, Option<Vec<usize>>) {
        match message {
            Message::SingleJob(job) => (Some(job), None),
            Message::ChainedJobs(_) => unreachable!(),
            Message::Terminate(target) => (None, Some(target)),
        }
    }

    fn calc_idle(since: &Option<SystemTime>) -> Option<Duration> {
        if let Some(s) = since {
            if let Ok(e) = s.elapsed() {
                return Some(e);
            }
        }

        None
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(behavior) = self.before_drop {
            behavior(self.id);
        }

        if is_debug_mode() {
            println!("Dropping worker {}", self.id);
        }

        self.retire();

        if let Some(behavior) = self.after_drop {
            behavior(self.id);
        }
    }
}
