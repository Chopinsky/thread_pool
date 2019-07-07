#![allow(dead_code)]

use std::sync::{atomic::{AtomicUsize, AtomicU8, Ordering}, Arc};
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel as channel;
use parking_lot::RwLock;
use crate::debug::is_debug_mode;
use crate::manager::{StatusBehaviors, StatusBehaviorDefinitions};
use crate::model::*;
use hashbrown::HashSet;

const TIMEOUT: Duration = Duration::from_micros(16);
const LONG_TIMEOUT: Duration = Duration::from_micros(96);
const LOT_COUNTS: usize = 3;
const LONG_PARKING_ROUNDS: u8 = 16;
const SHORT_PARKING_ROUNDS: u8 = 4;

pub(crate) struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
    before_drop: Option<WorkerUpdate>,
    after_drop: Option<WorkerUpdate>,
}

struct WorkStatus(i8, Option<Job>, Option<Vec<usize>>);

impl Worker {
    /// Create and spawn the worker, this will dispatch the worker to listen to work queue immediately
    pub(crate) fn new(
        name: Option<String>,
        my_id: usize,
        stack_size: usize,
        privileged: bool,
        pri_rx: channel::Receiver<Message>,
        rx: channel::Receiver<Message>,
        graveyard: Arc<RwLock<HashSet<usize>>>,
        max_idle: Arc<AtomicUsize>,
        pool_status: Arc<AtomicU8>,
        behavior_definition: &StatusBehaviors,
    ) -> Worker
    {
        behavior_definition.before_start(my_id);

        let worker: thread::JoinHandle<()> = Self::spawn_worker(
            name, my_id, stack_size, privileged, pri_rx, rx, graveyard, max_idle, pool_status
        );

        behavior_definition.after_start(my_id);

        Worker {
            id: my_id,
            thread: Some(worker),
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

    fn spawn_worker(
        name: Option<String>,
        my_id: usize,
        stack_size: usize,
        privileged: bool,
        pri_rx: channel::Receiver<Message>,
        rx: channel::Receiver<Message>,
        graveyard: Arc<RwLock<HashSet<usize>>>,
        max_idle: Arc<AtomicUsize>,
        pool_status: Arc<AtomicU8>,
    ) -> thread::JoinHandle<()>
    {
        let mut builder = thread::Builder::new();

        if name.is_some() {
            builder = builder.name(
                name.unwrap_or_else(|| format!("worker-{}", my_id))
            );
        }

        if stack_size > 0 {
            builder = builder.stack_size(stack_size);
        }

        builder.spawn(move || {
            let mut done: bool;
            let mut status: u8;
            let mut pri_work_count: u8 = 0;

            let mut since = if privileged {
                None
            } else {
                Some(SystemTime::now())
            };

            // main worker loop
            loop {
                // get ready to take new work from the channel
                {
                    if graveyard.read().contains(&my_id) {
                        return;
                    }
                }

                // get the pool status code
                status = pool_status.load(Ordering::Acquire);
                if status == FLAG_FORCE_CLOSE
                    || (status == FLAG_CLOSING && pri_rx.is_empty() && rx.is_empty())
                {
                    // if shutting down, check if we can abandon all work by checking forced
                    // close flag, or when all work have been processed.
                    return;
                }

                // wait for work loop
                let (work, target) =
                    match Worker::check_queues(
                        my_id, &pri_rx, &rx, &mut pri_work_count
                    )
                    {
                        // if the channels are disconnected, return
                        WorkStatus(-1, _, _) => return,
                        WorkStatus(_, job, target) => {
                            (job, target)
                        },
                    };

                // if there's a job, get it done first, and calc the idle period since last actual job
                done = work
                    .and_then(|job| {
                        // if we have work, do them now
                        Worker::handle_work(job, &mut since)
                    })
                    .or_else(|| {
                        // if we don't have the work, calculate the idle period
                        Worker::calc_idle(&since)
                    })
                    .and_then(|idle| {
                        // if idled longer than the expected worker life for unprivileged workers,
                        // then we're done now -- self-purging.
                        let max = max_idle.load(Ordering::Relaxed) as u128;
                        if max.gt(&0) && max.le(&idle.as_millis()) {
                            // mark self as a voluntary retiree
                            graveyard.write().insert(my_id);
                            return Some(());
                        }

                        None
                    })
                    .is_some();

                // if it's a target kill, handle it now
                done = done || target
                    .and_then(|id_slice| {
                        // update the graveyard
                        let mut found = false;

                        if !id_slice.is_empty() {
                            // write and done, keep the write lock scoped
                            let mut g = graveyard.write();
                            for id in id_slice {
                                g.insert(id);
                                found = found || (id == 0 && status == FLAG_FORCE_CLOSE) || id == my_id;
                            }
                        }

                        // if my id or a forced kill, just quit
                        if found {
                            return Some(());
                        }

                        None
                    })
                    .is_some();

                if done {
                    return;
                }

                if status == FLAG_HIBERNATING {
                    thread::park();
                }
            }
        }).unwrap()
    }

    fn check_queues(
        id: usize,
        pri_chan: &channel::Receiver<Message>,
        norm_chan: &channel::Receiver<Message>,
        pri_work_count: &mut u8,
    ) -> WorkStatus
    {
        // wait for work loop, 1/3 of workers will long-park for priority work, and 1/3 of workers
        // will long-park for normal work, the remainder 1/3 workers will be fluid and constantly
        // query both queues -- whichever yield a task, then it will execute that task.
        if *pri_work_count < 255 {
            // 1/3 of the workers is designated to wait longer for prioritised jobs
            let parking = id % LOT_COUNTS == 0;
            match Worker::fetch_work(
                pri_chan, norm_chan.is_empty(), !parking
            ) {
                Ok(message) => {
                    // message is the only place that can update the "done" field
                    let (job, target) = Worker::unpack_message(message);

                    if *pri_work_count < 4 {
                        // only add if we're below the continuous pri-work cap
                        *pri_work_count += 1;
                    } else if norm_chan.is_full() {
                        // if we've done 4 or more priority work in a row, check if
                        // we should skip if the normal channel is full and maybe
                        // blocking, by setting the special number
                        *pri_work_count = 255;
                    }

                    return WorkStatus(0, job, target);
                },
                Err(channel::RecvTimeoutError::Disconnected) => {
                    // sender has been dropped
                    return WorkStatus(-1, None, None);
                },
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
        match Worker::fetch_work(
            norm_chan, pri_chan.is_empty(), id % LOT_COUNTS == 1
        ) {
            Ok(message) => {
                // message is the only place that can update the "done" field
                let (job, target) = Worker::unpack_message(message);
                *pri_work_count = 0;

                return WorkStatus(0, job, target);
            },
            Err(channel::RecvTimeoutError::Disconnected) => {
                // sender has been dropped
                return WorkStatus(-1, None, None);
            },
            Err(channel::RecvTimeoutError::Timeout) => {
                // nothing to receive yet
            }
        };

        WorkStatus(0, None, None)
    }

    fn fetch_work(
        main_chan: &channel::Receiver<Message>,
        side_chan_empty: bool,
        can_skip: bool
    ) -> Result<Message, channel::RecvTimeoutError>
    {
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
                Err(channel::TryRecvError::Disconnected) => return Err(channel::RecvTimeoutError::Disconnected),
                Err(channel::TryRecvError::Empty) => {
                    if can_skip && !side_chan_empty {
                        // if there're normal work in queue, break to fetch the normal work
                        return Err(channel::RecvTimeoutError::Timeout);
                    }
                },
            }

            if wait > rounds {
                return Err(channel::RecvTimeoutError::Timeout);
            }
        }
    }

    fn handle_work(work: Job, since: &mut Option<SystemTime>) -> Option<Duration> {
        work.call_box();

        let mut idle = None;
        if since.is_some() {
            idle = Worker::calc_idle(&since);
            *since = Some(SystemTime::now());
        }

        idle
    }

    fn unpack_message(message: Message) -> (Option<Job>, Option<Vec<usize>>) {
        match message {
            Message::NewJob(job) => {
                (Some(job), None)
            },
            Message::Terminate(target) => {
                (None, Some(target))
            }
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