use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};
use crossbeam_channel as channel;
use crate::debug::is_debug_mode;
use crate::model::*;

pub(crate) struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

struct WorkCourier {
    target_id: Option<usize>,
    work: Option<Job>,
}

impl Worker {
    pub(crate) fn new(
        my_id: usize,
        rx: channel::Receiver<Message>,
        pri_rx: channel::Receiver<Message>,
        graveyard: Arc<RwLock<HashSet<usize>>>,
        life: Arc<RwLock<Duration>>,
        privileged: bool,
    ) -> Worker {

        let thread: thread::JoinHandle<()> = thread::spawn(move || {
            let mut courier = WorkCourier {
                target_id: None,
                work: None,
            };

            let mut since = if privileged {
                None
            } else {
                Some(SystemTime::now())
            };

            let mut idle: Option<Duration>;
            let mut trials: u8;
            let mut pri_work_count: u8 = 0;

            // main worker loop
            loop {
                // get ready to take new work from the channel
                if let Ok(g) = graveyard.read() {
                    if g.contains(&0) || g.contains(&my_id) {
                        return;
                    }
                }

                // reset internal states
                trials = 0;

                // wait for message loop
                loop {
                    if pri_work_count == 255 {
                        // if the worker has performed 4 consecutive prioritized work and the normal
                        // channel is full, we skip the priority work once to pick up a normal work
                        // such that it won't be blocked forever; meanwhile, reset the counter.
                        pri_work_count = 0;
                    } else {
                        // handle the priority queue if there're messages for work
                        match pri_rx.try_recv() {
                            Ok(message) => {
                                // message is the only place that can update the "done" field
                                Worker::unpack_message(message, &mut courier);

                                if pri_work_count < 4 {
                                    // only add if we're below the continuous pri-work cap
                                    pri_work_count += 1;
                                } else if rx.is_full() {
                                    // if we've done 4 or more priority work in a row, check if
                                    // we should skip if the normal channel is full and maybe
                                    // blocking, by setting the special number
                                    pri_work_count = 255;
                                }

                                break;
                            },
                            Err(channel::TryRecvError::Empty) => {
                                // if chan empty, do nothing and fall through to the normal chan handle
                            },
                            Err(channel::TryRecvError::Disconnected) => {
                                // sender has been dropped
                                return;
                            }
                        };
                    }

                    match rx.try_recv() {
                        Ok(message) => {
                            // message is the only place that can update the "done" field
                            Worker::unpack_message(message, &mut courier);
                            pri_work_count = 0;
                            break;
                        },
                        Err(channel::TryRecvError::Empty) => {
                            // update the trial counter
                            trials += 1;
                        },
                        Err(channel::TryRecvError::Disconnected) => {
                            // sender has been dropped
                            return;
                        }
                    };

                    if trials > 16 {
                        // idled for too long, check the idle period
                        break;
                    }
                }

                // if there's a job, get it done first, and calc the idle period since last actual job
                idle = if courier.work.is_some() {
                    Worker::handle_work(courier.work.take(), &mut since)
                } else if since.is_some() {
                    Worker::calc_idle(&since)
                } else {
                    None
                };

                //===========================
                //    after-work handling
                //===========================

                // if it's a target kill, handle it now
                if let Some(id) = courier.target_id.take() {
                    if id == 0 {
                        // all clear signal, remove self from both lists
                        if let Ok(mut g) = graveyard.write() {
                            g.clear();
                            g.insert(0);
                        }

                        return;
                    } else if id == my_id {
                        // rare case, where the worker happen to pick up the message
                        // to kill self, then do it
                        if let Ok(mut g) = graveyard.write() {
                            g.remove(&my_id);
                        }

                        return;
                    } else {
                        // async kill signal, update the lists with the target id
                        if let Ok(mut g) = graveyard.write() {
                            g.insert(id);
                        }
                    }
                }

                // if idled longer than the expected worker life for unprivileged workers,
                // then we're done now -- self-purging.
                if let Some(idle) = idle.take() {
                    if let Ok(expected_life) = life.read() {
                        if *expected_life > Duration::from_millis(0) && idle > *expected_life {
                            // in case the worker is also to be killed.
                            if let Ok(mut g) = graveyard.write() {
                                g.remove(&my_id);
                            }

                            return;
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

    pub(crate) fn get_id(&self) -> usize {
        self.id
    }

    // Calling `retire` on a worker will block the thread until the worker has done its work, or wake
    // up from hibernation. This could block the caller for an undetermined amount of time.
    pub(crate) fn retire(&mut self) {
        if let Some(thread) = self.thread.take() {
            // make sure the work is done
            thread.join().unwrap_or_else(|err| {
                eprintln!("Unable to drop worker: {}, error: {:?}", self.id, err);
            });
        }
    }

    fn handle_work(work: Option<Job>, since: &mut Option<SystemTime>) -> Option<Duration> {
        let mut idle = None;
        if let Some(work) = work {
            work.call_box();

            if since.is_some() {
                idle = Worker::calc_idle(&since);
                *since = Some(SystemTime::now());
            }
        }

        idle
    }

    fn unpack_message(
        message: Message,
        courier: &mut WorkCourier
    ) {
        match message {
            Message::NewJob(job) => {
                courier.work = Some(job);
            },
            Message::Terminate(target_id) => {
                courier.target_id = Some(target_id);
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
        if is_debug_mode() {
            println!("Dropping worker {}", self.id);
        }

        self.retire();
    }
}