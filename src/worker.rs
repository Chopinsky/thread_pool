use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};
use crossbeam_channel as channel;
use crate::debug::is_debug_mode;
use crate::model::*;

const YIELD_DURATION: u64 = 16;

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
       graveyard: Arc<RwLock<HashSet<usize>>>,
       is_privilege: bool
    ) -> Worker {

        let thread = thread::spawn(move || {
            let mut courier = WorkCourier {
                target_id: None,
                work: None,
            };

            let mut since = if is_privilege {
                None
            } else {
                Some(SystemTime::now())
            };

            loop {
                if let Ok(g) = graveyard.read() {
                    if g.contains(&0) || g.contains(&my_id) {
                        return;
                    }
                }

                //TODO: auto expire, if used, take effect here...

                match rx.recv_timeout(Duration::from_millis(YIELD_DURATION)) {
                    Ok(message) => {
                        // message is the only place that can update the "done" field
                        Worker::unpack_message(message, &mut courier);
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

                // if there's a job, get it done first
                if courier.work.is_some() {
                    Worker::handle_work(courier.work.take(), &mut since)
                }

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
                        return;
                    } else {
                        // async kill signal, update the lists with the target id
                        if let Ok(mut g) = graveyard.write() {
                            g.insert(id);
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

    fn handle_work(work: Option<Job>, since: &mut Option<SystemTime>) {
        if let Some(work) = work {
            work.call_box();

            if since.is_some() {
                *since = Some(SystemTime::now());
            }
        }
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
}

impl Drop for Worker {
    fn drop(&mut self) {
        if is_debug_mode() {
            println!("Dropping worker {}", self.id);
        }

        self.retire();
    }
}