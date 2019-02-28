use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender, SendError, SendTimeoutError};
use crate::debug::is_debug_mode;
use crate::model::*;
use crate::manager::*;

const RETRY_LIMIT: i8 = 4;
const CHAN_CAP: usize = 16;
const THRESHOLD: usize = 65535;
const AUTO_EXTEND_TRIGGER_SIZE: usize = 2;

static mut FORCE_CLOSE: AtomicBool = AtomicBool::new(false);

pub enum ExecutionError {
    Timeout,
    Disconnected,
    PoolPoisoned,
}

pub struct ThreadPool {
    init_size: usize,
    manager: Manager,
    chan: (Sender<Message>, Receiver<Message>),
    priority_chan: (Sender<Message>, Receiver<Message>),
    upgrade_threshold: usize,
    auto_extend_threshold: usize,
    auto_scale: bool,
    queue_timeout: Option<Duration>,
    closing: bool,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let pool_size = match size {
            _ if size < 1 => 1,
            _ if size > THRESHOLD => THRESHOLD,
            _ => size,
        };

        let (sender, receiver) = channel::bounded(CHAN_CAP);
        let (pri_rx, pri_tx) = channel::bounded(CHAN_CAP);

        let manager = Manager::new(pool_size, &receiver, &pri_tx);

        ThreadPool {
            init_size: pool_size,
            manager,
            chan: (sender, receiver),
            priority_chan: (pri_rx, pri_tx),
            upgrade_threshold: CHAN_CAP / 2,
            auto_extend_threshold: THRESHOLD,
            auto_scale: false,
            queue_timeout: None,
            closing: false,
        }
    }

    pub fn set_exec_timeout(&mut self, timeout: Option<Duration>) {
        self.queue_timeout = timeout;
    }

    pub fn toggle_auto_scale(&mut self, auto_scale: bool) {
        self.auto_scale = auto_scale;
    }

    pub fn exec<F: FnOnce() + Send + 'static>(
        &mut self, f: F, prioritized: bool) -> Result<(), ExecutionError>
    {
        let retry = if self.auto_scale {
            0
        } else {
            -1
        };
        
        match self.dispatch(Message::NewJob(Box::new(f)), retry, prioritized) {
            Ok(was_busy) => {
                if was_busy && self.auto_scale {
                    if let Some(target) = self.resize_target(self.init_size) {
                        self.resize(target);
                    }
                }

                Ok(())
            },
            Err(SendTimeoutError::Timeout(_)) => Err(ExecutionError::Timeout),
            Err(SendTimeoutError::Disconnected(_)) => Err(ExecutionError::Disconnected),
        }
    }

    pub fn execute<F: FnOnce() + Send + 'static>(
        &self, f: F) -> Result<(), ExecutionError>
    {
        match self.dispatch(Message::NewJob(Box::new(f)), -1, !self.priority_chan.0.is_full()) {
            Ok(_) => Ok(()),
            Err(SendTimeoutError::Timeout(_)) => Err(ExecutionError::Timeout),
            Err(SendTimeoutError::Disconnected(_)) => Err(ExecutionError::Disconnected),
        }
    }

    #[deprecated(
        since = "0.1.10",
        note = "Setup auto-scale using self.toggle_auto_scale(true), then call \
                function self.exec(...) instead; this API will be removed in 0.2.0"
    )]
    pub fn execute_with_autoscale<F: FnOnce() + Send + 'static>(
        &mut self, f: F) -> Result<(), ExecutionError>
    {
        match self.dispatch(Message::NewJob(Box::new(f)), 0, !self.priority_chan.0.is_full()) {
            Ok(was_busy) => {
                if was_busy {
                    if let Some(target) = self.resize_target(self.init_size) {
                        self.resize(target);
                    }
                }

                Ok(())
            },
            Err(SendTimeoutError::Timeout(_)) => Err(ExecutionError::Timeout),
            Err(SendTimeoutError::Disconnected(_)) => Err(ExecutionError::Disconnected),
        }
    }

    fn dispatch(&self, message: Message, retry: i8, with_priority: bool) -> Result<bool, SendTimeoutError<Message>> {
        if self.closing {
            return Err(SendTimeoutError::Disconnected(message));
        }

        let chan =
            if with_priority || (self.chan.0.is_empty() && self.priority_chan.0.len() <= self.upgrade_threshold) {
                // squeeze the work into the priority chan first even if a normal work
                &self.priority_chan.0
            } else {
                // normal work and then priority queue is full
                &self.chan.0
            };

        let was_busy = chan.is_full();

        match self.queue_timeout {
            Some(wait_period) => {
                let factor = if retry > 0 {
                    retry as u32
                } else {
                    1
                };

                return match chan.send_timeout(message, factor * wait_period) {
                    Ok(()) => Ok(was_busy),
                    Err(SendTimeoutError::Disconnected(msg)) => Err(SendTimeoutError::Disconnected(msg)),
                    Err(SendTimeoutError::Timeout(msg)) => {
                        if retry < 0 || retry > RETRY_LIMIT {
                            return Err(SendTimeoutError::Timeout(msg));
                        }

                        return self.dispatch(msg, retry + 1, with_priority);
                    },
                };
            },
            None => {
                if let Err(SendError(message)) = chan.send(message) {
                    return Err(SendTimeoutError::Disconnected(message));
                };
            }
        }

        Ok(was_busy)
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
        unsafe { FORCE_CLOSE.load(Ordering::SeqCst) }
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

        self.manager.extend_by(more, &self.chan.1, &self.priority_chan.1);
    }

    fn shrink(&mut self, less: usize) {
        if less == 0 {
            return;
        }

        for worker in self.manager.shrink_by(less) {
            // send the termination message -- async kill as this is not an urgent task
            if self.priority_chan.0.send(Message::Terminate(worker.get_id())).is_err() && is_debug_mode() {
                eprintln!("Failed to send the termination message to worker: {}", worker.get_id());
            }
        }
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
            l
        } else {
            Duration::from_millis(0)
        };

        self.manager.worker_auto_expire(actual_life);
    }

    fn kill_worker(&mut self, id: usize) {
        if !self.manager.dismiss_worker(id) {
            // can't find the worker with the given id, quit now.
            return;
        }

        if self.priority_chan.0.send(Message::Terminate(id)).is_err() && is_debug_mode() {
            eprintln!("Failed to send the termination message to worker: {}", id);
        }

        if is_debug_mode() {
            println!("Worker {} is told to be terminated...", id);
        }
    }

    fn clear(&mut self) {
        let mut sent = false;
        if let Ok(()) = self.priority_chan.0.send(Message::Terminate(0)) {
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
        unsafe { FORCE_CLOSE.store(true, Ordering::SeqCst); }
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
        self.chan.0.len()
    }

    #[inline]
    fn get_priority_queue_length(&self) -> usize {
            self.priority_chan.0.len()
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