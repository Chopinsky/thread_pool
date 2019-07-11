use std::time::Duration;
use crate::manager::{StatusBehaviors, StatusBehaviorSetter};
use crate::model::WorkerUpdate;

#[derive(Copy, Clone)]
pub enum TimeoutPolicy {
    DirectRun,
    Drop,
    LossyRetry,
}

#[derive(Clone)]
pub struct Config {
    non_blocking: bool,
    pool_name: Option<String>,
    refresh_period: Option<Duration>,
    worker_behaviors: StatusBehaviors,
    thread_size: usize,
    timeout_policy: TimeoutPolicy,
}

impl Config {
    pub fn new() -> Self {
        Config {
            non_blocking: false,
            pool_name: None,
            refresh_period: None,
            worker_behaviors: StatusBehaviors::default(),
            thread_size: 0,
            timeout_policy: TimeoutPolicy::Drop,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ConfigStatus {
    fn pool_name(&self) -> Option<&String>;
    fn refresh_period(&self) -> Option<Duration>;
    fn worker_behavior(&self) -> &StatusBehaviors;
    fn non_blocking(&self) -> bool;
    fn thread_size(&self) -> usize;
    fn timeout_policy(&self) -> TimeoutPolicy;
    fn set_pool_name(&mut self, name: String) -> &mut Self;
    fn set_refresh_period(&mut self, period: Option<Duration>) -> &mut Self;
    fn set_worker_behavior(&mut self, behavior: StatusBehaviors) -> &mut Self;
    fn set_none_blocking(&mut self, non_blocking: bool) -> &mut Self;
    fn set_thread_size(&mut self, size: usize) -> &mut Self;
    fn set_timeout_policy(&mut self, policy: TimeoutPolicy) -> &mut Self;
}

impl ConfigStatus for Config {
    /// Check the pool name from the config, if it's set
    fn pool_name(&self) -> Option<&String> {
        self.pool_name.as_ref()
    }

    /// Check the auto balancing period for the `index_mode`
    fn refresh_period(&self) -> Option<Duration> {
        self.refresh_period
    }

    /// Obtain a copy of the status behavior object
    fn worker_behavior(&self) -> &StatusBehaviors {
        &self.worker_behaviors
    }

    /// Check if the config has turned on the `non_blocking` mode
    fn non_blocking(&self) -> bool {
        self.non_blocking
    }

    /// Check the desired stack size for each thread in the pool
    fn thread_size(&self) -> usize {
        self.thread_size
    }

    /// Check the timeout policy for the job
    fn timeout_policy(&self) -> TimeoutPolicy {
        self.timeout_policy
    }

    fn set_pool_name(&mut self, name: String) -> &mut Self {
        if name.is_empty() {
            self.pool_name = None;
        } else {
            self.pool_name.replace(name);
        }

        self
    }

    fn set_refresh_period(&mut self, period: Option<Duration>) -> &mut Self {
        self.refresh_period = period;
        self
    }

    fn set_worker_behavior(&mut self, behavior: StatusBehaviors) -> &mut Self {
        self.worker_behaviors = behavior;
        self
    }

    /// Toggle on/off of the pool's non-blocking mode. If the pool is in the non-blocking mode, the
    /// `ThreadPool` will take the job submission and move on immediately, regardless of if the job
    /// submission is successful or not.
    ///
    /// Please use cautious when toggling the pool to the non-blocking more: if the pool is busy
    /// (i.e. all thread workers are busy) and the job queue is full, a new non-blocking job submission
    /// will cause the job to be dropped and lost forever.
    fn set_none_blocking(&mut self, non_blocking: bool) -> &mut Self {
        self.non_blocking = non_blocking;
        self
    }

    fn set_thread_size(&mut self, size: usize) -> &mut Self {
        self.thread_size = size;
        self
    }

    fn set_timeout_policy(&mut self, policy: TimeoutPolicy) -> &mut Self {
        self.timeout_policy = policy;
        self
    }
}

impl StatusBehaviorSetter for Config {
    fn set_before_start(&mut self, behavior: WorkerUpdate) {
        self.worker_behaviors.set_before_start(behavior);
    }

    fn set_after_start(&mut self, behavior: WorkerUpdate) {
        self.worker_behaviors.set_after_start(behavior);
    }

    fn set_before_drop(&mut self, behavior: WorkerUpdate) {
        self.worker_behaviors.set_before_drop(behavior);
    }

    fn set_after_drop(&mut self, behavior: WorkerUpdate) {
        self.worker_behaviors.set_after_drop(behavior);
    }
}