mod config;
mod debug;
mod model;
mod multi;
mod manager;
mod scheduler;
mod single;
mod worker;

pub mod shared_mode {
    pub use crate::single::{close, initialize, init_with_config, resize, run};
}

pub mod index_mode {
    pub use crate::multi::{close, initialize, resize_pool, run_with};
}

pub use crate::{
    scheduler::{ExecutionError, Hibernation, PoolManager, PoolState, ThreadPool, ThreadPoolStates},
    manager::{StatusBehaviors, StatusBehaviorSetter},
    config::{Config, ConfigStatus, TimeoutPolicy},
};
