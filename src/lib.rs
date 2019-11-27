mod config;
mod debug;
mod manager;
mod model;
mod multi;
mod scheduler;
mod single;
mod worker;
mod local_executor;

pub use crate::{
    config::{Config, ConfigStatus, TimeoutPolicy},
    manager::{StatusBehaviorSetter, StatusBehaviors},
    scheduler::{
        ExecutionError, Hibernation, PoolManager, PoolState, ThreadPool, ThreadPoolStates,
    },
};

pub mod shared_mode {
    pub use crate::single::{close, init_with_config, initialize, resize, run};
}

pub mod index_mode {
    pub use crate::multi::{close, initialize, resize_pool, run_with};
}

pub mod prelude {
    pub use crate::index_mode::*;
    pub use crate::shared_mode::*;
    pub use crate::scheduler::FuturesPool;
    pub use crate::*;
}
