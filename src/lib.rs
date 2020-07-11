mod config;
mod debug;
mod executor;
mod manager;
mod model;
mod multi;
mod pool;
mod single;
mod worker;

#[doc(hidden)]
pub mod core_export {
    pub use core::*;
}

pub use crate::{
    config::{Config, ConfigStatus, TimeoutPolicy},
    manager::{StatusBehaviorSetter, StatusBehaviors},
    pool::{
        ExecutionError, Hibernation, PoolManager, PoolState, ThreadPool, ThreadPoolStates,
    },
    executor::{
        block_on, FutPool,
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
    pub use crate::executor::block_on;
    pub use crate::shared_mode::*;
    pub use crate::*;
}
