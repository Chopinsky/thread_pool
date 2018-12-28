mod debug;
mod multi_pools;
mod single_pool;
mod scheduler;

pub mod shared_mode {
    pub use crate::single_pool::{close, initialize, resize, run};
}

pub mod index_mode {
    pub use crate::multi_pools::{close, initialize, resize_pool, run_with};
}

pub use crate::scheduler::{PoolManager, PoolState, ThreadPool};
