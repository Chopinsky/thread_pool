mod single_pool;
mod multi_pools;
mod debug;

pub mod common;

pub mod shared_mode {
    pub use single_pool::{close, initialize, resize, run};
}

pub mod index_mode {
    pub use multi_pools::{close, initialize, resize_pool, run_with};
}

pub use common::{PoolState, PoolManager, ThreadPool};