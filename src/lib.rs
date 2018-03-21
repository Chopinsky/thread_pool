pub mod common;
mod single_pool;
mod multi_pools;

pub mod shared_mode {
    pub use single_pool::{close, initialize, run};
}

pub mod keyed_mode {
    pub use multi_pools::{close, initialize, run};
}

pub use common::ThreadPool;
pub use common::PoolState;
