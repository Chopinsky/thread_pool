pub mod common;
mod single_pool;
mod multi_pools;

pub mod shared_mode {
    pub use single_pool::{close, initialize, resize, run};
}

pub mod index_mode {
    pub use multi_pools::{close, initialize, resize, run};
}

pub use common::ThreadPool;
pub use common::PoolState;
pub use common::PoolManager;