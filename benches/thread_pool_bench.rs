#[macro_use]
extern crate criterion;
extern crate threads_pool;

use std::sync::atomic;
use criterion::{black_box, Criterion};
use threads_pool::prelude::*;

fn pool_base(size: usize, bound: usize) {
    let mut pool = ThreadPool::new(size);

    for num in 1..bound {
        pool
            .exec(move || for _ in 1..num%4 { atomic::spin_loop_hint(); }, false)
            .unwrap_or_default();
    }

    pool.close();
}

fn pool_bench(c: &mut Criterion) {
    c.bench_function("base pool measurement", |b| {
        b.iter(|| pool_base(black_box(16), black_box(400)))
    });
}

fn single_bench(c: &mut Criterion) {
    shared_mode::initialize(black_box(16));
    let bound = black_box(400);

    c.bench_function("base single mode measurement", move |b| {
        b.iter(|| {
            for num in 1..bound {
                shared_mode::run(move || for _ in 1..num%16 { atomic::spin_loop_hint(); });
            }
        })
    });

    shared_mode::close();
}

criterion_group!(benches, pool_bench, single_bench);
criterion_main!(benches);
