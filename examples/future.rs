extern crate threads_pool;

use threads_pool::prelude::*;
use futures_channel::mpsc;
use futures_util::StreamExt;
use futures_executor::block_on;

fn main() {
    let size = 4;

    let mut pool = ThreadPool::new(size);

    let main = async {
        let (tx, mut rx) = mpsc::unbounded::<i32>();

        let fut_tx_result = async move {
            (0..100).for_each(|v| {
                println!("Sending ...");
                tx.unbounded_send(v).expect("Failed to send");
            })
        };

        pool.spawn(fut_tx_result);

        let mut pending = vec![];
        while let Some(v) = rx.next().await {
            println!("Receiving ...");
            pending.push(v * 2);
        };

        println!("{}", pending.len());
    };

    block_on(main);
}