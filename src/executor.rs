#![allow(unused)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Poll, Context};
use std::thread::{self, Thread};

use async_task::Task;
use crossbeam_deque::Worker;
use crossbeam_channel as channel;

#[macro_export]
macro_rules! pin_mut {
    ($($x:ident),*) => { $(
        // Move the value to ensure that it is owned
        let mut $x = $x;
        // Shadow the original binding so that it can't be directly accessed
        // ever again.
        #[allow(unused_mut)]
        let mut $x = unsafe {
            $crate::core_export::pin::Pin::new_unchecked(&mut $x)
        };
    )* }
}

thread_local! {
    static QUEUE: Arc<Worker<Task<()>>> = Arc::new(Worker::new_fifo());
//    static NOTIFIER: Arc<ThreadNotifier> = Arc::new(ThreadNotifier {
//        thread: thread::current(),
//    });
}

pub(crate) fn enqueue<F, R>(future: F) -> channel::Receiver<R>
where
    F: Future<Output = R> + 'static,
    R: Send + 'static,
{
    // If the task gets woken up, it will be sent into this channel.
    let (s, r) = channel::bounded(1);
    let fut = async move {
        s.send(future.await);
    };

    let schedule = move |task| {
        QUEUE.with(|q| {
            q.push(task);
        });
    };

    // Create a task with the future and the schedule function.
    let (task, handle) =
        async_task::spawn_local(fut, schedule, ());

    task.run();

    r
}

fn poll() {
    QUEUE.with(|q| {
        if let Some(mut task) = q.pop() {
            task.run();
        }
    });
}

/*
pub(crate) struct ThreadNotifier {
    thread: Thread,
}

impl ArcWake for ThreadNotifier {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.thread.unpark();
    }
}

pub(crate) fn poll_executor<F>(mut f: F)
where
    F: FnMut(&mut Context<'_>)
{
    let _enter = enter().expect(
        "cannot execute `LocalPool` executor from within another executor",
    );

    NOTIFIER.with(|notifier| {
        let waker = waker_ref(notifier);
        let mut cx = Context::from_waker(&waker);
        f(&mut cx)
    });
}

pub(crate) fn fut_run(mut f: FutJob) {
//    let mut fut = Pin::from(f);

    poll_executor(move |ctx: &mut Context<'_>| {
        loop {
            if let Poll::Ready(_) = f.as_mut().poll(ctx) {
                return;
            }

            thread::park();
        };
    });
}
*/
