#![allow(unused)]

use std::pin::Pin;
use std::sync::Arc;
use std::thread::{self, Thread};
use futures_executor::enter;
use futures_task::{waker_ref, Context, Poll, ArcWake};
use crate::model::FutJob;

thread_local! {
    static NOTIFIER: Arc<ThreadNotifier> = Arc::new(ThreadNotifier {
        thread: thread::current(),
    });
}

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

pub(crate) fn fut_run(f: FutJob) {
    let mut fut = Pin::from(f); //unsafe { Pin::new_unchecked(&mut f) };

    poll_executor(move |ctx: &mut Context<'_>| {
        loop {
            if let Poll::Ready(_) = fut.as_mut().poll(ctx) {
                return;
            }

            thread::park();
        };
    });
}