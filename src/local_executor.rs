#![allow(unused)]

use std::pin::Pin;
use std::sync::Arc;
use std::thread::{self, Thread};

thread_local! {
//    static NOTIFIER: Arc<ThreadNotifier> = Arc::new(ThreadNotifier {
//        thread: thread::current(),
//    });
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
