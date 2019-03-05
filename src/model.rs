pub(crate) type Job = Box<FnBox + Send + 'static>;
pub(crate) type WorkerUpdate = fn(id: usize);

pub(crate) trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

pub(crate) enum Message {
    NewJob(Job),
    Terminate(usize),
}