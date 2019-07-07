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
    Terminate(Vec<usize>),
}

pub(crate) const FLAG_NORMAL: u8 = 0;
pub(crate) const FLAG_CLOSING: u8 = 1;
pub(crate) const FLAG_FORCE_CLOSE: u8 = 2;
pub(crate) const FLAG_HIBERNATING: u8 = 4;
pub(crate) const FLAG_LAZY_INIT: u8 = 8;