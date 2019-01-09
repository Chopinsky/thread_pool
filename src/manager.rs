use crate::worker::Worker;

pub(crate) struct Manager {
    workers: Vec<Worker>,
}

impl Manager {
    pub(crate) fn new(range: usize) -> Manager {
        Manager {
            workers: Vec::with_capacity(range),
        }
    }
}