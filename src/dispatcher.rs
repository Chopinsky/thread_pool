use crate::model::Message;
use crossbeam_channel as channel;
use std::collections::HashSet;
use std::time::Duration;

pub(crate) struct Dispatcher {
    receiver: channel::Receiver<Message>,
    subscribers: HashSet<usize>,
    graveyard: HashSet<usize>,
}

impl Dispatcher {
    pub(crate) fn new(receiver: channel::Receiver<Message>) -> Self {
        Dispatcher {
            receiver,
            subscribers: HashSet::new(),
            graveyard: HashSet::new(),
        }
    }

    pub(crate) fn insert(&mut self, id: usize) -> bool {
        self.subscribers.insert(id)
    }

    pub(crate) fn clear(&mut self) {
        self.graveyard.extend(&self.subscribers);
        self.subscribers.clear();
    }

    pub(crate) fn verify(&mut self, id: usize) -> bool {
        if self.graveyard.contains(&id) {
            // if to kill, done and mark; should have already been removed from the subscribers list
            // by now
            self.graveyard.remove(&id);
            self.subscribers.remove(&id);
            return false;
        }

        if !self.subscribers.contains(&id) {
            // the worker is added ad hoc: it's not on either list. Now add it to subscribers
            self.subscribers.insert(id);
        }

        true
    }

    pub(crate) fn kill(&mut self, target_id: usize, is_async_kill: bool) {
        self.subscribers.remove(&target_id);

        if is_async_kill {
            self.graveyard.insert(target_id);
        } else {
            self.graveyard.remove(&target_id);
        }
    }

    pub(crate) fn len(&mut self) -> usize {
        self.subscribers.len()
    }

    pub(crate) fn try_recv(&mut self) -> Result<Message, channel::RecvTimeoutError> {
        self.receiver.recv_timeout(Duration::from_millis(255))
    }
}