use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::ModelMessage;

/// Thread-safe inboxes for messages that arrive while an agent Turn is active.
/// Runtimes can drain these from `steering_messages` and `follow_up_messages`.
#[derive(Clone, Default)]
pub struct MessageQueue {
    inner: Arc<Mutex<Queues>>,
}

#[derive(Default)]
struct Queues {
    steering: VecDeque<ModelMessage>,
    follow_up: VecDeque<ModelMessage>,
}

impl MessageQueue {
    pub fn steer(&self, message: ModelMessage) {
        self.inner
            .lock()
            .expect("message queue poisoned")
            .steering
            .push_back(message);
    }

    pub fn follow_up(&self, message: ModelMessage) {
        self.inner
            .lock()
            .expect("message queue poisoned")
            .follow_up
            .push_back(message);
    }

    pub fn drain_steering(&self) -> Vec<ModelMessage> {
        self.inner
            .lock()
            .expect("message queue poisoned")
            .steering
            .drain(..)
            .collect()
    }

    pub fn drain_follow_ups(&self) -> Vec<ModelMessage> {
        self.inner
            .lock()
            .expect("message queue poisoned")
            .follow_up
            .drain(..)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        let queues = self.inner.lock().expect("message queue poisoned");
        queues.steering.is_empty() && queues.follow_up.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{MessageContent, MessageRole};

    use super::*;

    #[test]
    fn drains_each_inbox_in_arrival_order() {
        let queue = MessageQueue::default();
        let message = |text: &str| ModelMessage {
            role: MessageRole::User,
            content: MessageContent::Text { text: text.into() },
        };
        queue.steer(message("one"));
        queue.steer(message("two"));
        queue.follow_up(message("later"));
        assert_eq!(queue.drain_steering().len(), 2);
        assert_eq!(queue.drain_follow_ups().len(), 1);
        assert!(queue.is_empty());
    }
}
