use crossbeam_channel::{Receiver, Sender, TryRecvError};

pub struct ItemsPool<F, T> {
    init: F,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<F, T> ItemsPool<F, T> {
    pub fn new(init: F) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        ItemsPool { init, sender, receiver }
    }

    pub fn into_items(self) -> crossbeam_channel::IntoIter<T> {
        self.receiver.into_iter()
    }
}

impl<F: Fn() -> T, T> ItemsPool<F, T> {
    pub fn with<G, R>(&self, f: G) -> R
    where
        G: FnOnce(&mut T) -> R,
    {
        let mut item = match self.receiver.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => (self.init)(),
            Err(TryRecvError::Disconnected) => unreachable!(),
        };

        // Run the user's closure with the retrieved cache
        let result = f(&mut item);

        if let Err(e) = self.sender.send(item) {
            unreachable!("error when sending into channel {e}");
        }

        result
    }
}
