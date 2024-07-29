use crossbeam_channel::{Receiver, Sender, TryRecvError};

pub struct ItemsPool<F, T, E>
where
    F: Fn() -> Result<T, E>,
{
    init: F,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<F, T, E> ItemsPool<F, T, E>
where
    F: Fn() -> Result<T, E>,
{
    pub fn new(init: F) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        ItemsPool { init, sender, receiver }
    }

    pub fn into_items(self) -> crossbeam_channel::IntoIter<T> {
        self.receiver.into_iter()
    }

    pub fn with<G, R>(&self, f: G) -> Result<R, E>
    where
        G: FnOnce(&mut T) -> Result<R, E>,
    {
        let mut item = match self.receiver.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => (self.init)()?,
            Err(TryRecvError::Disconnected) => unreachable!(),
        };

        // Run the user's closure with the retrieved item
        let result = f(&mut item);

        if let Err(e) = self.sender.send(item) {
            unreachable!("error when sending into channel {e}");
        }

        result
    }
}
