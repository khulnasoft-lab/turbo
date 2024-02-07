use tokio::sync::watch::{self, error::RecvError, Ref};

#[derive(Debug)]
pub struct OptionalWatch<T>(watch::Receiver<Option<T>>);

impl<T> Clone for OptionalWatch<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// A handy wrapper around types that are watched and may be None.
/// `SomeRef` is a reference type that is known to be `Some`.
impl<T> OptionalWatch<T> {
    pub fn new() -> (watch::Sender<Option<T>>, OptionalWatch<T>) {
        let (tx, rx) = watch::channel(None);
        (tx, OptionalWatch(rx))
    }

    /// Wait for the value to be available and then return it.
    pub async fn get(&mut self) -> Result<SomeRef<'_, T>, RecvError> {
        let recv = self.0.wait_for(|f| f.is_some()).await?;
        Ok(SomeRef(recv))
    }
}

pub struct SomeRef<'a, T>(Ref<'a, Option<T>>);

impl<'a, T> std::ops::Deref for SomeRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("checked")
    }
}

#[cfg(test)]
mod test {
    use futures::FutureExt;

    /// Futures have a method that allow you to fetch the value of a future
    /// if it is immediately available. This is useful for, for example,
    /// allowing consumers to poll a future and get the value if it is
    /// available, but otherwise just continue on, rather than wait.
    #[tokio::test]
    pub async fn now_or_never_works() {
        let (tx, mut rx) = super::OptionalWatch::new();

        tx.send(Some(42)).unwrap();

        assert_eq!(*rx.get().now_or_never().unwrap().unwrap(), 42);
    }
}
