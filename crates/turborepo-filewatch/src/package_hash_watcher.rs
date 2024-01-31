use futures::{stream, Stream};

pub struct PackageHashWatcher;

pub struct HashUpdate {
    pub package: String,
    pub task: String,
    pub hash: String,
}

impl PackageHashWatcher {
    pub fn subscribe(&self) -> impl Stream<Item = HashUpdate> {
        stream::empty()
    }
}
