use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::StreamExt;
use tokio::{
    sync::{watch, watch::error::RecvError, Mutex},
    time::error::Elapsed,
};
use turborepo_repository::discovery::PackageDiscovery;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use crate::{
    daemon::FileWatching,
    run::{package_hashes::PackageHasher, task_id::TaskId, Error},
    task_hash::PackageInputsHashes,
};

/// WatchingPackageHasher discovers all the packages / tasks in a project,
/// watches them all, and recalculates all the hashes for them eagerly.
pub struct WatchingPackageHasher<PD, PH> {
    package_discovery: Arc<Mutex<PD>>,
    /// the fallback is used occasionally to flush / report any drifted
    /// hashes. the result is compared with the current map, and warnings
    /// are logged
    fallback: PH,
    interval: Duration,
    map: Arc<Mutex<HashMap<TaskId<'static>, String>>>,

    watcher_rx: watch::Receiver<Option<Arc<FileWatching>>>,
}

#[derive(thiserror::Error, Debug)]
enum WaitError {
    #[error(transparent)]
    Elapsed(#[from] Elapsed),
    #[error(transparent)]
    Unavailable(#[from] RecvError),
}

impl<PD, PH: PackageHasher> WatchingPackageHasher<PD, PH> {
    pub async fn new(
        discovery: Arc<Mutex<PD>>,
        mut fallback: PH,
        interval: Duration,
        watcher_rx: watch::Receiver<Option<Arc<FileWatching>>>,
    ) -> Self {
        let map = Arc::new(Mutex::new(
            fallback
                .calculate_hashes(Default::default())
                .await
                .unwrap()
                .hashes,
        ));

        /// listen to updates from the file watcher and update the map
        let subscriber = tokio::task::spawn({
            let watcher_rx = watcher_rx.clone();
            let map = map.clone();
            async move {
                let watch = Self::wait_for_filewatching(watcher_rx.clone())
                    .await
                    .unwrap();
                let mut stream = watch.package_hash_watcher.subscribe();
                while let Some(update) = stream.next().await {
                    let mut map = map.lock().await;
                    map.insert(
                        TaskId::new(&update.package, &update.task).into_owned(),
                        update.hash,
                    );
                }
            }
        });

        Self {
            interval,
            package_discovery: discovery,
            fallback,
            map,
            watcher_rx,
        }
    }

    async fn wait_for_filewatching(
        watcher_rx: watch::Receiver<Option<Arc<FileWatching>>>,
    ) -> Result<Arc<FileWatching>, WaitError> {
        let mut rx = watcher_rx.clone();
        let fw = tokio::time::timeout(Duration::from_secs(1), rx.wait_for(|opt| opt.is_some()))
            .await??;

        return Ok(fw.as_ref().expect("guaranteed some above").clone());
    }
}

impl<PD: PackageDiscovery + Send, PH: PackageHasher + Send> PackageHasher
    for WatchingPackageHasher<PD, PH>
{
    async fn calculate_hashes(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        let data = self.map.lock().await;
        Ok(PackageInputsHashes {
            hashes: data.clone(),
            expanded_hashes: Default::default(),
        })
    }
}
