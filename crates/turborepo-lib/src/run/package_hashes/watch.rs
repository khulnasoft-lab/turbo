use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{FutureExt, StreamExt};
use tokio::{sync::watch::error::RecvError, time::error::Elapsed};
use turborepo_filewatch::OptionalWatch;
use turborepo_repository::discovery::{PackageDiscovery, PackageDiscoveryBuilder};
use turborepo_telemetry::events::generic::GenericEventBuilder;

use super::PackageHasherBuilder;
use crate::{
    daemon::FileWatching,
    run::{package_hashes::PackageHasher, task_id::TaskId, Error},
    task_hash::PackageInputsHashes,
};

/// WatchingPackageHasher discovers all the packages / tasks in a project,
/// watches them all, and recalculates all the hashes for them eagerly.
pub struct WatchingPackageHasher<PD, PH> {
    package_discovery: PD,
    /// the fallback is used occasionally to flush / report any drifted
    /// hashes. the result is compared with the current map, and warnings
    /// are logged
    fallback: OptionalWatch<PH>,
    interval: Duration,
    hashes_rx: OptionalWatch<HashMap<TaskId<'static>, String>>,

    file_watching: FileWatching,
}

#[derive(thiserror::Error, Debug)]
enum WaitError {
    #[error(transparent)]
    Elapsed(#[from] Elapsed),
    #[error(transparent)]
    Unavailable(#[from] RecvError),
}

impl<PD, PH: PackageHasher + Send + Sync + 'static> WatchingPackageHasher<PD, PH> {
    pub fn new<PHB: PackageHasherBuilder<Output = PH> + Send + 'static>(
        package_discovery: PD,
        fallback: PHB,
        interval: Duration,
        file_watching: FileWatching,
    ) -> Self {
        let (hashes_tx, hashes_rx) = OptionalWatch::new();
        let hashes_tx = Arc::new(hashes_tx);

        let (hasher_tx, hasher_rx) = OptionalWatch::new();
        let hasher_tx = Arc::new(hasher_tx);

        // listen to updates from the file watcher and update the map
        let _subscriber = tokio::task::spawn({
            let file_watching = file_watching.clone();
            let hashes_tx = hashes_tx.clone();
            async move {
                let hasher = fallback.build().await.unwrap();

                let data = hasher
                    .calculate_hashes(Default::default())
                    .await
                    .unwrap()
                    .hashes;

                hasher_tx.send(Some(hasher));
                hashes_tx.send(Some(data));

                let mut stream = file_watching.package_hash_watcher.subscribe();
                while let Some(_update) = stream.next().await {
                    hashes_tx.send_modify(|_d| {
                        todo!();
                        // d.insert(
                        //     TaskId::new(&update.package,
                        // &update.task).into_owned(),
                        //     update.hash,
                        // );
                    })
                }
            }
        });

        Self {
            interval,
            package_discovery,
            fallback: hasher_rx,
            hashes_rx,
            file_watching,
        }
    }
}

impl<PD: PackageDiscovery + Send + Sync, PH: PackageHasher + Send + Sync> PackageHasher
    for WatchingPackageHasher<PD, PH>
{
    async fn calculate_hashes(
        &self,
        _run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        // clone here to avoid a mutable reference to self / a mutex
        let mut hashes_rx = self.hashes_rx.clone();

        let data = hashes_rx.get().now_or_never();
        if let Some(Ok(data)) = data {
            Ok(PackageInputsHashes {
                hashes: data.to_owned(),
                expanded_hashes: Default::default(),
            })
        } else {
            Err(Error::PackageHashingUnavailable)
        }
    }
}
