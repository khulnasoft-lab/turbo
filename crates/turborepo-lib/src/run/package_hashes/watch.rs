use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;
use turborepo_repository::discovery::PackageDiscovery;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use crate::{
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
    map: Mutex<HashMap<TaskId<'static>, String>>,
}

impl<PD, PH> WatchingPackageHasher<PD, PH> {
    pub fn new(discovery: Arc<Mutex<PD>>, fallback: PH, interval: Duration) -> Self {
        Self {
            interval,
            package_discovery: discovery,
            fallback,
            map: Default::default(),
        }
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
