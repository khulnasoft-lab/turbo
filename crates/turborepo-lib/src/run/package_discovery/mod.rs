use std::sync::Arc;

use tokio::{
    join,
    sync::{watch::Receiver, Mutex},
};
use turbopath::AbsoluteSystemPathBuf;
use turborepo_filewatch::package_watcher::PackageWatcher;
use turborepo_repository::discovery::{DiscoveryResponse, Error, PackageDiscovery, WorkspaceData};

use crate::daemon::{proto::PackageManager, DaemonClient, FileWatching};

#[derive(Debug)]
pub struct DaemonPackageDiscovery<C> {
    daemon: DaemonClient<C>,
}

impl<C> DaemonPackageDiscovery<C> {
    pub fn new(daemon: DaemonClient<C>) -> Self {
        Self { daemon }
    }
}

impl<C: Clone + Send + Sync> PackageDiscovery for DaemonPackageDiscovery<C> {
    async fn discover_packages(&self) -> Result<DiscoveryResponse, Error> {
        tracing::debug!("discovering packages using daemon");

        // we clone here, rather than mutex, so that we can du multiple requests in
        // parallel
        let mut client = self.daemon.clone();

        let response = client
            .discover_packages()
            .await
            .map_err(|e| Error::Failed(Box::new(e)))?;

        Ok(DiscoveryResponse {
            workspaces: response
                .package_files
                .into_iter()
                .map(|p| WorkspaceData {
                    package_json: AbsoluteSystemPathBuf::new(p.package_json).expect("absolute"),
                    turbo_json: p
                        .turbo_json
                        .map(|t| AbsoluteSystemPathBuf::new(t).expect("absolute")),
                })
                .collect(),
            package_manager: PackageManager::from_i32(response.package_manager)
                .expect("valid")
                .into(),
        })
    }
}
