use std::{collections::HashMap, fs::File, sync::Arc};

use futures::{stream, Stream, TryFutureExt};
use notify::{Event, EventKind};
use serde_json::Value;
use tokio::sync::{broadcast, oneshot, watch};
use turbopath::{AbsoluteSystemPathBuf, AnchoredSystemPathBuf};
use turborepo_repository::{
    change_mapper::ChangeMapper, package_graph::PackageGraph, package_json::PackageJson,
};

use crate::{
    package_watcher::{PackageWatcher, WatchingPackageDiscovery},
    NotifyError,
};

pub struct PackageHashWatcher {
    repo_root: AbsoluteSystemPathBuf,
    package_watcher: Arc<PackageWatcher>,
    _handle: tokio::task::JoinHandle<()>,
}

pub struct HashUpdate {
    pub package: String,
    pub task: String,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct FileHashes(pub HashMap<turbopath::RelativeUnixPathBuf, String>);

impl PackageHashWatcher {
    pub fn new(
        repo_root: AbsoluteSystemPathBuf,
        recv: broadcast::Receiver<Result<Event, NotifyError>>,
        package_watcher: Arc<PackageWatcher>,
    ) -> Self {
        let (_exit_tx, exit_rx) = oneshot::channel();
        let subscriber = Subscriber::new(exit_rx, recv, package_watcher.clone(), repo_root.clone());
        let handle = tokio::spawn(subscriber.watch());
        Self {
            _handle: handle,
            repo_root,
            package_watcher,
        }
    }

    /// general algorithm:
    /// - watch turbo.json
    ///
    /// - discover all the packages / tasks in a project and subscribe to
    ///   changes
    /// - if the list changes add a new watch for that package
    ///
    /// - for all packages in the workspace
    ///   - watch for changes to the list of scripts in the package json
    ///   - get all the globs for all those scripts
    ///   - walk and hash the combined files
    ///   - store those hashes for querying
    pub fn subscribe(&self) -> impl Stream<Item = HashUpdate> {
        stream::empty()
    }
}

struct Subscriber {
    exit_rx: oneshot::Receiver<()>,
    recv: broadcast::Receiver<Result<Event, NotifyError>>,
    package_watcher: Arc<PackageWatcher>,
    repo_root: AbsoluteSystemPathBuf,
}

/// The underlying task that listens to file system events and updates the
/// internal package state.
impl Subscriber {
    fn new(
        exit_rx: oneshot::Receiver<()>,
        recv: broadcast::Receiver<Result<Event, NotifyError>>,
        package_watcher: Arc<PackageWatcher>,
        repo_root: AbsoluteSystemPathBuf,
    ) -> Self {
        Self {
            recv,
            exit_rx,
            package_watcher,
            repo_root,
        }
    }

    async fn watch(mut self) {
        let mut packages_rx = self.package_watcher.watch();

        let (package_graph_tx, mut package_graph_rx) = watch::channel(None);
        let package_graph_tx = Arc::new(package_graph_tx);

        let root_package_json = self.repo_root.join_component("package.json");
        let root_package_json = File::open(root_package_json).unwrap();

        let root_package_json = serde_json::from_reader::<_, Value>(root_package_json).unwrap();
        let root_package_json = PackageJson::from_value(root_package_json).unwrap();

        let handle_package_changes = {
            let repo_root = self.repo_root.clone();
            async move {
                // we could would use a while let here, but watcher::Ref cannot be held across
                // an await so we use a loop and do the transformation in its own scope to keep
                // the borrow checker happy
                loop {
                    let packages = {
                        let changes = packages_rx.get().await.unwrap();
                        changes
                            .as_inner()
                            .iter()
                            .map(|(k, v)| {
                                let package_json = File::open(&v.package_json).unwrap();

                                let package_json =
                                    serde_json::from_reader::<_, Value>(package_json).unwrap();
                                let package_json = PackageJson::from_value(package_json).unwrap();
                                (k.to_owned(), package_json.to_owned())
                            })
                            .collect()
                    };

                    let package_graph =
                        PackageGraph::builder(&repo_root, root_package_json.clone());

                    let package_graph = package_graph
                        .with_package_discovery(WatchingPackageDiscovery::new(
                            self.package_watcher.clone(),
                        ))
                        .with_package_jsons(Some(packages))
                        .build()
                        .await
                        .unwrap();

                    package_graph_tx.send(Some(package_graph));
                }
            }
        };

        let handle_file_update = {
            async move {
                while let Ok(Ok(event)) = self.recv.recv().into_future().await {
                    match event.kind {
                        EventKind::Any | EventKind::Access(_) | EventKind::Other => {
                            // no-op
                        }
                        _ => {
                            let package_graph =
                                package_graph_rx.wait_for(|f| f.is_some()).await.unwrap();
                            let package_graph = package_graph.as_ref().expect("checked");
                            let change_mapper = ChangeMapper::new(package_graph, vec![], vec![]);
                            let _changed_packages = change_mapper
                                .changed_packages(
                                    event
                                        .paths
                                        .into_iter()
                                        .map(|p| {
                                            AnchoredSystemPathBuf::new(
                                                &self.repo_root,
                                                AbsoluteSystemPathBuf::new(p.to_string_lossy())
                                                    .unwrap(),
                                            )
                                            .unwrap()
                                        })
                                        .collect(),
                                    None,
                                )
                                .unwrap();

                            todo!("send the update")
                        }
                    }
                }
            }
        };

        tokio::select! {
            biased;
            _ = &mut self.exit_rx => {}
            _ = handle_file_update => {}
            _ = handle_package_changes => {}
        }
    }
}
