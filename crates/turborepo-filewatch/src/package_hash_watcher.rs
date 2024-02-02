use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    sync::Arc,
};

use futures::{stream, Stream, TryFutureExt};
use itertools::Itertools;
use notify::{Event, EventKind};
use tokio::sync::{broadcast, oneshot};
use turbopath::{AbsoluteSystemPath, AbsoluteSystemPathBuf};
use turborepo_repository::package_json::PackageJson;

use crate::{broadcast_map::HashmapEvent, package_watcher::PackageWatcher, NotifyError};

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
    pub async fn new(
        repo_root: AbsoluteSystemPathBuf,
        recv: broadcast::Receiver<Result<Event, NotifyError>>,
        package_watcher: Arc<PackageWatcher>,
    ) -> Self {
        let (exit_tx, exit_rx) = oneshot::channel();
        let subscriber =
            Subscriber::new(exit_rx, recv, package_watcher.clone(), repo_root.clone()).await;
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
    async fn new(
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
        let mut package_changes = self.package_watcher.subscribe();
        let packages = self.package_watcher.get_package_data().await;

        let file = File::open(self.repo_root.join_component("turbo.json")).unwrap();
        let turbo_json = serde_json::from_reader::<_, TurboJson>(file).unwrap();

        // we need two hashmaps here, one to map a package to all the globs it has,
        // for easy lookup if a package changes. these globs are relative to the package
        let mut package_to_globs = HashMap::new();
        // and one to map an absolute glob to the package + task that 'owns' it for the
        // purpose of reporting hash changes
        let mut glob_to_package = HashMap::<String, Vec<(String, String)>>::new();

        for package in packages {
            overwrite_globs_for_package(
                &turbo_json,
                &package.package_json,
                &[],
                &mut package_to_globs,
            );
        }

        loop {
            tokio::select! {
                biased;
                _ = &mut self.exit_rx => {
                    break;
                }
                file_update = self.recv.recv().into_future() => {
                    match file_update {
                        Ok(Ok(event)) => {
                            match event.kind {
                                // when a file is created / modified, we need to update the hash
                                // for that file
                                EventKind::Create(_) | EventKind::Modify(_) => {
                                    // ignore
                                },
                                // when a file is deleted, we need to make sure it is removed from the
                                // file hashes for the globs that it matches
                                EventKind::Remove(_) => {
                                    // ignore
                                },
                                _ => {
                                    // ignore
                                }
                            }
                        },
                        Ok(Err(_)) => {
                            // the file watcher has been dropped, ignore
                        },
                        Err(_) => {
                            // the file watcher has been dropped, ignore
                        }
                    }
                }
                package_change = package_changes.recv().into_future() => {
                    match package_change {
                        // if a package is added or updated, we need to get the new globs and walk them
                        // if they have not changed
                        Ok((path, HashmapEvent::Insert(v) | HashmapEvent::Update(v))) => {
                            let package_json = v.package_json;


                        }
                        // if a package is removed, we need to remove the globs from the internal state
                        Ok((path, HashmapEvent::Remove)) => {

                        }
                        Err(_) => {
                            // the package watcher has been dropped, ignore
                        }
                    }
                }
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct TurboJson {
    pipeline: HashMap<String, Pipeline>,
}

#[derive(serde::Deserialize)]
struct Pipeline {
    inputs: Vec<String>,
}

fn overwrite_globs_for_package(
    turbo_json: &TurboJson,
    package_json_path: &AbsoluteSystemPath,
    gitignore_globs: &[String],
    globs: &mut HashMap<AbsoluteSystemPathBuf, BTreeSet<String>>,
) {
    let file = File::open(package_json_path).unwrap();
    let package_json = serde_json::from_reader::<_, serde_json::Value>(file).unwrap();
    let package_json = PackageJson::from_value(package_json).unwrap();

    let package_root = package_json_path.parent().expect("has a parent");

    globs.insert(
        package_root.to_owned(),
        package_json
            .scripts
            .iter()
            .flat_map(|(name, _)| {
                turbo_json
                    .pipeline
                    .get(name)
                    .map(|p| p.inputs.clone())
                    .unwrap_or_else(|| gitignore_globs.to_vec())
            })
            .unique()
            .collect(),
    );
}
