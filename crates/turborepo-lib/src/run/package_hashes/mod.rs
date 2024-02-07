pub mod watch;

use std::collections::HashMap;

use rayon::prelude::*;
use turbopath::{AbsoluteSystemPathBuf, AnchoredSystemPath, RelativeUnixPathBuf};
use turborepo_repository::{
    discovery::PackageDiscoveryBuilder,
    package_graph::{PackageGraph, WorkspaceInfo, WorkspaceName},
    package_json::PackageJson,
    package_manager,
};
use turborepo_scm::SCM;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use super::task_id::TaskId;
use crate::{
    engine::{EngineBuilder, TaskNode},
    hash::FileHashes,
    run::error::Error,
    task_graph::TaskDefinition,
    task_hash::PackageInputsHashes,
    turbo_json::TurboJson,
    DaemonClient,
};

pub trait PackageHasher {
    fn calculate_hashes(
        &self,
        run_telemetry: GenericEventBuilder,
    ) -> impl std::future::Future<Output = Result<PackageInputsHashes, Error>> + Send;
}

/// We want to allow for lazily generating the PackageDiscovery implementation
/// to prevent unnecessary work. This trait allows us to do that.
///
/// Note: there is a blanket implementation for everything that implements
/// PackageDiscovery
pub trait PackageHasherBuilder {
    type Output: PackageHasher;
    type Error: std::error::Error;

    fn build(self) -> impl std::future::Future<Output = Result<Self::Output, Self::Error>> + Send;
}

impl<T: PackageHasher + Send> PackageHasherBuilder for T {
    type Output = T;
    type Error = std::convert::Infallible;

    async fn build(self) -> Result<Self::Output, Self::Error> {
        Ok(self)
    }
}

pub struct LocalPackageHasherBuilder<PDB: PackageDiscoveryBuilder + Sync> {
    pub repo_root: AbsoluteSystemPathBuf,
    pub discovery: PDB,
    pub scm: SCM,
}

impl<PDB> PackageHasherBuilder for LocalPackageHasherBuilder<PDB>
where
    PDB: PackageDiscoveryBuilder + Sync + Send,
    PDB::Output: Send + Sync,
    PDB::Error: Into<package_manager::Error>,
{
    type Output = LocalPackageHashes;
    type Error = std::convert::Infallible;

    async fn build(self) -> Result<Self::Output, Self::Error> {
        let package_json_path = self.repo_root.join_component("package.json");
        let root_package_json = PackageJson::load(&package_json_path).unwrap();
        let root_turbo_json = TurboJson::load(
            &self.repo_root,
            AnchoredSystemPath::empty(),
            &root_package_json,
            false,
        )
        .unwrap();

        let pkg_dep_graph = {
            PackageGraph::builder(&self.repo_root, root_package_json)
                .with_package_discovery(self.discovery)
                .build()
                .await
                .unwrap()
        };

        let engine = EngineBuilder::new(&self.repo_root, &pkg_dep_graph, false)
            .with_root_tasks(root_turbo_json.pipeline.keys().cloned())
            .with_tasks(root_turbo_json.pipeline.keys().cloned())
            .with_turbo_jsons(Some(
                [(WorkspaceName::Root, root_turbo_json)]
                    .into_iter()
                    .collect(),
            ))
            .with_workspaces(
                pkg_dep_graph
                    .workspaces()
                    .map(|(name, _)| name.to_owned())
                    .collect(),
            )
            .build()
            .unwrap();

        Ok(LocalPackageHashes::new(
            self.scm,
            pkg_dep_graph
                .workspaces()
                .map(|(name, info)| (name.to_owned(), info.to_owned()))
                .collect(),
            engine.tasks().cloned(),
            engine.task_definitions().to_owned(),
            self.repo_root,
        ))
    }
}

pub struct LocalPackageHashes {
    scm: SCM,
    workspaces: HashMap<WorkspaceName, WorkspaceInfo>,
    tasks: Vec<TaskNode>,
    task_definitions: HashMap<TaskId<'static>, TaskDefinition>,
    repo_root: AbsoluteSystemPathBuf,
}

impl LocalPackageHashes {
    pub fn new(
        scm: SCM,
        workspaces: HashMap<WorkspaceName, WorkspaceInfo>,
        tasks: impl Iterator<Item = TaskNode>,
        task_definitions: HashMap<TaskId<'static>, TaskDefinition>,
        repo_root: AbsoluteSystemPathBuf,
    ) -> Self {
        let tasks: Vec<_> = tasks.collect();
        tracing::debug!(
            "creating new local package hasher with {} tasks and {} definitions across {} \
             workspaces",
            tasks.len(),
            task_definitions.len(),
            workspaces.len()
        );
        Self {
            scm,
            workspaces,
            tasks,
            task_definitions,
            repo_root,
        }
    }
}

impl PackageHasher for LocalPackageHashes {
    async fn calculate_hashes(
        &self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        tracing::debug!("running local package hash discovery in {}", self.repo_root);
        let package_inputs_hashes = PackageInputsHashes::calculate_file_hashes(
            &self.scm,
            self.tasks.par_iter(),
            &self.workspaces,
            &self.task_definitions,
            &self.repo_root,
            &run_telemetry,
        )?;
        Ok(package_inputs_hashes)
    }
}

pub struct DaemonPackageHasher<C> {
    daemon: DaemonClient<C>,
}

impl<C: Clone + Send + Sync> PackageHasher for DaemonPackageHasher<C> {
    async fn calculate_hashes(
        &self,
        _run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        // clone to avoid using a mutex or a mutable reference
        let mut daemon = self.daemon.clone();
        let package_hashes = daemon.discover_package_hashes().await;

        package_hashes
            .map(|resp| {
                let mapping: HashMap<_, _> = resp
                    .file_hashes
                    .into_iter()
                    .map(|fh| (fh.relative_path, fh.hash))
                    .collect();

                let (expanded_hashes, hashes) = resp
                    .package_hashes
                    .into_iter()
                    .map(|ph| {
                        (
                            (
                                TaskId::new(&ph.package, &ph.task).into_owned(),
                                FileHashes(
                                    ph.inputs
                                        .into_iter()
                                        .filter_map(|f| {
                                            mapping.get(&f).map(|hash| {
                                                (
                                                    RelativeUnixPathBuf::new(f).unwrap(),
                                                    hash.to_owned(),
                                                )
                                            })
                                        })
                                        .collect(),
                                ),
                            ),
                            (TaskId::from_owned(ph.package, ph.task), ph.hash),
                        )
                    })
                    .unzip();

                PackageInputsHashes {
                    expanded_hashes,
                    hashes,
                }
            })
            .map_err(Error::Daemon)
    }
}

impl<C> DaemonPackageHasher<C> {
    pub fn new(daemon: DaemonClient<C>) -> Self {
        Self { daemon }
    }
}

impl<T: PackageHasher + Send + Sync> PackageHasher for Option<T> {
    async fn calculate_hashes(
        &self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        tracing::debug!("hashing packages using optional strategy");

        match self {
            Some(d) => d.calculate_hashes(run_telemetry).await,
            None => {
                tracing::debug!("no strategy available");
                Err(Error::PackageHashingUnavailable)
            }
        }
    }
}

/// Attempts to run the `primary` strategy for an amount of time
/// specified by `timeout` before falling back to `fallback`
pub struct FallbackPackageHasher<P, F> {
    primary: P,
    fallback: F,
    timeout: std::time::Duration,
}

impl<P: PackageHasher, F: PackageHasher> FallbackPackageHasher<P, F> {
    pub fn new(primary: P, fallback: F, timeout: std::time::Duration) -> Self {
        Self {
            primary,
            fallback,
            timeout,
        }
    }
}

impl<A: PackageHasher + Send + Sync, B: PackageHasher + Send + Sync> PackageHasher
    for FallbackPackageHasher<A, B>
{
    async fn calculate_hashes(
        &self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        tracing::debug!("discovering packages using fallback strategy");

        tracing::debug!("attempting primary strategy");
        match tokio::time::timeout(
            self.timeout,
            self.primary.calculate_hashes(run_telemetry.clone()),
        )
        .await
        {
            Ok(Ok(packages)) => Ok(packages),
            Ok(Err(err1)) => {
                tracing::debug!("primary strategy failed, attempting fallback strategy");
                match self.fallback.calculate_hashes(run_telemetry).await {
                    Ok(packages) => Ok(packages),
                    // if the backup is unavailable, return the original error
                    Err(Error::PackageHashingUnavailable) => Err(err1),
                    Err(err2) => Err(err2),
                }
            }
            Err(_) => {
                tracing::debug!("primary strategy timed out, attempting fallback strategy");
                self.fallback.calculate_hashes(run_telemetry).await
            }
        }
    }
}
