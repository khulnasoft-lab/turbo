pub mod watch;

use std::collections::HashMap;

use rayon::prelude::*;
use turbopath::{AbsoluteSystemPathBuf, RelativeUnixPathBuf};
use turborepo_repository::package_graph::{WorkspaceInfo, WorkspaceName};
use turborepo_scm::SCM;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use super::task_id::TaskId;
use crate::{
    engine::TaskNode, hash::FileHashes, run::error::Error, task_graph::TaskDefinition,
    task_hash::PackageInputsHashes, DaemonClient,
};

pub trait PackageHasher {
    fn calculate_hashes(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> impl std::future::Future<Output = Result<PackageInputsHashes, Error>> + Send;
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
        &mut self,
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

pub struct DaemonPackageHasher<'a, C: Clone> {
    daemon: &'a mut DaemonClient<C>,
}

impl<'a, C: Clone + Send> PackageHasher for DaemonPackageHasher<'a, C> {
    async fn calculate_hashes(
        &mut self,
        _run_telemetry: GenericEventBuilder,
    ) -> Result<PackageInputsHashes, Error> {
        let package_hashes = self.daemon.discover_package_hashes().await;

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

impl<'a, C: Clone> DaemonPackageHasher<'a, C> {
    pub fn new(daemon: &'a mut DaemonClient<C>) -> Self {
        Self { daemon }
    }
}

impl<T: PackageHasher + Send> PackageHasher for Option<T> {
    async fn calculate_hashes(
        &mut self,
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

impl<A: PackageHasher + Send, B: PackageHasher + Send> PackageHasher
    for FallbackPackageHasher<A, B>
{
    async fn calculate_hashes(
        &mut self,
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
