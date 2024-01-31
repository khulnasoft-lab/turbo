use rayon::iter::ParallelBridge;
use turbopath::AbsoluteSystemPath;
use turborepo_repository::package_graph::PackageGraph;
use turborepo_scm::SCM;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use super::task_id::TaskId;
use crate::{engine::Engine, run::error::Error, task_hash::PackageInputsHashes, DaemonClient};

pub trait PackageHasher {
    async fn calculate_workspaces(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes, Error>;
}

pub struct WorkspaceHashes {
    pub package_inputs: PackageInputsHashes,
}

pub struct LocalPackageHashes<'a> {
    scm: SCM,
    pkg_dep_graph: &'a PackageGraph,
    engine: &'a Engine,
    repo_root: &'a AbsoluteSystemPath,
}

impl<'a> LocalPackageHashes<'a> {
    pub fn new(
        scm: SCM,
        pkg_dep_graph: &'a PackageGraph,
        engine: &'a Engine,
        repo_root: &'a AbsoluteSystemPath,
    ) -> Self {
        Self {
            scm,
            pkg_dep_graph,
            engine,
            repo_root,
        }
    }
}

impl<'a> PackageHasher for LocalPackageHashes<'a> {
    async fn calculate_workspaces(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes, Error> {
        let workspaces = self.pkg_dep_graph.workspaces().collect();
        let package_inputs_hashes = PackageInputsHashes::calculate_file_hashes(
            &self.scm,
            self.engine.tasks().par_bridge(),
            workspaces,
            self.engine.task_definitions(),
            self.repo_root,
            &run_telemetry,
        )?;
        Ok(WorkspaceHashes {
            package_inputs: package_inputs_hashes,
        })
    }
}

pub struct DaemonPackageHasher<'a, C: Clone> {
    daemon: &'a mut DaemonClient<C>,
}

impl<'a, C: Clone> PackageHasher for DaemonPackageHasher<'a, C> {
    async fn calculate_workspaces(
        &mut self,
        _run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes, Error> {
        let package_hashes = self.daemon.discover_package_hashes().await;

        package_hashes
            .map(|resp| WorkspaceHashes {
                package_inputs: PackageInputsHashes {
                    hashes: resp
                        .into_iter()
                        .map(|f| (TaskId::new(&f.package, &f.task).into_owned(), f.hash))
                        .collect(),
                    ..Default::default()
                },
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
    async fn calculate_workspaces(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes, Error> {
        tracing::debug!("hashing packages using optional strategy");

        match self {
            Some(d) => d.calculate_workspaces(run_telemetry).await,
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
    async fn calculate_workspaces(
        &mut self,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes, Error> {
        tracing::debug!("discovering packages using fallback strategy");

        tracing::debug!("attempting primary strategy");
        match tokio::time::timeout(
            self.timeout,
            self.primary.calculate_workspaces(run_telemetry.clone()),
        )
        .await
        {
            Ok(Ok(packages)) => Ok(packages),
            Ok(Err(err1)) => {
                tracing::debug!("primary strategy failed, attempting fallback strategy");
                match self.fallback.calculate_workspaces(run_telemetry).await {
                    Ok(packages) => Ok(packages),
                    // if the backup is unavailable, return the original error
                    Err(Error::PackageHashingUnavailable) => Err(err1),
                    Err(err2) => Err(err2),
                }
            }
            Err(_) => {
                tracing::debug!("primary strategy timed out, attempting fallback strategy");
                self.fallback.calculate_workspaces(run_telemetry).await
            }
        }
    }
}
