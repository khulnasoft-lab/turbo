use rayon::iter::ParallelBridge;
use tracing::debug;
use turbopath::AbsoluteSystemPath;
use turborepo_env::EnvironmentVariableMap;
use turborepo_repository::package_graph::PackageGraph;
use turborepo_scm::SCM;
use turborepo_telemetry::events::generic::GenericEventBuilder;

use super::global_hash::GlobalHashableInputs;
use crate::{
    cli::EnvMode,
    config::TurboJson,
    engine::Engine,
    run::{error::Error, global_hash::get_global_hash_inputs},
    task_hash::PackageInputsHashes,
};

pub trait PackageHasher {
    async fn calculate_workspaces<'a>(
        &self,
        root_external_dependencies_hash: Option<&'a str>,
        pkg_dep_graph: &PackageGraph,
        repo_root: &'a AbsoluteSystemPath,
        root_turbo_json: &'a TurboJson,
        env_at_execution_start: &'a EnvironmentVariableMap,
        engine: &Engine,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes<'a>, Error>;
}

struct WorkspaceHashes<'a> {
    pub global_hash: String,
    pub global_inputs: GlobalHashableInputs<'a>,
    pub package_inputs: PackageInputsHashes,
}

pub struct LocalPackageHashes {
    env_mode: EnvMode,
    framework_inference: bool,
    scm: SCM,
}

impl LocalPackageHashes {
    pub fn new(env_mode: EnvMode, framework_inference: bool, scm: SCM) -> Self {
        Self {
            env_mode,
            framework_inference,
            scm,
        }
    }
}

impl PackageHasher for LocalPackageHashes {
    async fn calculate_workspaces<'b>(
        &self,
        root_external_dependencies_hash: Option<&'b str>,
        pkg_dep_graph: &PackageGraph,
        repo_root: &'b AbsoluteSystemPath,
        root_turbo_json: &'b TurboJson,
        env_at_execution_start: &'b EnvironmentVariableMap,
        engine: &Engine,
        run_telemetry: GenericEventBuilder,
    ) -> Result<WorkspaceHashes<'b>, Error> {
        let mut global_hash_inputs = get_global_hash_inputs(
            root_external_dependencies_hash,
            repo_root,
            pkg_dep_graph.package_manager(),
            pkg_dep_graph.lockfile(),
            &root_turbo_json.global_deps,
            env_at_execution_start,
            &root_turbo_json.global_env,
            root_turbo_json.global_pass_through_env.as_deref(),
            self.env_mode,
            self.framework_inference,
            root_turbo_json.global_dot_env.as_deref(),
        )?;
        let global_hash = global_hash_inputs.calculate_global_hash_from_inputs();
        debug!("global hash: {}", global_hash);
        let workspaces = pkg_dep_graph.workspaces().collect();
        let package_inputs_hashes = PackageInputsHashes::calculate_file_hashes(
            &self.scm,
            engine.tasks().par_bridge(),
            workspaces,
            engine.task_definitions(),
            repo_root,
            &run_telemetry,
        )?;
        Ok(WorkspaceHashes {
            global_hash,
            global_inputs: global_hash_inputs,
            package_inputs: package_inputs_hashes,
        })
    }
}
