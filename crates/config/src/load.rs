//! One call from a `litestream.yml` path to everything an application starts: the file read
//! as the daemon reads it, then mapped to one [`DbPlan`] per database.

use std::path::Path;

use crate::GoConfig;
use crate::error::ConfigError;
use crate::expand::{Env, Expand, expand_path};
use crate::map::{MapError, Plan, plan};

/// Why a file could not become a [`Plan`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LoadPlanError {
    /// The file did not load: missing, unreadable, or a problem in its keys.
    #[error("{0}")]
    Config(#[from] ConfigError),
    /// The file loaded but cannot be started as written.
    #[error("{0}")]
    Plan(#[from] MapError),
}

/// Reads `path` as the daemon's `-config` does (`~` and `$VAR` in the path expanded, then
/// environment expansion over the text unless [`Expand::Off`]) and maps it to a [`Plan`]:
/// per database its path, [`lsr_replica::Config`], [`ReplicaPlan`](crate::ReplicaPlan) (open
/// it with [`open_store`](crate::open_store)) and restore-if-missing setting. Keys that
/// configure only the daemon (`addr`, `exec`, `logging`) are loaded but not part of a plan.
///
/// # Errors
/// [`LoadPlanError`]: the file's own problems, or what a start would refuse.
pub fn load_plan(path: &Path, expand: Expand, env: &dyn Env) -> Result<Plan, LoadPlanError> {
    let path = expand_path(&path.to_string_lossy(), env).unwrap_or_else(|_| path.to_owned());
    let loaded = GoConfig::load(&path, expand, env)?;
    Ok(plan(&loaded.config, env)?)
}
