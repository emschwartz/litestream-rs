//! The runner's side of the directory-store staging profile ([`crate::staging`]): putting staging
//! on the real store before the first boot, and judging what each power loss did to it.

use super::{Opts, Runner};
use crate::crash_law::CrashLaw;
use crate::oracle::Violation;
use crate::seed::Seed;

impl Runner {
    /// Puts staging on the directory store over the in-memory filesystem when `opts` asks.
    /// The filesystem's disk is the run's: all-or-nothing, or the block law at the block size
    /// the WAL's law drew, with draws of its own.
    pub(super) fn with_staging_fs(mut self, seed: Seed, opts: &Opts) -> Self {
        if !opts.staging_fs {
            return self;
        }
        let law = match (opts.block_crash_law, CrashLaw::drawn(seed.stream(17))) {
            (true, CrashLaw::Blocks { block, .. }) => CrashLaw::Blocks {
                block,
                seed: seed.stream(20),
            },
            (true | false, _) => CrashLaw::AllOrNothing,
        };
        self.w.use_fs_staging(law);
        self.report.reach.bump("staging_fs.runs");
        self
    }

    /// Every staged file the store reported durable survived each power loss since the last
    /// call as it was.
    pub(super) fn judge_staging_kept(&mut self) -> Result<(), Violation> {
        let (reverted, losses) = self.w.staging_raw.take_log();
        for loss in losses {
            self.report.reach.bump("staging_fs.power_loss");
            if loss.undone > 0 {
                self.report.reach.bump("staging_fs.undone_entries");
            }
            if loss.changed > 0 {
                self.report.reach.bump("staging_fs.changed_files");
            }
        }
        match reverted.first().map(crate::staging::Reverted::describe) {
            None => Ok(()),
            Some((key, how)) => Err(Violation::StagedReverted {
                key,
                how: how.to_owned(),
            }),
        }
    }
}
