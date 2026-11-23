//! The runner's side of [`crate::marks`]: what the stores and SQLite saw, added to the run's
//! fault-class and row counts at its end.

use super::Runner;

impl Runner {
    pub(super) fn tally_marks(&mut self) {
        let hits: Vec<_> = [&self.w.staging_faults, &self.w.remote_faults]
            .into_iter()
            .flat_map(crate::FaultHandle::hits)
            .map(|(_, shape, hits)| (shape, hits))
            .collect();
        let queued = self.w.sqlite.queued_injections();
        self.marks.tally(&mut self.report.reach, hits, queued);
        crate::marks::take_rows(&mut self.report.reach);
    }
}
