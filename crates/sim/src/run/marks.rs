//! The runner's side of [`crate::marks`]: what the stores and SQLite saw, added to the run's
//! fault-class counts at its end.

use super::Runner;

impl Runner {
    pub(super) fn tally_marks(&mut self) {
        let hits: Vec<_> = [
            (crate::generate::Target::Staging, &self.w.staging_faults),
            (crate::generate::Target::Remote, &self.w.remote_faults),
        ]
        .into_iter()
        .flat_map(|(target, faults)| {
            faults
                .hits()
                .into_iter()
                .map(move |(op, shape, hits)| (target, op, shape, hits))
        })
        .collect();
        let taken = self.w.sqlite.taken_injections();
        self.marks.tally(&mut self.report.reach, &hits, &taken);
    }
}
