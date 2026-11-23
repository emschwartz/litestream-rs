//! The replica on the S3 store ([`crate::remote`]): the run's switch, and the provider faults a
//! history draws for it ([`crate::Op::HttpFault`]).

use super::{Opts, Runner};
use lsr_store_s3::Shape;

use crate::generate::HttpFault;
use crate::oracle::Violation;

impl Runner {
    /// Puts the replica on S3 when `opts` asks, before the first boot.
    pub(super) fn with_remote_s3(mut self, opts: &Opts) -> Self {
        if opts.remote_s3 {
            self.w.use_s3_remote();
            self.report.reach.bump("remote_s3.runs");
        }
        self
    }

    /// Pushes `fault` onto the S3 store's fault layer; a step with nothing to act on on
    /// `MemStore`.
    pub(super) fn http_fault(&mut self, fault: HttpFault) -> Result<(), Violation> {
        let pushed = match self.w.remote_raw.wire() {
            Some(wire) => {
                wire.push(crate::remote::provider_fault(fault));
                true
            }
            None => false,
        };
        self.scenario_bool("HttpFault", pushed)
    }

    /// The provider faults a listing, a HEAD or a GET has met so far: each cut a read short,
    /// as an object-level read fault does.
    pub(super) fn http_read_faults(&self) -> u32 {
        let fired = self
            .w
            .remote_raw
            .wire()
            .map_or_else(Vec::new, |wire| wire.fired());
        let reads = fired
            .iter()
            .filter(|fault| matches!(fault.on(), Shape::List | Shape::Head | Shape::Get))
            .count();
        u32::try_from(reads).unwrap_or(u32::MAX)
    }

    /// The provider faults pushed and not yet met by a request, at the end of a run.
    pub(super) fn http_faults_pending(&self) -> usize {
        self.w.remote_raw.wire().map_or(0, |wire| wire.pending())
    }
}
