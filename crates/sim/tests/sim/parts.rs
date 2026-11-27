//! The replica on the S3 store sending in parts (`lsr_sim::remote`,
//! `docs/decisions/D233.md`): parts stay started between two ticks and land in an order
//! the seed draws, some of them after the process that started them ended.
//!
//! The swarm draws parts for half of its S3 runs; the seeds here all send in parts, and the
//! states only parts reach are asserted of them (`lsr_sim::reach::UNREQUIRED` names each).

use lsr_sim::remote::Parts;
use lsr_sim::{Opts, Reach, Seed, Weights, replay_line, run_with};

const STEPS: u32 = 150;
const SEEDS: u64 = 16;

/// The swarm's mix for `n` on the S3 replica, sending in parts whatever the seed drew: the
/// drawn part size and width when it drew them, else ones that follow from `n`.
fn weights(n: u64) -> Weights {
    let mut weights = Weights::swarm(Seed::new(n));
    weights.remote_s3 = true;
    weights.http_fault = 40;
    if weights.remote_parts.is_none() {
        weights.remote_parts = Parts::new(
            if n.is_multiple_of(2) { 64 } else { 256 },
            [1, 2, 3][usize::try_from(n % 3).unwrap()],
        );
    }
    weights
}

/// Every judge of a swarm run holds with parts in flight between ticks, and the seeds reach
/// what only parts reach: parts landing out of the order they were started in, an upload
/// with every lane taken, ticks that wait for a part, and parts that land or are lost after
/// their process ended. (A part's request failing while others are started is rare in so
/// few seeds; `scenarios` has it.)
#[test]
fn swarm_seeds_pass_with_the_s3_replica_sending_in_parts() {
    let mut reach = Reach::default();
    for n in 1..=SEEDS {
        let seed = Seed::new(n);
        let report =
            run_with(seed, STEPS, &weights(n), &Opts::default()).unwrap_or_else(|failure| {
                panic!(
                    "{failure}\nparts {:?}; {}",
                    weights(n).remote_parts,
                    replay_line(seed, Some(STEPS))
                )
            });
        reach.merge(&report.reach);
    }
    assert_eq!(reach.get("remote_s3.parts.runs"), SEEDS);
    for key in [
        "parts.landed",
        "parts.landed.out_of_order",
        "parts.full",
        "parts.tick_waited",
        "parts.at_exit.landed",
        "parts.at_exit.lost",
        "reread.given",
    ] {
        assert!(reach.get(key) > 0, "no seed reached {key}");
    }
}

/// A multipart upload in short explicit scenarios: what happens with parts started and not
/// yet landed (they land out of order, one fails, the engine is stopped, the process dies,
/// the staged file changes, the application commits). Each scenario lands its own parts
/// ([`PartsOp::ByHand`]) up to the moment it is about, and every run ends with the drain and
/// the judges of any other run: the replica restores every transaction afterwards, no part was
/// sent for an upload already ended, and no sweep left an upload open.
mod scenarios {
    use std::num::NonZeroU64;

    use lsr_core::{Did, Event, Fault};
    use lsr_sim::generate::{HttpFault, HttpShape, MetaOp, PartsOp};
    use lsr_sim::remote::Parts;
    use lsr_sim::run::{Seen, run_ops};
    use lsr_sim::{Op, Opts, RunReport, Seed};

    /// A part, and a tick's bytes: each slice of a staged file is one part. The first file (the
    /// snapshot of the eleven-page database) is 2,950 bytes: five parts and a last one of 390.
    const PART: u64 = 512;
    const FILE: u64 = 2950;

    fn opts() -> Opts {
        Opts {
            slice_bytes: NonZeroU64::new(PART),
            record: true,
            remote_s3: true,
            remote_parts: Parts::new(PART, 4),
            ..Opts::default()
        }
    }

    fn run_seeded(seed: u64, ops: &[Op]) -> RunReport {
        run_ops(Seed::new(seed), ops, &opts())
            .map_err(|failure| failure.violation.to_string())
            .unwrap()
    }

    fn run(ops: &[Op]) -> RunReport {
        run_seeded(1, ops)
    }

    fn events(report: &RunReport) -> Vec<&Event> {
        report
            .seen
            .iter()
            .filter_map(|seen| {
                if let Seen::Event(event) = seen {
                    Some(event)
                } else {
                    None
                }
            })
            .collect()
    }

    fn ticks(n: usize) -> impl Iterator<Item = Op> {
        std::iter::repeat_n(Op::Tick, n)
    }

    /// The parts land by hand; a commit; the boot, the sync that stages the first file, and
    /// `slices` slices of it, each a whole part. Four parts may be started at once.
    fn sliced(slices: usize) -> Vec<Op> {
        let mut ops = vec![
            Op::Parts(PartsOp::ByHand),
            Op::Commit {
                pages: 3,
                grow: true,
            },
        ];
        ops.extend(ticks(slices.saturating_add(2)));
        ops
    }

    /// The rest of a run: the parts land by the seed again, and the engine has the ticks to
    /// send whatever it still owes before the replica is restored and judged.
    fn the_rest() -> impl Iterator<Item = Op> {
        [Op::Parts(PartsOp::BySeed)]
            .into_iter()
            .chain(ticks(60))
            .chain([Op::Restore])
    }

    fn lands(order: &[u8]) -> impl Iterator<Item = Op> + '_ {
        order.iter().map(|at| Op::Parts(PartsOp::Land(*at)))
    }

    /// Where the first file is published: the first level-0 file the engine reports put.
    fn published(all: &[&Event]) -> usize {
        all.iter()
            .position(|event| matches!(event, Event::Uploaded(Did::Put(_))))
            .unwrap_or_else(|| panic!("the first file was never published: {all:?}"))
    }

    /// Where the engine first reports `fault` and waits to try again.
    fn retried(all: &[&Event], fault: Fault) -> usize {
        all.iter()
            .position(|event| matches!(event, Event::Retrying { fault: was, .. } if *was == fault))
            .unwrap_or_else(|| panic!("{fault:?} was never reported: {all:?}"))
    }

    fn sent(all: &[&Event]) -> Vec<u64> {
        all.iter()
            .filter_map(|event| {
                if let Event::Sending { bytes } = event {
                    Some(*bytes)
                } else {
                    None
                }
            })
            .collect()
    }

    /// With four parts started and a fifth whole and waiting, the upload takes no more: the
    /// tick that finds it so reads nothing and is no slice, and the ticks after it wait
    /// without sending a byte until a part lands.
    #[test]
    fn an_upload_with_every_lane_taken_waits_and_reads_nothing() {
        let mut ops = sliced(5);
        ops.extend(ticks(4));
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        assert_eq!(sent(&all[..7]), [512, 1024, 1536, 2048, 2560], "{all:?}");
        assert!(
            all[7..11].iter().all(|event| matches!(event, Event::Idle)),
            "{all:?}"
        );
        assert!(published(&all) > 11, "{all:?}");
        assert!(report.reach.get("parts.full") >= 4);
        assert!(report.reach.get("parts.tick_waited") >= 4);
        assert_eq!(report.reach.get("parts.uploads_open_at_the_end"), 0);
    }

    /// The four parts started land newest first, and then the last part before the fifth:
    /// the replica holds the file the engine staged, which the restore at the end judges
    /// byte for byte.
    #[test]
    fn parts_that_land_newest_first_make_the_file_that_was_staged() {
        let mut ops = sliced(5);
        ops.extend(lands(&[3, 2, 1, 0]));
        ops.push(Op::Tick);
        ops.extend(lands(&[1, 0]));
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        assert_eq!(
            sent(&all[..8]),
            [512, 1024, 1536, 2048, 2560, FILE],
            "{all:?}"
        );
        assert_eq!(published(&all), 8, "{all:?}");
        assert!(report.reach.get("parts.landed.out_of_order") >= 4);
        assert_eq!(report.reach.get("parts.landed.faulted"), 0);
    }

    /// A part's request fails on every try its upload gives it (eight, as Go's uploader does).
    fn part_fails_for_good() -> impl Iterator<Item = Op> {
        (0..8).map(|_| Op::HttpFault(HttpFault::Internal(HttpShape::UploadPart)))
    }

    /// One request of a part fails while the first, third and fourth are started. The part is
    /// sent again by its own upload, the parts already sent stay, and the engine never sees a
    /// fault: the file is published once, from its first slice, with no second upload.
    #[test]
    fn a_part_that_fails_once_is_sent_again_and_the_file_is_not_restarted() {
        let mut ops = sliced(4);
        ops.push(Op::HttpFault(HttpFault::Internal(HttpShape::UploadPart)));
        ops.extend(lands(&[1]));
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        assert!(
            !all.iter()
                .any(|event| matches!(event, Event::Retrying { .. })),
            "{all:?}"
        );
        assert_eq!(report.reach.get("http_fault.unmet"), 0);
        // The fault was met, by one landing, and the part still ended well.
        assert_eq!(report.reach.get("parts.landed.faulted"), 1);
        assert_eq!(report.reach.get("parts.uploads_open_at_the_end"), 0);
    }

    /// Every try of the second part's request fails while the first, third and fourth are
    /// started. The engine reports the replica's fault at its next tick, nothing of that upload
    /// is ever published, the three parts left send nothing more, and the file goes up again
    /// whole.
    #[test]
    fn a_part_that_fails_with_others_started_fails_the_upload_and_the_file_goes_again() {
        let mut ops = sliced(4);
        ops.extend(part_fails_for_good());
        ops.extend(lands(&[1]));
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        let failed = retried(&all, Fault::StoreTransient);
        assert_eq!(failed, 6, "{all:?}");
        assert!(failed < published(&all), "{all:?}");
        assert_eq!(sent(&all[failed..])[0], PART, "{all:?}");
        assert_eq!(report.reach.get("parts.landed.faulted"), 1);
        assert_eq!(report.reach.get("parts.uploads_open_at_the_end"), 0);
    }

    /// Every byte of the file is read and its last part is started, and that part fails: the
    /// failure is the upload's, reported before anything is published, and the file goes
    /// again.
    #[test]
    fn the_last_part_failing_after_the_file_was_read_whole_fails_the_upload() {
        let mut ops = sliced(5);
        ops.extend(lands(&[0, 0, 0, 0]));
        ops.push(Op::Tick);
        ops.extend(part_fails_for_good());
        ops.extend(lands(&[1]));
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        let failed = retried(&all, Fault::StoreTransient);
        assert_eq!(sent(&all[..failed]).last(), Some(&FILE), "{all:?}");
        assert!(failed < published(&all), "{all:?}");
        assert_eq!(report.reach.get("parts.landed.faulted"), 1);
    }

    /// The engine is stopped with four parts started: the drain sends the rest of the file
    /// and ends with it on the replica.
    #[test]
    fn a_stop_with_parts_started_finishes_the_file() {
        let mut ops = sliced(4);
        ops.push(Op::Parts(PartsOp::BySeed));
        ops.push(Op::Shutdown);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        let booted_again = all
            .iter()
            .skip(1)
            .position(|event| matches!(event, Event::Booted { .. }))
            .unwrap_or_else(|| panic!("{all:?}"));
        assert!(published(&all) <= booted_again, "{all:?}");
        assert_eq!(report.shutdowns, 1);
    }

    /// The process dies, or the power goes, with four parts started. Each reached the bucket
    /// or was never sent, as the seed has it; either way the upload is open in the bucket
    /// with nothing in the next process that remembers it. That process's sweep removes it
    /// (the run's judge fails a sweep that leaves one), and the file goes again whole.
    #[test]
    fn a_process_that_dies_with_parts_started_leaves_an_upload_the_next_boot_removes() {
        let (mut landed, mut lost) = (0, 0);
        for (seed, death) in [
            (1, Op::Restart),
            (2, Op::PowerLoss),
            (3, Op::Restart),
            (4, Op::PowerLoss),
        ] {
            let mut ops = sliced(4);
            ops.push(death);
            ops.extend(the_rest());
            let report = run_seeded(seed, &ops);
            let all = events(&report);
            let rebooted = all
                .iter()
                .skip(1)
                .position(|event| matches!(event, Event::Booted { .. }))
                .unwrap_or_else(|| panic!("{all:?}"));
            assert!(rebooted < published(&all), "{all:?}");
            let (here, gone) = (
                report.reach.get("parts.at_exit.landed"),
                report.reach.get("parts.at_exit.lost"),
            );
            assert!(
                here.saturating_add(gone) >= 4,
                "seed {seed}: {here} and {gone}"
            );
            landed += here;
            lost += gone;
            assert_eq!(report.reach.get("parts.uploads_open_at_the_end"), 0);
        }
        assert!(
            landed > 0 && lost > 0,
            "{landed} landed and {lost} were lost"
        );
    }

    /// The staged file changes with four parts of it started and none yet read back. The
    /// third part's second read meets the change, so that part is never sent; the engine
    /// reports the staged file lost, not the replica at fault, and nothing of the file is
    /// published.
    #[test]
    fn a_staged_file_changed_under_started_parts_is_refused_when_a_part_reads_it_back() {
        let mut ops = sliced(4);
        ops.push(Op::Meta(MetaOp::FlipNewestByte));
        ops.extend(lands(&[2]));
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        let lost = retried(&all, Fault::StagedLost);
        assert_eq!(lost, 6, "{all:?}");
        assert!(
            !all[..lost]
                .iter()
                .any(|event| matches!(event, Event::Uploaded(Did::Put(_)))),
            "{all:?}"
        );
        assert!(report.reach.get("reread.refused") >= 1);
        assert_eq!(
            report.reach.get("reread.refused"),
            report.reach.get("reread.refused.reported")
        );
        assert_eq!(report.reach.get("parts.landed.faulted"), 0);
    }

    /// The application commits while the upload has every lane taken. The tick that cannot
    /// send still syncs the WAL: a waiting upload holds nothing else up.
    #[test]
    fn a_commit_while_the_upload_waits_for_its_parts_is_staged() {
        let mut ops = sliced(5);
        ops.push(Op::Tick);
        ops.push(Op::Commit {
            pages: 2,
            grow: false,
        });
        ops.push(Op::Tick);
        ops.extend(the_rest());
        let report = run(&ops);
        let all = events(&report);
        assert!(matches!(all[7], Event::Idle), "{all:?}");
        // The WAL step ran: here it checkpoints, and the checkpoint's seal stages the commit.
        assert!(matches!(all[8], Event::Checkpointed { .. }), "{all:?}");
        assert!(published(&all) > 8, "{all:?}");
        assert_eq!(report.reach.get("parts.uploads_open_at_the_end"), 0);
    }

    /// A provider fault on each request of a multipart upload, with the parts landing as the
    /// seed has them: the fault is met, the engine reports it or never sees it, and the
    /// replica restores every transaction at the end.
    #[test]
    fn a_fault_on_each_request_of_a_multipart_upload_is_survived() {
        use HttpShape::{Abort, Complete, CreateMultipart, UploadPart};
        for fault in [
            HttpFault::Internal(CreateMultipart),
            HttpFault::ConnectionReset(CreateMultipart),
            HttpFault::TruncateBody(CreateMultipart),
            HttpFault::Internal(UploadPart),
            HttpFault::AfterPublish(UploadPart),
            HttpFault::WrongEtag(UploadPart),
            HttpFault::BodyMismatch(UploadPart),
            HttpFault::ConnectionReset(UploadPart),
            HttpFault::Internal(Complete),
            HttpFault::AfterPublish(Complete),
            HttpFault::WrongEtag(Complete),
            HttpFault::TruncateBody(Complete),
            HttpFault::Internal(Abort),
        ] {
            let mut ops = vec![
                Op::HttpFault(fault),
                Op::Commit {
                    pages: 3,
                    grow: true,
                },
            ];
            ops.extend(ticks(80));
            ops.push(Op::Restore);
            let report = run_ops(Seed::new(1), &ops, &opts())
                .unwrap_or_else(|failure| panic!("{fault:?}: {}", failure.violation));
            // An abort is sent only for an upload that failed, which this run has none of.
            if !matches!(fault, HttpFault::Internal(Abort)) {
                assert_eq!(report.reach.get("http_fault.unmet"), 0, "{fault:?}");
            }
            assert!(report.reach.get("parts.landed") > 0, "{fault:?}");
        }
    }

    /// The run ends with many staged files the replica never took, sent in small parts: the
    /// replica refused every request while they were staged, so the drain stops for its deadline
    /// with the backlog still owed, and the engine started again sends it a slice a tick. Ticks
    /// that wait for a part to land are slices of the same sending, bounded as slices are, not
    /// ticks spent unsettled after the boot (a 5,000-step large-profile seed, shrunk).
    #[test]
    fn ticks_waiting_for_parts_after_the_last_boot_count_as_slices() {
        use lsr_sim::generate::Hold;
        let big = || Op::Commit {
            pages: 32,
            grow: true,
        };
        let mut ops = vec![big()];
        ops.extend(ticks(4));
        for _ in 0..48 {
            ops.extend([big(), Op::Tick]);
        }
        ops.push(Op::Hold {
            hold: Hold::CredentialsRevoked,
            ticks: 8,
        });
        // Met by the drain's first upload: the engine waits out the refusal past the drain's
        // deadline, so the backlog is still owed when the engine starts again.
        ops.extend((0..3).map(|ahead| Op::StoreFault {
            target: lsr_sim::Target::Remote,
            op: lsr_core::StoreOp::Upload,
            ahead,
            shape: lsr_sim::FaultShape::Error(lsr_core::StoreError::Denied {
                op: lsr_core::StoreOp::Upload,
                why: lsr_core::Denied::BucketMissing,
            }),
        }));
        let opts = Opts {
            slice_bytes: NonZeroU64::new(64),
            remote_s3: true,
            remote_parts: Parts::new(64, 1),
            trace: true,
            ..Opts::default()
        };
        let report = match run_ops(Seed::new(1), &ops, &opts) {
            Ok(report) => report,
            Err(failure) => panic!(
                "step {}: {}\n{:#?}",
                failure.step, failure.violation, failure.trace
            ),
        };
        assert!(
            report
                .trace
                .iter()
                .any(|line| line.contains("DeadlineReached(Unfinished")),
            "{:#?}",
            report.trace
        );
        assert!(
            report.reach.get("parts.tick_waited") > 0,
            "{:#?}",
            report.trace
        );
    }
}
