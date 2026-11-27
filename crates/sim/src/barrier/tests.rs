use super::*;

/// A checkpointer that grants every barrier.
struct Grants;

impl Checkpointer for Grants {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        Ok(Barrier::new(1))
    }

    fn end_barrier(&mut self, _barrier: Barrier) -> Result<(), SourceError> {
        Ok(())
    }

    fn checkpoint(&mut self, _mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        Err(SourceError::Busy)
    }

    fn renew_read(&mut self) -> Result<ReadLockHold, SourceError> {
        Ok(ReadLockHold::first())
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

fn watching(watch: &BarrierWatch) -> Watching<Grants> {
    Watching::new(
        Grants,
        watch.clone(),
        (RecorderLog::default(), RecorderLog::default()),
        Box::new(|| 0),
    )
}

/// The checker can fail: a barrier the tick's checkpointer took and did not release is still
/// open when the next tick's checkpointer takes over.
#[test]
fn a_barrier_left_held_is_open_after_its_tick() {
    let watch = BarrierWatch::default();
    let barrier = watching(&watch).begin_barrier().unwrap();
    assert!(watch.take().open);
    watching(&watch).end_barrier(barrier).unwrap();
    let held = watch.take();
    assert_eq!((held.holds, held.open, held.remote_requests), (1, false, 0));
}

#[test]
fn a_process_that_dies_takes_its_barrier_with_it() {
    let watch = BarrierWatch::default();
    let _died_with = watching(&watch).begin_barrier().unwrap();
    watch.process_died();
    assert_eq!(watch.take(), Held::default());
}
